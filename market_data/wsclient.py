"""A minimal, dependency-free WebSocket client (RFC 6455) — text frames only, for vendor feeds.

Why not a library: the capture venv carries no websocket package, and adding one to the process that writes the F&O
store is a dependency change we avoid for a trial. This covers exactly what a request/response JSON feed needs:
the upgrade handshake (ws:// and wss://), masked client text frames, fragmented server frames, ping -> pong, and
close. Nothing here logs payloads, so an API key sent in a frame never reaches a log.
"""
from __future__ import annotations

import base64
import hashlib
import os
import socket
import ssl
import struct
import time
from urllib.parse import urlparse

_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'


class WebSocketError(RuntimeError):
    pass


class WebSocket:
    def __init__(self, url: str, timeout: float = 15.0, verify_tls: bool = True, rcvbuf: int = 0):
        self.url = url
        self.rcvbuf = rcvbuf      # set BEFORE connect, so the TCP window can scale to it
        self.timeout = timeout
        self.verify_tls = verify_tls
        self.sock = None
        self._buf = b''
        self.msg_first = 0.0      # monotonic time the current message's first frame header was read
        self.msg_bytes = 0

    # --- connection ---------------------------------------------------------------------------------------------
    def connect(self):
        u = urlparse(self.url)
        secure = u.scheme == 'wss'
        port = u.port or (443 if secure else 80)
        if self.rcvbuf:
            raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.rcvbuf)
            raw.settimeout(self.timeout)
            raw.connect((socket.gethostbyname(u.hostname), port))
        else:
            raw = socket.create_connection((u.hostname, port), timeout=self.timeout)
        if secure:
            ctx = ssl.create_default_context()
            if not self.verify_tls:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            raw = ctx.wrap_socket(raw, server_hostname=u.hostname)
        key = base64.b64encode(os.urandom(16)).decode()
        path = (u.path or '/') + (('?' + u.query) if u.query else '')
        host = u.hostname + ('' if u.port in (None, 80, 443) else f':{u.port}')
        raw.sendall((f'GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                     f'Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n').encode())
        head = b''
        while b'\r\n\r\n' not in head:
            chunk = raw.recv(4096)
            if not chunk:
                raise WebSocketError('connection closed during the handshake')
            head += chunk
        header, _, rest = head.partition(b'\r\n\r\n')
        lines = header.decode('latin-1').split('\r\n')
        if ' 101 ' not in lines[0] + ' ':
            raise WebSocketError(f'handshake refused: {lines[0]}')
        accept = next((l.split(':', 1)[1].strip() for l in lines[1:] if l.lower().startswith('sec-websocket-accept:')), '')
        expect = base64.b64encode(hashlib.sha1((key + _GUID).encode()).digest()).decode()
        if accept != expect:
            raise WebSocketError('handshake accept key mismatch')
        self.sock, self._buf = raw, rest
        return self

    def close(self):
        if self.sock:
            try:
                self._send_frame(0x8, b'')
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    # --- frames ---------------------------------------------------------------------------------------------------
    def _send_frame(self, opcode: int, payload: bytes):
        mask = os.urandom(4)
        n = len(payload)
        head = bytes([0x80 | opcode])
        if n < 126:
            head += bytes([0x80 | n])
        elif n < 65536:
            head += bytes([0x80 | 126]) + struct.pack('>H', n)
        else:
            head += bytes([0x80 | 127]) + struct.pack('>Q', n)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(head + mask + masked)

    def send(self, text: str):
        self._send_frame(0x1, text.encode('utf-8'))

    def _read(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise WebSocketError('connection closed by the server')
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def recv(self) -> str:
        """The next complete TEXT message. Answers pings; raises on close."""
        parts = []
        while True:
            b1, b2 = self._read(2)
            if not parts:
                self.msg_first = time.monotonic()
            fin, opcode = b1 & 0x80, b1 & 0x0F
            n = b2 & 0x7F
            if n == 126:
                n = struct.unpack('>H', self._read(2))[0]
            elif n == 127:
                n = struct.unpack('>Q', self._read(8))[0]
            if b2 & 0x80:
                mask = self._read(4)
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(self._read(n)))
            else:
                data = self._read(n)
            if opcode == 0x9:                   # ping
                self._send_frame(0xA, data)
                continue
            if opcode == 0xA:                   # pong
                continue
            if opcode == 0x8:                   # close
                raise WebSocketError('closed by the server: ' + data[2:].decode('utf-8', 'replace'))
            parts.append(data)
            if fin:
                self.msg_bytes = sum(len(x) for x in parts)
                return b''.join(parts).decode('utf-8', 'replace')
