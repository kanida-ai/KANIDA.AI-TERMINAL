"""GDF adapter — offline, against a scripted socket that behaves the way the live trial key did on 21 Sep 2026."""
import json
from datetime import date, datetime

import pytest

from market_data.gdf_provider import GdfProvider, exchange_of, gdf_identifier
from market_data.types import IST, ProviderError, TokenError

KEY = 'secret-key-never-in-an-error'


class ScriptedSocket:
    """Replies like the server: after auth it pushes permission frames and heartbeats before anything else."""

    def __init__(self, url, answer, auth_ok=True):
        self.answer, self.auth_ok, self.sent, self.queue = answer, auth_ok, [], []

    def connect(self):
        return self

    def send(self, text):
        msg = json.loads(text)
        self.sent.append(msg)
        if msg['MessageType'] == 'Authenticate':
            self.queue += [{'Complete': self.auth_ok, 'Message': 'Welcome!' if self.auth_ok else 'Invalid key',
                            'MessageType': 'AuthenticateResult'},
                           {'AllowVMRunning': False, 'MessageType': 'AllowVMRunningResult'},
                           {'AllowServerOSRunning': False, 'MessageType': 'AllowServerOSRunningResult'},
                           {'MessageType': 'Echo'}]
        else:
            self.queue += [{'MessageType': 'Echo'}] + self.answer(msg)

    def recv(self):
        item = self.queue.pop(0)
        return item if isinstance(item, str) else json.dumps(item)

    def close(self):
        pass


def ts(h, m, d=21):
    return int(datetime(2026, 9, d, h, m, tzinfo=IST).timestamp())


def history_answer(bars):
    def answer(msg):
        if msg['MessageType'] == 'GetHistory':
            return [{'Request': msg, 'Result': bars}]
        return [{'PaketID': 30, 'Message': 'Function not enabled.', 'MessageType': 'RequestError'}]
    return answer


def provider(answer, auth_ok=True):
    return GdfProvider(url='wss://example/', api_key=KEY,
                       socket_factory=lambda u: ScriptedSocket(u, answer, auth_ok), reply_timeout=2)


def bar(h, m, close=100.0, oi=1000, d=21):
    return {'LastTradeTime': ts(h, m, d), 'QuotationLot': 1, 'TradedQty': 10, 'OpenInterest': oi,
            'Open': close, 'High': close + 1, 'Low': close - 1, 'Close': close}


def test_identifiers_are_built_from_contract_fields():
    assert gdf_identifier('NIFTY', 'CE', date(2026, 9, 22), 23400) == 'OPTIDX_NIFTY_22SEP2026_CE_23400'
    assert gdf_identifier('RELIANCE', 'PE', date(2026, 9, 29), 1240.0) == 'OPTSTK_RELIANCE_29SEP2026_PE_1240'
    assert gdf_identifier('WIPRO', 'CE', date(2026, 9, 29), 167.5) == 'OPTSTK_WIPRO_29SEP2026_CE_167.5'
    assert gdf_identifier('BANKNIFTY', 'FUT', date(2026, 9, 29)) == 'FUTIDX_BANKNIFTY_29SEP2026_XX_0'
    assert gdf_identifier('NIFTY', 'EQ') == 'NIFTY 50' and gdf_identifier('RELIANCE', 'EQ') == 'RELIANCE'
    assert exchange_of('OPTIDX_NIFTY_22SEP2026_CE_23400') == 'NFO' and exchange_of('NIFTY-I') == 'NFO'
    assert exchange_of('NIFTY BANK') == 'NSE_IDX' and exchange_of('RELIANCE') == 'NSE'


def test_replies_are_matched_by_type_not_by_arrival_order():
    p = provider(history_answer([bar(9, 15)]))
    rows = p.history('RELIANCE', '15minute', datetime(2026, 9, 21, 9, 0, tzinfo=IST), datetime(2026, 9, 21, 15, 30, tzinfo=IST))
    assert len(rows) == 1, 'heartbeats and permission pushes are never taken for the answer'
    assert p.permissions['AllowVMRunningResult']['AllowVMRunning'] is False
    assert p.permissions['AllowServerOSRunningResult']['AllowServerOSRunning'] is False


def test_bar_timestamps_are_bar_starts_and_come_back_oldest_first():
    p = provider(history_answer([bar(9, 30, 101), bar(9, 15, 100)]))
    rows = p.history('RELIANCE', '15minute', datetime(2026, 9, 21, 9, 0, tzinfo=IST), datetime(2026, 9, 21, 10, 0, tzinfo=IST))
    assert [r['bar_start'].strftime('%H:%M') for r in rows] == ['09:15', '09:30']
    assert rows[0]['OpenInterest'] == 1000, 'history() keeps open interest for F&O callers'


def test_candles_are_session_bars_only():
    """21 Sep: the vendor served a 15:30-start post-close bar, and NSE_IDX served flat 'bars' at 20:45-21:15."""
    bars = [bar(9, 15), bar(15, 15), bar(15, 30), bar(20, 45), bar(21, 0)]
    p = provider(history_answer(bars))
    got = p.candles('NIFTY 50', '15minute', datetime(2026, 9, 21, 9, 15, tzinfo=IST), datetime(2026, 9, 21, 15, 30, tzinfo=IST))
    assert [c.bar_start.strftime('%H:%M') for c in got] == ['09:15', '15:15']
    assert all(c.vendor_id == 'gdf' and c.exchange == 'NSE_IDX' for c in got)


def test_a_disabled_function_is_an_error_not_an_empty_answer():
    p = provider(history_answer([]))
    with pytest.raises(ProviderError, match='Function not enabled'):
        p.call({'MessageType': 'GetLastQuote'}, lambda j: j.get('MessageType') == 'LastQuoteResult')


def test_a_refused_key_is_a_token_error_that_never_repeats_the_key():
    p = provider(history_answer([]), auth_ok=False)
    with pytest.raises(TokenError) as e:
        p.limitation()
    assert KEY not in str(e.value)


def test_a_plain_text_diagnostic_is_surfaced():
    p = provider(lambda m: ['Access Denied. Key already in use by other session.'])
    with pytest.raises(ProviderError, match='Key already in use'):
        p.limitation()


def test_the_provider_declares_what_the_trial_measured():
    p = provider(history_answer([]))
    assert p.provider_id == 'gdf' and p.delay_seconds == 900 and p.rate_limit_per_second == 1.0
    assert p.supported_timeframes == ('15minute',), 'DAY and HOUR are disabled on the trial'


def test_niftyfpi_is_an_index_family_underlying():
    """22 Sep mapping check: 163 NIFTYFPI options and its future missed because it was spelled as a stock."""
    assert gdf_identifier('NIFTYFPI', 'CE', date(2026, 9, 29), 1300) == 'OPTIDX_NIFTYFPI_29SEP2026_CE_1300'
    assert gdf_identifier('NIFTYFPI', 'FUT', date(2026, 9, 29)) == 'FUTIDX_NIFTYFPI_29SEP2026_XX_0'
