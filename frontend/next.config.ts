import type { NextConfig } from "next";

/**
 * Same-origin API access for the Power-User Portal.
 *
 * Browsers behind a Cloudflared tunnel can't reach the operator's
 * localhost:8001 — so without a rewrite, every client-side fetch silently
 * fails. The rewrite makes /api/power/* a same-origin path the tunnel
 * already handles; lib/power-api.ts uses '' as the API base in the browser
 * which combines with the path to give a same-origin relative URL.
 *
 * In production (Vercel), NEXT_PUBLIC_API_URL is set to the Railway domain
 * and this rewrite is a no-op (frontend just hits the absolute URL).
 */
const BACKEND_ORIGIN = process.env.BACKEND_ORIGIN || "http://127.0.0.1:8001";

const nextConfig: NextConfig = {
  // Next.js 16 dev mode blocks cross-origin client requests to _next/* assets
  // and route handlers by default. The Cloudflared tunnel serves us under a
  // different host, so without this allowlist the Random-Replay click silently
  // fails (and so does any other client-side fetch).  Comma-separated list,
  // wildcards supported.
  allowedDevOrigins: [
    "*.trycloudflare.com",
    "10.0.0.192",            // LAN IP for direct phone access
    "192.168.*",
    "127.0.0.1",
    "localhost",
  ],

  async rewrites() {
    return [
      // Power-user public + authed surfaces
      { source: "/api/power/:path*",         destination: `${BACKEND_ORIGIN}/api/power/:path*` },
      { source: "/api/power-auth/:path*",    destination: `${BACKEND_ORIGIN}/api/power-auth/:path*` },
      // Falcon operator surface (used by /falcon/* admin pages)
      { source: "/api/falcon/:path*",        destination: `${BACKEND_ORIGIN}/api/falcon/:path*` },
      // Legacy quant endpoints under /api/admin, /api/quant, etc.
      { source: "/api/admin/:path*",         destination: `${BACKEND_ORIGIN}/api/admin/:path*` },
      // Agent Builder (v0) — same-origin proxy so the browser reaches the backend.
      { source: "/api/builder/:path*",       destination: `${BACKEND_ORIGIN}/api/builder/:path*` },
    ];
  },
};

export default nextConfig;
