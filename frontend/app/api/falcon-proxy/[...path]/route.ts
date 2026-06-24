/**
 * Falcon operator API proxy — same-origin, server-side, behind site Basic Auth.
 *
 * The Falcon console (/falcon/*) talks to the live execution backend
 * (/api/falcon/trade/*). Those endpoints are gated by an operator token
 * (FALCON_OPERATOR_TOKEN). We must NOT ship that token to the browser, so the
 * console calls this same-origin proxy instead of api.kanida.ai directly:
 *
 *   browser  →  /api/falcon-proxy/<path>   (Vercel, behind HTTP Basic Auth)
 *            →  this route handler injects X-Operator-Token (server-only env)
 *            →  ${NEXT_PUBLIC_API_URL}/<path>   (backend)
 *
 * Because the middleware keeps /api/falcon-proxy behind Basic Auth (it is NOT
 * in POWER_PORTAL_PATHS), only an authenticated operator session can reach it,
 * and the secret never leaves the server. This adds AUTH transport only — it
 * forwards method/query/body verbatim and changes no order logic.
 */
import { NextRequest, NextResponse } from 'next/server'

export const dynamic = 'force-dynamic'

const BACKEND = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8001'
const TOKEN = process.env.FALCON_OPERATOR_TOKEN || ''

async function forward(req: NextRequest, path: string[]): Promise<NextResponse> {
  const target = `${BACKEND}/${path.join('/')}${req.nextUrl.search}`

  const headers: Record<string, string> = { 'X-Operator-Token': TOKEN }
  const ct = req.headers.get('content-type')
  if (ct) headers['content-type'] = ct

  const init: RequestInit = { method: req.method, headers, cache: 'no-store' }
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    init.body = await req.text()
  }

  try {
    const r = await fetch(target, init)
    const body = await r.text()
    return new NextResponse(body, {
      status: r.status,
      headers: { 'content-type': r.headers.get('content-type') || 'application/json' },
    })
  } catch {
    return NextResponse.json({ error: 'proxy_unreachable' }, { status: 502 })
  }
}

type Ctx = { params: Promise<{ path: string[] }> }

export async function GET(req: NextRequest, ctx: Ctx)    { return forward(req, (await ctx.params).path) }
export async function POST(req: NextRequest, ctx: Ctx)   { return forward(req, (await ctx.params).path) }
export async function PATCH(req: NextRequest, ctx: Ctx)  { return forward(req, (await ctx.params).path) }
export async function PUT(req: NextRequest, ctx: Ctx)    { return forward(req, (await ctx.params).path) }
export async function DELETE(req: NextRequest, ctx: Ctx) { return forward(req, (await ctx.params).path) }
