import { redirect } from 'next/navigation'

// Root URL — Falcon V7.1 is the active surface. Landing page on hold pending
// designer review (the AI-rebuild attempt was rejected; awaiting Pudhuraja's
// own UI). Backup attempts preserved at:
//   app/welcome/LandingPage.tsx           (v2 — single-viewport invention attempt)
//   app/welcome/LandingPage.v1-saas-backup.tsx  (v1 — 13-section SaaS template)
//   app/engine/EnginePage.tsx             (deep-dive engine page)
// These are no longer routed — they're parked on disk for future use.
export default function Home() {
  redirect('/falcon')
}
