/**
 * Service worker for KANIDA.AI Web Push (Sprint 5c-1 Layer 2).
 *
 * Registered once from /power/admin when the operator clicks "Enable push".
 * Handles two events:
 *   - 'push'              → render a notification from the JSON payload
 *   - 'notificationclick' → focus an existing tab or open the magic-link URL
 *
 * Payload shape (from backend/power_user/services/web_push.notify_auth_needed):
 *   { title: string, body: string, url: string, tag?: string }
 */

self.addEventListener('install', (event) => {
  // Take over immediately on first install so the first push works without reload
  event.waitUntil(self.skipWaiting())
})

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim())
})

self.addEventListener('push', (event) => {
  let payload = { title: 'KANIDA.AI', body: 'New notification', url: '/power/admin' }
  if (event.data) {
    try {
      payload = { ...payload, ...event.data.json() }
    } catch (_e) {
      payload.body = event.data.text() || payload.body
    }
  }

  const options = {
    body:        payload.body,
    icon:        '/next.svg',
    badge:       '/next.svg',
    tag:         payload.tag || 'kanida-default',
    requireInteraction: true,           // Don't auto-dismiss — magic link expires in 15 min
    data:        { url: payload.url || '/power/admin' },
  }

  event.waitUntil(self.registration.showNotification(payload.title, options))
})

self.addEventListener('notificationclick', (event) => {
  event.notification.close()
  const targetUrl = (event.notification.data && event.notification.data.url) || '/power/admin'

  event.waitUntil((async () => {
    const allClients = await self.clients.matchAll({ type: 'window', includeUncontrolled: true })
    // Try to focus an existing tab on the same origin
    for (const client of allClients) {
      try {
        const u = new URL(client.url)
        if (u.origin === self.location.origin) {
          await client.focus()
          await client.navigate(targetUrl)
          return
        }
      } catch (_e) { /* skip malformed URLs */ }
    }
    // Otherwise open a new tab
    await self.clients.openWindow(targetUrl)
  })())
})
