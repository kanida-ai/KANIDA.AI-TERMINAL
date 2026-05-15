'use client'
import { useEffect, useRef, useState } from 'react'
import { FalconAPI } from '../../lib/falcon-api'

type Item = Awaited<ReturnType<typeof FalconAPI.inbox>>['items'][number]

export function InboxBell() {
  const [open, setOpen]   = useState(false)
  const [items, setItems] = useState<Item[]>([])
  const [unread, setUnread] = useState(0)
  const ref = useRef<HTMLDivElement>(null)

  const refresh = () => {
    FalconAPI.inbox(20, false).then(d => {
      setItems(d.items); setUnread(d.unread)
    }).catch(() => {})
  }
  useEffect(() => {
    refresh()
    const t = setInterval(refresh, 30000)
    return () => clearInterval(t)
  }, [])

  useEffect(() => {
    const onClick = (e: MouseEvent) => {
      if (open && ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('click', onClick)
    return () => document.removeEventListener('click', onClick)
  }, [open])

  const markAllRead = async () => {
    const ids = items.filter(i => i.status !== 'read').map(i => i.id)
    if (!ids.length) return
    await FalconAPI.inboxMarkRead(ids)
    refresh()
  }

  return (
    <div ref={ref} className="relative">
      <button onClick={() => setOpen(o => !o)}
              className="relative px-2 py-1 text-neutral-300 hover:text-neutral-100">
        🔔
        {unread > 0 && (
          <span className="absolute -top-0.5 -right-0.5 bg-amber-500 text-black
                            text-[10px] rounded-full px-1.5 py-px font-bold">
            {unread}
          </span>
        )}
      </button>
      {open && (
        <div className="absolute right-0 top-full mt-1 w-96 max-h-[28rem] overflow-y-auto
                         bg-neutral-900 border border-neutral-800 rounded shadow-lg z-50">
          <div className="px-3 py-2 border-b border-neutral-800 flex items-center justify-between">
            <strong className="text-sm">Notifications</strong>
            {unread > 0 && (
              <button onClick={markAllRead} className="text-xs text-amber-400 hover:underline">
                Mark all read
              </button>
            )}
          </div>
          {items.length === 0 && (
            <div className="text-neutral-500 text-sm p-4 text-center">No notifications.</div>
          )}
          {items.map(i => (
            <div key={i.id} className={`p-3 border-b border-neutral-800 text-sm
                                          ${i.status !== 'read' ? 'bg-amber-500/5' : ''}`}>
              <div className="flex items-baseline justify-between">
                <strong className="text-neutral-100">{i.subject}</strong>
                <span className="text-xs text-neutral-500">
                  {new Date(i.created_at + 'Z').toLocaleString()}
                </span>
              </div>
              {i.payload?.kind === 'falcon_signals' && i.payload.top10 && (
                <div className="mt-1 text-xs text-neutral-300">
                  Top:{' '}
                  {i.payload.top10.slice(0, 5).map(t => (
                    <span key={t.symbol} className="mr-2">
                      <strong>{t.symbol}</strong>
                      <span className="text-neutral-500"> ({t.score.toFixed(0)})</span>
                    </span>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
