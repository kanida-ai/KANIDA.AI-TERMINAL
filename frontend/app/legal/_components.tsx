/**
 * Shared presentational pieces for the /legal/* pages.
 *
 * Server-only, no client JS. Co-located under app/legal/ with a leading
 * underscore so Next.js treats it as a private folder (not a route).
 *
 * M6 (Legal).
 */
import type { ReactNode } from 'react'

/**
 * The MANDATORY content-marking banner. Every legal page must render this at
 * the very top, visibly, so no draft is mistaken for a binding document.
 */
export function DraftBanner() {
  return (
    <div
      role="alert"
      className="mb-8 rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3
                 text-sm text-amber-200"
    >
      <span className="font-semibold">DRAFT — pending legal review.</span>{' '}
      Not yet legally binding.
    </div>
  )
}

/** Page header: eyebrow + title + "last updated" line. */
export function LegalHeader({
  eyebrow,
  title,
  updated,
}: {
  eyebrow: string
  title: string
  updated: string
}) {
  return (
    <header className="mb-8">
      <p className="text-xs tracking-[0.2em] uppercase text-mint-300 mb-2">{eyebrow}</p>
      <h1 className="text-3xl md:text-4xl font-bold tracking-tight leading-tight">{title}</h1>
      <p className="mt-3 text-xs text-neutral-500">{updated}</p>
    </header>
  )
}

/** A titled section of legal prose. */
export function Section({ heading, children }: { heading: string; children: ReactNode }) {
  return (
    <section className="space-y-3">
      <h2 className="text-lg md:text-xl font-bold text-neutral-100">{heading}</h2>
      <div className="space-y-3 text-sm md:text-base text-neutral-300 leading-relaxed">
        {children}
      </div>
    </section>
  )
}

/**
 * Wrapper for a legal document: constrains width, renders the mandatory draft
 * banner, the header, and the body sections, then a footer note.
 */
export function LegalArticle({
  eyebrow,
  title,
  updated,
  children,
}: {
  eyebrow: string
  title: string
  updated: string
  children: ReactNode
}) {
  return (
    <article className="max-w-3xl mx-auto py-2 md:py-6">
      <DraftBanner />
      <LegalHeader eyebrow={eyebrow} title={title} updated={updated} />
      <div className="space-y-8">{children}</div>
      <p className="mt-12 pt-6 border-t border-neutral-800 text-xs text-neutral-600 italic">
        This page is a draft template pending legal review and is not legal advice.
      </p>
    </article>
  )
}
