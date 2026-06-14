/**
 * /legal/risk — Risk Disclosure (SEBI-aware).
 *
 * Static content page. Prose source of truth: docs/launch/legal/risk.md
 * (edit there first, mirror here). DRAFT — pending legal review.
 *
 * THE key SEBI page: states informational/educational only, NOT investment
 * advice, past performance != future results, and that the "Live" persona
 * portfolios are MODEL / simulated track records (not real client capital).
 * Intended to also be shown at signup with checkbox consent (built in M7).
 *
 * M6 (Legal).
 */
import { LegalArticle, Section } from '../_components'

export const dynamic = 'force-static'

export default function RiskPage() {
  return (
    <LegalArticle eyebrow="Legal" title="Risk Disclosure" updated="Last updated: [DATE] · Effective: [DATE]">
      <p className="text-sm md:text-base text-neutral-200 leading-relaxed font-semibold">
        Please read this disclosure carefully before using Kanida.AI. Trading and
        investing in securities involves substantial risk. By using the Platform
        you acknowledge and accept the risks described below.
      </p>

      <Section heading="1. Informational and educational only — not investment advice">
        <p>
          Kanida.AI is an <strong className="text-neutral-100">information, analytics, and
          educational platform</strong>. The content it provides — including pattern
          observations, ranked lists, scores, and narratives about why the engine
          &quot;noticed&quot; a stock — is{' '}
          <strong className="text-neutral-100">general information for your independent
          consideration only.</strong> It is{' '}
          <strong className="text-neutral-100">NOT investment advice, NOT a research report
          prepared for you, and NOT a personal recommendation</strong> to buy, sell,
          or hold any security. It does not take into account your personal
          financial situation, objectives, or risk appetite.{' '}
          <strong className="text-neutral-100">You alone decide what to do with your money,
          and you are solely responsible for your trades.</strong>
        </p>
      </Section>

      <Section heading="2. We are not a SEBI-registered intermediary">
        <p>
          <strong className="text-neutral-100">Kanida.AI is not registered with the
          Securities and Exchange Board of India (SEBI) as an investment adviser,
          research analyst, stock broker, or portfolio manager, and does not act in
          any such capacity.</strong> We do not manage your money, hold your funds or
          securities, or execute trades on your behalf. Any trades you choose to
          place are executed by you through your own broker.
        </p>
      </Section>

      <Section heading="3. Past performance is not future results">
        <p>
          <strong className="text-neutral-100">Past performance is not indicative of, and
          provides no guarantee of, future results.</strong> Markets change. A pattern
          that worked historically may not work again. Different market regimes can
          and do break strategies that previously performed well.
        </p>
      </Section>

      <Section heading="4. &quot;Live&quot;, backtest, and simulated figures are MODEL track records">
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-4">
          <p>
            All performance figures shown on the Platform — including{' '}
            <strong className="text-neutral-100">win rates, hit rates, backtests,
            walk-forward simulations, and the &quot;Live&quot; persona portfolios</strong> — are{' '}
            <strong className="text-neutral-100">model, simulated, or hypothetical track
            records derived from historical and/or paper-traded data.</strong> They are{' '}
            <strong className="text-neutral-100">NOT returns earned on real customer capital
            and NOT a record of trades placed for you.</strong> Hypothetical and
            simulated results have inherent limitations: they are constructed with
            the benefit of hindsight, may not reflect the impact of real-world
            trading costs, slippage, liquidity, taxes, or your own behaviour, and{' '}
            <strong className="text-neutral-100">no representation is made that any account
            will or is likely to achieve results similar to those shown.</strong>{' '}
            Headline figures (for example, long-run walk-forward growth numbers shown
            on our credibility page) are illustrations of past, simulated engine
            behaviour, not a forecast or promise.
          </p>
        </div>
      </Section>

      <Section heading="5. No guarantee of returns; you can lose capital">
        <p>
          <strong className="text-neutral-100">No assurance or guarantee of any return is
          made.</strong> The value of investments can go down as well as up.{' '}
          <strong className="text-neutral-100">You may lose some or all of your invested
          capital.</strong> Only invest money you can afford to lose. Leverage,
          derivatives, and concentrated positions can increase losses beyond your
          initial outlay.
        </p>
      </Section>

      <Section heading="6. Market, data, and technology risks">
        <p>
          Market data may be delayed, incomplete, or inaccurate. The Platform&apos;s
          models, scores, and outputs may contain errors or fail in unforeseen
          market conditions. Technical issues (outages, latency, bugs) may affect
          availability or content. None of these reduce your responsibility for your
          own decisions.
        </p>
      </Section>

      <Section heading="7. Do your own research and seek advice">
        <p>
          Before acting on any information from the Platform, consider doing your own
          research and consulting a SEBI-registered investment adviser or other
          qualified professional who can account for your personal circumstances.
        </p>
      </Section>

      <Section heading="8. Your acknowledgement">
        <p>
          By creating an account and using the Platform, you confirm that you have
          read, understood, and accepted this Risk Disclosure, that you understand
          Kanida.AI does not provide investment advice, and that all trading and
          investment decisions — and their consequences — are entirely your own.
        </p>
      </Section>
    </LegalArticle>
  )
}
