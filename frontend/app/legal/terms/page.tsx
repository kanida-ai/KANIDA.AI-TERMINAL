/**
 * /legal/terms — Terms of Service.
 *
 * Static content page. Prose source of truth: docs/launch/legal/terms.md
 * (edit there first, mirror here). DRAFT — pending legal review.
 *
 * M6 (Legal).
 */
import Link from 'next/link'
import { LegalArticle, Section } from '../_components'

export const dynamic = 'force-static'

export default function TermsPage() {
  return (
    <LegalArticle eyebrow="Legal" title="Terms of Service" updated="Last updated: [DATE] · Effective: [DATE]">
      <p className="text-sm md:text-base text-neutral-300 leading-relaxed">
        These Terms of Service (&quot;Terms&quot;) govern your access to and use of the
        Kanida.AI website, applications, and services (collectively, the
        &quot;Platform&quot;), operated by [LEGAL ENTITY NAME] (&quot;Kanida.AI&quot;, &quot;we&quot;, &quot;us&quot;,
        or &quot;our&quot;). By creating an account, subscribing, or otherwise using the
        Platform, you agree to these Terms. If you do not agree, do not use the
        Platform.
      </p>

      <Section heading="1. What Kanida.AI is — and is not">
        <p>
          Kanida.AI is an <strong className="text-neutral-100">information, analytics, and
          educational technology platform</strong>. It surfaces algorithmically-derived,
          historically-validated market patterns and ranked observations about
          Indian equities for your independent consideration.
        </p>
        <p>
          <strong className="text-neutral-100">Kanida.AI is not an investment adviser,
          research analyst, stock broker, portfolio manager, or any other
          SEBI-registered intermediary, and does not hold itself out as one.</strong>{' '}
          Nothing on the Platform constitutes a personal recommendation,
          solicitation, or offer to buy, sell, or hold any security, nor
          investment, financial, legal, accounting, or tax advice. All content is
          general in nature and does not account for your individual financial
          situation, objectives, or risk tolerance. You are solely responsible for
          every trading and investment decision you make. See our{' '}
          <Link href="/legal/risk" className="text-mint-400 hover:text-mint-300 underline">
            Risk Disclosure
          </Link>{' '}
          for the full statement.
        </p>
      </Section>

      <Section heading="2. Eligibility">
        <p>
          You may use the Platform only if you are at least 18 years of age, are a
          resident of India, and are legally capable of entering into a binding
          contract. By using the Platform you represent that you meet these
          requirements. The Platform is intended for use within India and is not
          directed at persons in any jurisdiction where such use would be contrary
          to law or regulation.
        </p>
      </Section>

      <Section heading="3. Your account">
        <p>
          You agree to provide accurate information, to keep your login credentials
          confidential, and to be responsible for all activity under your account.
          Notify us promptly of any unauthorised use. We may suspend or terminate
          accounts that violate these Terms or that we reasonably believe are being
          used unlawfully.
        </p>
      </Section>

      <Section heading="4. Performance figures and &quot;Live&quot; content">
        <p>
          Any performance figures, backtests, walk-forward simulations, win rates,
          hit rates, or &quot;Live&quot; persona portfolios shown on the Platform are{' '}
          <strong className="text-neutral-100">model or simulated track records derived
          from historical and/or paper-traded data, not the results of actual
          capital deployed on your behalf.</strong> They are illustrative of how the
          engine has behaved on past data and are subject to the assumptions, costs,
          and limitations described alongside them.{' '}
          <strong className="text-neutral-100">Past performance is not indicative of, and
          does not guarantee, future results.</strong> You must not treat any such
          figure as a forecast or a promise of return.
        </p>
      </Section>

      <Section heading="5. Subscriptions, fees, and billing">
        <p>
          Access to certain features requires a paid subscription. The launch plan
          is <strong className="text-neutral-100">₹999 per month (inclusive of applicable
          taxes unless stated otherwise), billed monthly and auto-renewing</strong>{' '}
          until you cancel. Payments are processed by our third-party payment
          partner (Razorpay); we do not store your full card details. By subscribing
          you authorise recurring charges in accordance with the payment partner&apos;s
          mandate. Fees, plans, and features may change on prospective notice.
          Refunds are governed by our{' '}
          <Link href="/legal/refund" className="text-mint-400 hover:text-mint-300 underline">
            Refund Policy
          </Link>.
        </p>
      </Section>

      <Section heading="6. Acceptable use">
        <p>
          You agree not to: (a) scrape, resell, redistribute, or commercially
          exploit Platform content without our written consent; (b)
          reverse-engineer, copy, or attempt to extract the underlying models,
          patterns, or data; (c) share your account or circumvent the paywall; (d)
          use the Platform for any unlawful, manipulative, or market-abusive
          purpose; or (e) interfere with the Platform&apos;s security or operation.
        </p>
      </Section>

      <Section heading="7. Intellectual property">
        <p>
          The Platform, including its software, models, pattern library, content,
          and branding, is owned by Kanida.AI or its licensors and is protected by
          applicable intellectual-property laws. We grant you a limited,
          non-exclusive, non-transferable, revocable licence to access the Platform
          for your personal, non-commercial use during your subscription.
        </p>
      </Section>

      <Section heading="8. Third-party services">
        <p>
          The Platform may rely on or link to third-party services (e.g. payment
          processors, brokers, data vendors). We are not responsible for the
          content, policies, or actions of those third parties, and your use of them
          is governed by their own terms.
        </p>
      </Section>

      <Section heading="9. Disclaimers">
        <p>
          The Platform is provided <strong className="text-neutral-100">&quot;as is&quot; and &quot;as
          available&quot;, without warranties of any kind</strong>, express or implied,
          including as to accuracy, completeness, timeliness, merchantability, or
          fitness for a particular purpose. We do not warrant that the content is
          error-free, that any pattern will recur, or that any outcome will be
          achieved. Market data may be delayed, incomplete, or inaccurate.
        </p>
      </Section>

      <Section heading="10. Limitation of liability and indemnity">
        <p>
          To the maximum extent permitted by law, Kanida.AI and its officers,
          employees, and affiliates shall not be liable for any trading or
          investment losses, lost profits, or any indirect, incidental, special, or
          consequential damages arising from your use of the Platform. Our aggregate
          liability for any claim shall not exceed the total subscription fees you
          paid to us in the three (3) months preceding the event giving rise to the
          claim. You agree to indemnify and hold us harmless from claims arising out
          of your breach of these Terms or your use of the Platform.
        </p>
      </Section>

      <Section heading="11. Suspension and termination">
        <p>
          You may cancel your subscription at any time per the Refund Policy. We may
          suspend or terminate your access, with or without notice, for breach of
          these Terms or to comply with law. Provisions that by their nature should
          survive termination (including Sections 7, 9, 10, and 13) will survive.
        </p>
      </Section>

      <Section heading="12. Changes to these Terms">
        <p>
          We may update these Terms from time to time. Material changes will be
          notified through the Platform or by email. Continued use after changes
          take effect constitutes acceptance.
        </p>
      </Section>

      <Section heading="13. Governing law and dispute resolution">
        <p>
          These Terms are governed by the laws of India. Subject to applicable law,
          the courts at [CITY, STATE] shall have exclusive jurisdiction over any
          dispute [or disputes shall be resolved by arbitration seated at [CITY]
          under the Arbitration and Conciliation Act, 1996 — to be confirmed by
          counsel].
        </p>
      </Section>

      <Section heading="14. Contact">
        <p>Questions about these Terms: [SUPPORT EMAIL].</p>
      </Section>
    </LegalArticle>
  )
}
