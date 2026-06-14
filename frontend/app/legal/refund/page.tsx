/**
 * /legal/refund — Refund & Cancellation Policy.
 *
 * Static content page. Prose source of truth: docs/launch/legal/refund.md
 * (edit there first, mirror here). DRAFT — pending legal review.
 *
 * Reflects the ₹999/mo monthly India subscription (CONTRACT plan_code
 * monthly_999) and the M3 cancel behaviour (access until current_end).
 *
 * M6 (Legal).
 */
import Link from 'next/link'
import { LegalArticle, Section } from '../_components'

export const dynamic = 'force-static'

export default function RefundPage() {
  return (
    <LegalArticle eyebrow="Legal" title="Refund & Cancellation Policy" updated="Last updated: [DATE] · Effective: [DATE]">
      <p className="text-sm md:text-base text-neutral-300 leading-relaxed">
        This Refund &amp; Cancellation Policy applies to paid subscriptions to the
        Kanida.AI Platform operated by [LEGAL ENTITY NAME] (&quot;Kanida.AI&quot;, &quot;we&quot;,
        &quot;us&quot;). It should be read together with our{' '}
        <Link href="/legal/terms" className="text-mint-400 hover:text-mint-300 underline">
          Terms of Service
        </Link>.
      </p>

      <Section heading="1. The plan">
        <p>
          The Kanida.AI subscription is <strong className="text-neutral-100">₹999 per
          month (inclusive of applicable taxes unless stated otherwise), billed
          monthly</strong> and <strong className="text-neutral-100">auto-renewing</strong>{' '}
          until you cancel. Each payment buys access for one (1) monthly billing
          cycle. Payments are processed in India by Razorpay.
        </p>
      </Section>

      <Section heading="2. Cancelling your subscription">
        <p>
          You may cancel at any time from your account billing page (or by
          contacting support). When you cancel:
        </p>
        <ul className="list-disc list-inside space-y-1.5 marker:text-mint-500/60">
          <li><strong className="text-neutral-100">No further charges</strong> are made after the current cycle.</li>
          <li><strong className="text-neutral-100">Your access continues until the end of the billing cycle you have already paid for.</strong> You are not cut off immediately.</li>
          <li>After that date, paid features are disabled until you resubscribe.</li>
        </ul>
      </Section>

      <Section heading="3. Refunds">
        <p>
          Because access is delivered immediately and continuously for the full
          cycle you have paid for,{' '}
          <strong className="text-neutral-100">we do not provide pro-rata refunds for the
          unused portion of a billing cycle</strong> once that cycle has started.
          Cancelling stops the next renewal; it does not refund the current month.
        </p>
        <p>
          We may, at our discretion, issue a refund in cases of: a duplicate or
          erroneous charge; a charge after a valid cancellation; or a documented
          billing error on our side. Approved refunds are returned to the original
          payment method via Razorpay, typically within 5–7 business days (timing
          depends on your bank or payment provider).
        </p>
      </Section>

      <Section heading="4. Free (founding / comp) accounts">
        <p>
          Founding members and invite-based complimentary (&quot;comp&quot;) accounts are{' '}
          <strong className="text-neutral-100">free</strong>. There are no charges on these
          accounts and therefore nothing to refund.
        </p>
      </Section>

      <Section heading="5. Failed or missed payments">
        <p>
          If an auto-renewal payment fails, we may retry the charge and/or notify
          you. If payment is not completed, your subscription may lapse and paid
          access may be suspended until payment succeeds. We do not charge you for a
          cycle you did not receive access to.
        </p>
      </Section>

      <Section heading="6. Disputes and chargebacks">
        <p>
          If you believe you were charged in error, please contact us first at
          [SUPPORT EMAIL] so we can resolve it quickly. Raising a chargeback with
          your bank without contacting us may delay resolution and may result in
          suspension of your account pending investigation.
        </p>
      </Section>

      <Section heading="7. Changes to this Policy">
        <p>
          We may update this Policy on prospective notice. Changes do not
          retroactively affect a cycle already paid for.
        </p>
      </Section>

      <Section heading="8. Contact">
        <p>Refund or billing questions: [SUPPORT EMAIL].</p>
      </Section>
    </LegalArticle>
  )
}
