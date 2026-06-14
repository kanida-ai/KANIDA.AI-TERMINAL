/**
 * /legal/privacy — Privacy Policy.
 *
 * Static content page. Prose source of truth: docs/launch/legal/privacy.md
 * (edit there first, mirror here). DRAFT — pending legal review.
 *
 * M6 (Legal).
 */
import { LegalArticle, Section } from '../_components'

export const dynamic = 'force-static'

export default function PrivacyPage() {
  return (
    <LegalArticle eyebrow="Legal" title="Privacy Policy" updated="Last updated: [DATE] · Effective: [DATE]">
      <p className="text-sm md:text-base text-neutral-300 leading-relaxed">
        This Privacy Policy explains how [LEGAL ENTITY NAME] (&quot;Kanida.AI&quot;, &quot;we&quot;,
        &quot;us&quot;) collects, uses, shares, and protects your personal data when you use
        the Kanida.AI Platform. We act as a{' '}
        <strong className="text-neutral-100">Data Fiduciary</strong> under the Digital
        Personal Data Protection Act, 2023 (&quot;DPDP Act&quot;) and process data in
        accordance with the Information Technology Act, 2000 and the rules
        thereunder.
      </p>

      <Section heading="1. Data we collect">
        <ul className="list-disc list-inside space-y-1.5 marker:text-mint-500/60">
          <li><strong className="text-neutral-100">Account data:</strong> your email address and any display name you provide.</li>
          <li><strong className="text-neutral-100">Authentication data:</strong> invite codes, Google sign-in identifier (where you choose Google sign-in), and session tokens.</li>
          <li><strong className="text-neutral-100">Billing data:</strong> subscription status and payment references. Payments are processed by Razorpay; we do not store your full card, UPI, or bank account details.</li>
          <li><strong className="text-neutral-100">Usage data:</strong> pages viewed, features used, and basic technical metadata (e.g. IP address, device/browser type, timestamps in IST) for security, rate-limiting, and product improvement.</li>
        </ul>
      </Section>

      <Section heading="2. How we use your data">
        <p>
          We use your data to: create and manage your account; provide and improve
          the Platform; process subscriptions and payments; communicate
          transactional messages (e.g. welcome, payment receipts, payment-failure
          notices); secure the Platform and prevent abuse; and comply with legal
          obligations. We rely on your consent and on legitimate-use grounds
          permitted under the DPDP Act.
        </p>
      </Section>

      <Section heading="3. We do not sell your data">
        <p>
          We do not sell your personal data. We do not send marketing email to
          founding members, and we send commercial communications only where
          permitted.
        </p>
      </Section>

      <Section heading="4. Who we share data with">
        <p>
          We share data only with service providers who help us run the Platform,
          under contractual data-protection obligations, including: our payment
          processor (Razorpay), our email/transactional provider, our hosting and
          database provider, and, where required, law-enforcement or regulators in
          response to lawful requests. The specific processors are listed at
          [PROCESSORS PAGE / on request].
        </p>
      </Section>

      <Section heading="5. Where your data is stored">
        <p>
          Your data is hosted with our infrastructure provider in [REGION]. Where
          data is transferred or stored outside India, we do so in accordance with
          applicable law, including the transfer rules under the DPDP Act.
        </p>
      </Section>

      <Section heading="6. How long we keep it">
        <p>
          We retain personal data for as long as your account is active and
          thereafter only as long as necessary for legitimate business or legal
          purposes (for example, billing and tax records for [RETENTION PERIOD]).
          When no longer needed, data is deleted or anonymised.
        </p>
      </Section>

      <Section heading="7. Your rights">
        <p>
          Subject to applicable law, you may request access to, correction of, or
          erasure of your personal data, and may withdraw consent. To exercise these
          rights, or to raise a grievance, contact our{' '}
          <strong className="text-neutral-100">Grievance Officer</strong>: [GRIEVANCE
          OFFICER NAME], [GRIEVANCE OFFICER EMAIL]. We will respond within the
          timelines required by law.
        </p>
      </Section>

      <Section heading="8. Children">
        <p>
          The Platform is intended for users aged 18 and above. We do not knowingly
          collect personal data from children. If you believe a child has provided
          us data, contact us and we will delete it.
        </p>
      </Section>

      <Section heading="9. Security">
        <p>
          We use reasonable technical and organisational safeguards (including
          encryption in transit, access controls, and rate-limiting) to protect your
          data. No method of transmission or storage is fully secure, and we cannot
          guarantee absolute security.
        </p>
      </Section>

      <Section heading="10. Cookies and similar technologies">
        <p>
          We use strictly necessary cookies/session tokens to keep you signed in and
          to secure the Platform. We do not use third-party advertising trackers.
        </p>
      </Section>

      <Section heading="11. Changes to this Policy">
        <p>
          We may update this Policy. Material changes will be notified through the
          Platform or by email, and the &quot;Last updated&quot; date above will change.
        </p>
      </Section>

      <Section heading="12. Contact">
        <p>Privacy questions: [SUPPORT EMAIL]. Grievances: [GRIEVANCE OFFICER EMAIL].</p>
      </Section>
    </LegalArticle>
  )
}
