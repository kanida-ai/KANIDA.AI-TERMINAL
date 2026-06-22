/** /power/learn — thin shell (how Falcon works, the engine, the tiers). */
import { LaunchPending } from '@/components/power/LaunchPending'

export const dynamic = 'force-dynamic'

export default function LearnPage() {
  return (
    <LaunchPending
      title="Learn Falcon"
      summary="How Falcon works under the hood: the pattern-mining and walk-forward validation pipeline, what the quality tiers mean, and how to read a signal."
      willDo={[
        'Explain the engine: how patterns are mined and validated out-of-sample.',
        'Define each quality tier as a qualitative band, with honest caveats.',
        'Teach how to read a signal card — why-selected, evidence, sector, risk.',
      ]}
      expect={[
        'Plain language, explainability-led — no hype, no “certified” claims.',
        'Short, skimmable lessons rather than a manual.',
      ]}
      whyNotYet="The learning content is being written to match the live engine exactly, so it stays accurate as the product evolves."
    />
  )
}
