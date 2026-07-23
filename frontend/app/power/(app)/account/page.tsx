/** /power/account — thin shell (profile / broker / persona land here in later phases). */
import { LaunchPending } from '@/components/power/LaunchPending'

export const dynamic = 'force-dynamic'

export default function AccountPage() {
  return (
    <LaunchPending
      title="Account"
      summary="Your profile, trading persona and broker connection — the settings that personalize Falcon to how you trade."
      willDo={[
        'Pick and switch your trading persona (Falcon Top 10 Swing is live today).',
        'Connect your broker for Co-Trading and AutoTrade (when those go live).',
        'Manage profile details and notification preferences.',
      ]}
      expect={[
        'Persona choice changes which signals and risk controls you see.',
        'Your broker connection is only ever used with your explicit action.',
      ]}
      whyNotYet="Profile and persona settings move here as Co-Trading, AutoTrade and the persona picker come online. For now sign-out and basic account info live in the nav."
    />
  )
}
