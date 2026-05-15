import { redirect } from 'next/navigation'

// Pre-V7.1 analytics dashboard. Replaced by Falcon V7.1 + the operator dashboard
// at /falcon. Original page preserved at page.tsx.legacy-backup.
export default function AnalysisRedirect() {
  redirect('/falcon')
}
