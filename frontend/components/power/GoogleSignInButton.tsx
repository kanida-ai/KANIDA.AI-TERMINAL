'use client'

/**
 * GoogleSignInButton — wraps Google Identity Services (GIS) "Sign in with Google".
 *
 * Why GIS not NextAuth: operator-locked criterion is "Google sign-in posts
 * id_token to /api/power/auth/google — uses our backend's verify path, not a
 * homegrown flow." GIS gives us exactly that — an id_token signed by Google
 * — which we POST straight to our backend. No extra layer, no session adapter.
 *
 * Flow:
 *   1. Loads GIS script asynchronously
 *   2. Renders Google's official button (their style, their accessibility)
 *   3. On user select-account, callback fires with id_token (Google JWT)
 *   4. Parent's `onCredential(id_token)` handles the rest
 *
 * Parent is responsible for:
 *   - Calling /api/power/auth/google (via exchangeGoogleIdToken)
 *   - Routing to /power/today on OK, /power/redeem on NEEDS_INVITE
 *   - Storing JWT in HTTPOnly cookie via /api/power/session
 */
import { useEffect, useRef } from 'react'
import Script from 'next/script'

// GIS types (minimal — we only use the bits we need)
type GsiCredentialResponse = {
  credential:   string   // the Google ID token (JWT)
  select_by?:   string
}

declare global {
  interface Window {
    google?: {
      accounts: {
        id: {
          initialize: (config: {
            client_id:     string
            callback:      (resp: GsiCredentialResponse) => void
            auto_select?:  boolean
            ux_mode?:      'popup' | 'redirect'
          }) => void
          renderButton: (parent: HTMLElement, options: {
            type?:    'standard' | 'icon'
            theme?:   'outline' | 'filled_blue' | 'filled_black'
            size?:    'small' | 'medium' | 'large'
            text?:    'signin_with' | 'signup_with' | 'continue_with' | 'signin'
            shape?:   'rectangular' | 'pill'
            logo_alignment?: 'left' | 'center'
            width?:   number
          }) => void
        }
      }
    }
  }
}


type Props = {
  /** Fires on successful Google sign-in with the id_token. */
  onCredential: (idToken: string) => void
  /** Fires if GIS script fails to load or Google config is missing. */
  onError?: (msg: string) => void
}

export function GoogleSignInButton({ onCredential, onError }: Props) {
  const buttonRef = useRef<HTMLDivElement | null>(null)
  const clientId = process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID

  useEffect(() => {
    if (!clientId) {
      onError?.('Google Sign-In not configured. Contact admin.')
      return
    }
    let cancelled = false

    const tryInit = () => {
      if (cancelled) return
      if (!window.google?.accounts?.id) {
        // Script still loading — retry shortly
        setTimeout(tryInit, 100)
        return
      }
      window.google.accounts.id.initialize({
        client_id: clientId,
        ux_mode:   'popup',
        callback:  (resp) => {
          if (resp.credential) onCredential(resp.credential)
        },
      })
      if (buttonRef.current) {
        window.google.accounts.id.renderButton(buttonRef.current, {
          type:    'standard',
          theme:   'filled_black',
          size:    'large',
          text:    'signin_with',
          shape:   'rectangular',
          width:   280,
        })
      }
    }
    tryInit()
    return () => { cancelled = true }
  }, [clientId, onCredential, onError])

  return (
    <>
      <Script
        src="https://accounts.google.com/gsi/client"
        strategy="afterInteractive"
        onError={() => onError?.('Failed to load Google Sign-In')}
      />
      <div ref={buttonRef} aria-label="Sign in with Google" />
      {!clientId && (
        <p className="text-xs text-red-300 mt-2">
          <code>NEXT_PUBLIC_GOOGLE_CLIENT_ID</code> not configured.
          Set it in <code>frontend/.env.local</code>.
        </p>
      )}
    </>
  )
}
