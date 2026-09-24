import React, { useEffect, useState } from 'react'
import { Navigate, useLocation } from 'react-router-dom'
import { reload } from 'firebase/auth'
import { useAuth } from '../../../contexts/AuthContext'
import { initializeFirebase, getFirebaseAuth } from '../../../services/firebase'
import { userMustEnrollTotpMfaOnFirebase } from '../../../services/firebaseTotpMfa'
import { canUseDarwinTools, isDarwinPreviewUser, isFullSiteAdmin } from '../../../utils/darwinPreviewAccess'

interface ProtectedRouteProps {
  children: React.ReactNode
  /** Station editor / design system — not Darwin preview accounts. */
  requireFullAdmin?: boolean
  /** Departures tools, bash, API status — owner or Darwin preview accounts. */
  requireDarwinTools?: boolean
}

type ProfileCheck = 'idle' | 'checking' | 'ok' | 'need-email-verify' | 'need-totp-enroll'

const isLocalDevLoginBypassEnabled =
  import.meta.env.DEV && import.meta.env.VITE_LOCAL_DEV_LOGIN_BYPASS === 'true'

/**
 * Requires a signed-in user with verified email and TOTP (authenticator) MFA enrolled.
 */
const ProtectedRoute: React.FC<ProtectedRouteProps> = ({ children, requireFullAdmin = false, requireDarwinTools = false }) => {
  if (isLocalDevLoginBypassEnabled) {
    return <>{children}</>
  }

  const { user, loading } = useAuth()
  const location = useLocation()
  const [profileCheck, setProfileCheck] = useState<ProfileCheck>('idle')

  useEffect(() => {
    if (loading) return

    if (!user) {
      setProfileCheck('idle')
      return
    }

    let cancelled = false
    setProfileCheck('checking')

    void (async () => {
      try {
        await initializeFirebase()
        const u = getFirebaseAuth()?.currentUser
        if (cancelled) return
        if (!u) {
          setProfileCheck('need-email-verify')
          return
        }
        try {
          await reload(u)
        } catch {
          /* still check with cached user */
        }
        if (cancelled) return

        if (!u.emailVerified) {
          setProfileCheck('need-email-verify')
          return
        }
        if (userMustEnrollTotpMfaOnFirebase(u)) {
          setProfileCheck('need-totp-enroll')
          return
        }
        setProfileCheck('ok')
      } catch {
        if (!cancelled) setProfileCheck('need-email-verify')
      }
    })()

    return () => {
      cancelled = true
    }
  }, [user, loading])

  if (loading || (user && profileCheck === 'checking')) {
    return (
      <div
        style={{
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          minHeight: '200px',
          fontSize: '18px',
          color: 'var(--text-secondary)'
        }}
      >
        Loading…
      </div>
    )
  }

  if (!user) {
    return <Navigate to="/log-in" state={{ from: location }} replace />
  }

  if (profileCheck === 'need-email-verify') {
    return <Navigate to="/log-in" replace state={{ from: location, reason: 'verify-email' as const }} />
  }

  if (profileCheck === 'need-totp-enroll') {
    return <Navigate to="/log-in" replace state={{ from: location, reason: 'enroll-totp' as const }} />
  }

  if (profileCheck !== 'ok') {
    return (
      <div
        style={{
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          minHeight: '200px',
          fontSize: '18px',
          color: 'var(--text-secondary)'
        }}
      >
        Loading…
      </div>
    )
  }

  if (requireDarwinTools && !canUseDarwinTools(user)) {
    return <Navigate to={isDarwinPreviewUser(user) ? '/departures' : '/log-in'} replace />
  }

  if (requireFullAdmin && !isFullSiteAdmin(user)) {
    return <Navigate to={isDarwinPreviewUser(user) ? '/departures' : '/log-in'} replace />
  }

  return <>{children}</>
}

export default ProtectedRoute
