import React, { useState, useEffect, useRef } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import {
  signInWithEmailAndPassword,
  reload,
  getMultiFactorResolver,
  multiFactor,
  TotpMultiFactorGenerator,
  type MultiFactorError,
  type MultiFactorResolver,
  type TotpSecret
} from 'firebase/auth'
import QRCode from 'qrcode'
import { useAuth } from '../../contexts/AuthContext'
import { initializeFirebase, getFirebaseAuth, sendUserEmailVerification } from '../../services/firebase'
import {
  getTotpMfaHints,
  isMultiFactorAuthRequiredError,
  mapTotpMfaError,
  TOTP_ISSUER_NAME,
  userMustEnrollTotpMfaOnFirebase
} from '../../services/firebaseTotpMfa'
import { BUTWideButton } from '../../components/buttons'
import { MFA_AUTOFILL, MFA_OTP_INPUT_NAME } from '../../constants/mfaAutofill'
import './LoginPage.css'
import { postLoginPath } from '../../utils/darwinPreviewAccess'

type LoginStep = 'credentials' | 'verify-email' | 'checking-session' | 'totp-signin' | 'totp-enroll'

const LoginPage: React.FC = () => {
  const { user } = useAuth()
  const navigate = useNavigate()
  const location = useLocation()
  const redirectedForVerify =
    location.state &&
    typeof location.state === 'object' &&
    'reason' in location.state &&
    (location.state as { reason?: string }).reason === 'verify-email'
  const redirectedForTotpEnroll =
    location.state &&
    typeof location.state === 'object' &&
    'reason' in location.state &&
    (location.state as { reason?: string }).reason === 'enroll-totp'

  const [step, setStep] = useState<LoginStep>('credentials')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [info, setInfo] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)

  const mfaResolverRef = useRef<MultiFactorResolver | null>(null)
  const totpSecretRef = useRef<TotpSecret | null>(null)
  const [totpSignInCode, setTotpSignInCode] = useState('')
  const [totpEnrollCode, setTotpEnrollCode] = useState('')
  const [totpQrDataUrl, setTotpQrDataUrl] = useState<string | null>(null)
  const [totpSecretKeyDisplay, setTotpSecretKeyDisplay] = useState<string | null>(null)
  const [totpEnrollLoading, setTotpEnrollLoading] = useState(false)

  useEffect(() => {
    if (step === 'totp-signin') return

    if (!user) {
      setStep('credentials')
      return
    }

    const run = async () => {
      setStep('checking-session')
      await initializeFirebase()
      const auth = getFirebaseAuth()
      const u = auth?.currentUser
      if (!u) {
        setStep('credentials')
        return
      }

      if (!u.emailVerified) {
        setStep('verify-email')
        return
      }

      if (userMustEnrollTotpMfaOnFirebase(u)) {
        setStep('totp-enroll')
        return
      }

      navigate(postLoginPath(u), { replace: true })
    }

    void run()
  }, [user, navigate, step])

  useEffect(() => {
    if (step !== 'totp-enroll' || !user) {
      setTotpQrDataUrl(null)
      setTotpSecretKeyDisplay(null)
      totpSecretRef.current = null
      return
    }

    let cancelled = false
    setTotpEnrollLoading(true)
    setError(null)

    void (async () => {
      try {
        await initializeFirebase()
        const u = getFirebaseAuth()?.currentUser
        if (!u || cancelled) return

        const session = await multiFactor(u).getSession()
        const secret = await TotpMultiFactorGenerator.generateSecret(session)
        if (cancelled) return

        totpSecretRef.current = secret
        setTotpSecretKeyDisplay(secret.secretKey)
        const accountLabel = u.email?.trim() || 'account'
        const uri = secret.generateQrCodeUrl(accountLabel, TOTP_ISSUER_NAME)
        const dataUrl = await QRCode.toDataURL(uri, { width: 220, margin: 2 })
        if (!cancelled) setTotpQrDataUrl(dataUrl)
      } catch (e) {
        if (!cancelled) setError(mapTotpMfaError(e))
      } finally {
        if (!cancelled) setTotpEnrollLoading(false)
      }
    })()

    return () => {
      cancelled = true
      totpSecretRef.current = null
    }
  }, [step, user])

  const mapEmailAuthError = (err: unknown): string => {
    if (err && typeof err === 'object' && 'code' in err) {
      const code = (err as { code: string }).code
      if (code === 'auth/invalid-credential' || code === 'auth/wrong-password' || code === 'auth/user-not-found') {
        return 'Invalid email or password.'
      }
      if (code === 'auth/invalid-email') return 'Please enter a valid email address.'
      if (code === 'auth/unverified-email') {
        return 'Verify your email before signing in. Check your inbox or request a new link below.'
      }
    }
    return err instanceof Error ? err.message : 'Something went wrong.'
  }

  const handleCredentialsSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)
    setInfo(null)
    setSubmitting(true)
    mfaResolverRef.current = null
    setTotpSignInCode('')

    try {
      await initializeFirebase()
      const auth = getFirebaseAuth()
      if (!auth) throw new Error('Authentication is not available.')

      try {
        await signInWithEmailAndPassword(auth, email, password)
        const u = auth.currentUser
        if (!u) throw new Error('Sign-in incomplete.')

        if (!u.emailVerified) {
          setStep('verify-email')
          setSubmitting(false)
          return
        }

        if (userMustEnrollTotpMfaOnFirebase(u)) {
          setStep('totp-enroll')
          setSubmitting(false)
          return
        }

        navigate(postLoginPath(u), { replace: true })
      } catch (err: unknown) {
        if (isMultiFactorAuthRequiredError(err)) {
          const resolver = getMultiFactorResolver(auth, err as MultiFactorError)
          const totpHints = getTotpMfaHints(resolver)
          if (totpHints.length === 0) {
            setError(
              'This account uses a second factor this app does not support (e.g. SMS only). ' +
                'In Firebase Console → Authentication, remove phone MFA or add TOTP for this user.'
            )
            setSubmitting(false)
            return
          }
          mfaResolverRef.current = resolver
          setStep('totp-signin')
          setSubmitting(false)
          return
        }
        throw err
      }
    } catch (err: unknown) {
      setError(mapEmailAuthError(err))
    } finally {
      setSubmitting(false)
    }
  }

  const handleTotpSignInVerify = async () => {
    setError(null)
    const resolver = mfaResolverRef.current
    const auth = getFirebaseAuth()
    if (!resolver || !auth) {
      setError('Session expired. Sign in again.')
      return
    }
    const hints = getTotpMfaHints(resolver)
    const hint = hints[0]
    if (!hint) {
      setError('No authenticator factor found.')
      return
    }
    const code = totpSignInCode.replace(/\s/g, '')
    if (!/^\d{6,8}$/.test(code)) {
      setError('Enter the 6-digit code from your authenticator app.')
      return
    }
    setSubmitting(true)
    try {
      const assertion = TotpMultiFactorGenerator.assertionForSignIn(hint.uid, code)
      await resolver.resolveSignIn(assertion)
      mfaResolverRef.current = null
      setTotpSignInCode('')
      setStep('checking-session')
      navigate(postLoginPath(getFirebaseAuth()?.currentUser), { replace: true })
    } catch (err) {
      setError(mapTotpMfaError(err))
    } finally {
      setSubmitting(false)
    }
  }

  const handleTotpEnrollComplete = async () => {
    setError(null)
    const u = getFirebaseAuth()?.currentUser
    const secret = totpSecretRef.current
    if (!u || !secret) {
      setError('Setup expired. Refresh the page.')
      return
    }
    const code = totpEnrollCode.replace(/\s/g, '')
    if (!/^\d{6,8}$/.test(code)) {
      setError('Enter the code shown in your authenticator app.')
      return
    }
    setSubmitting(true)
    try {
      const assertion = TotpMultiFactorGenerator.assertionForEnrollment(secret, code)
      await multiFactor(u).enroll(assertion, 'Authenticator app')
      totpSecretRef.current = null
      setTotpEnrollCode('')
      setTotpQrDataUrl(null)
      setTotpSecretKeyDisplay(null)
      await reload(u)
      navigate(postLoginPath(u), { replace: true })
    } catch (err) {
      setError(mapTotpMfaError(err))
    } finally {
      setSubmitting(false)
    }
  }

  const handleResendVerification = async () => {
    setError(null)
    setInfo(null)
    const auth = getFirebaseAuth()
    const u = auth?.currentUser
    if (!u) return
    setSubmitting(true)
    try {
      await sendUserEmailVerification(u)
      setInfo('Verification email sent again.')
    } catch (err) {
      setError(mapEmailAuthError(err))
    } finally {
      setSubmitting(false)
    }
  }

  const handleReloadAfterVerify = async () => {
    setError(null)
    const auth = getFirebaseAuth()
    const u = auth?.currentUser
    if (!u) return
    setSubmitting(true)
    try {
      await reload(u)
      if (!u.emailVerified) {
        setError('Email not verified yet. Open the link in your inbox, then try again.')
        setSubmitting(false)
        return
      }
      if (userMustEnrollTotpMfaOnFirebase(u)) {
        setStep('totp-enroll')
      } else {
        navigate(postLoginPath(u), { replace: true })
      }
    } catch (err) {
      setError(mapEmailAuthError(err))
    } finally {
      setSubmitting(false)
    }
  }

  const showCredentials = step === 'credentials' && !user
  const showVerifyEmail = step === 'verify-email'
  const showTotpSignIn = step === 'totp-signin'
  const showTotpEnroll = step === 'totp-enroll'
  const showChecking = step === 'checking-session'

  return (
    <div className="login-page">
      <div className="login-card">
        <h1 className="login-title">
          {showTotpSignIn
            ? 'Authenticator code'
            : showTotpEnroll
              ? 'Set up authenticator'
              : showVerifyEmail
                ? 'Verify your email'
                : 'Log in'}
        </h1>
        <p className="login-subtitle">
          {showTotpSignIn
            ? 'Open your authenticator app and enter the code for this account.'
            : showTotpEnroll
              ? 'Scan the QR code or enter the secret key manually, then enter a code to confirm.'
              : showVerifyEmail
                ? 'Open the link we emailed you, then continue below.'
                : 'Sign in with email and password.'}
        </p>

        {redirectedForVerify && (
          <p className="login-info-banner" role="status">
            Verify your email to access this site.
          </p>
        )}
        {redirectedForTotpEnroll && (
          <p className="login-info-banner" role="status">
            Add an authenticator app before continuing.
          </p>
        )}
        {info && <p className="login-info-banner">{info}</p>}

        {showChecking && (
          <p className="login-subtitle" style={{ marginTop: '1rem' }}>
            Checking your session…
          </p>
        )}

        {showCredentials && (
          <>
            <form onSubmit={handleCredentialsSubmit} className="login-form">
              <div className="login-field">
                <label htmlFor="login-email" className="login-label login-label--credentials">
                  Email
                </label>
                <TXTINPWideButton
                  id="login-email"
                  type="email"
                  value={email}
                  onInputChange={(e) => setEmail(e.target.value)}
                  placeholder="you@example.com"
                  autoComplete="email"
                  required
                  colorVariant="secondary"
                />
              </div>
              <div className="login-field">
                <label htmlFor="login-password" className="login-label login-label--credentials">
                  Password
                </label>
                <TXTINPWideButton
                  id="login-password"
                  type="password"
                  value={password}
                  onInputChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••"
                  autoComplete="current-password"
                  required
                  minLength={6}
                  colorVariant="secondary"
                />
              </div>
              {error && (
                <p className="login-error" role="alert">
                  {error}
                </p>
              )}
              <BUTWideButton
                type="submit"
                width="fill"
                className="login-submit"
                colorVariant="accent"
                disabled={submitting || email.trim().length === 0 || password.length === 0}
              >
                {submitting ? 'Please wait…' : 'Log in'}
              </BUTWideButton>
            </form>
          </>
        )}

        {showTotpSignIn && (
          <form
            id="login-totp-signin-form"
            className="login-mfa-flow"
            method="post"
            action="#"
            onSubmit={(e) => {
              e.preventDefault()
              void handleTotpSignInVerify()
            }}
            autoComplete="on"
          >
            {error && (
              <p className="login-error" role="alert">
                {error}
              </p>
            )}
            {/* Links OTP autofill (Apple Passwords, Chrome/Edge password managers, etc.) to this sign-in */}
            <input
              className="autofill-bridge-input"
              type="email"
              name="username"
              autoComplete={MFA_AUTOFILL.signInUsername}
              value={email}
              readOnly
              tabIndex={-1}
              aria-hidden="true"
            />
            <label htmlFor="login-totp-signin" className="login-label login-label--totp-signin">
              6-digit code
            </label>
            <TXTINPWideButton
              id="login-totp-signin"
              name={MFA_OTP_INPUT_NAME}
              inputMode="numeric"
              autoComplete={MFA_AUTOFILL.signInOtp}
              pattern="[0-9]*"
              autoCapitalize="off"
              autoCorrect="off"
              spellCheck={false}
              enterKeyHint="done"
              maxLength={8}
              value={totpSignInCode}
              onInputChange={(e) => setTotpSignInCode(e.target.value.replace(/\D/g, '').slice(0, 8))}
              placeholder="123456"
              disabled={submitting}
            
                colorVariant="secondary"
              />
            <div className="login-phone-verify-actions login-phone-verify-actions--inline">
              <BUTWideButton
                type="button"
                ariaLabel="Back to login details"
                width="hug"
                shape="left-rounded"
                iconPosition="left"
                icon={
                  <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
                    <path d="M10 3 5 8l5 5" />
                  </svg>
                }
                colorVariant="primary"
                className="login-auth-back-button"
                onClick={() => {
                  mfaResolverRef.current = null
                  setTotpSignInCode('')
                  setStep('credentials')
                  setError(null)
                }}
                disabled={submitting}
              />
              <BUTWideButton
                type="submit"
                width="fill"
                shape="right-rounded"
                className="login-submit"
                colorVariant="accent"
                disabled={submitting || !/^\d{6}$/.test(totpSignInCode.trim())}
              >
                {submitting ? 'Verifying…' : 'Verify code & Log in'}
              </BUTWideButton>
            </div>
          </form>
        )}

        {showTotpEnroll && (
          <div className="login-mfa-flow">
            {error && (
              <p className="login-error" role="alert">
                {error}
              </p>
            )}
            {totpEnrollLoading && <p className="login-subtitle">Preparing setup…</p>}
            {!totpEnrollLoading && totpQrDataUrl && (
              <>
                <div className="login-totp-qr-wrap">
                  <img src={totpQrDataUrl} alt="QR code for authenticator app" className="login-totp-qr" width={220} height={220} />
                </div>
                {totpSecretKeyDisplay && (
                  <p className="login-totp-secret-hint">
                    <span className="login-totp-secret-label">Manual key</span>
                    <code className="login-totp-secret-key">{totpSecretKeyDisplay}</code>
                  </p>
                )}
                <form
                  id="login-totp-enroll-form"
                  className="login-totp-enroll-form"
                  method="post"
                  action="#"
                  onSubmit={(e) => {
                    e.preventDefault()
                    void handleTotpEnrollComplete()
                  }}
                  autoComplete="on"
                >
                  <input
                    className="autofill-bridge-input"
                    type="email"
                    name="username"
                    autoComplete={MFA_AUTOFILL.enrollUsername}
                    value={user?.email ?? ''}
                    readOnly
                    tabIndex={-1}
                    aria-hidden="true"
                  />
                  <label htmlFor="login-totp-enroll" className="login-label">
                    Enter code from the app
                  </label>
                  <TXTINPWideButton
                    id="login-totp-enroll"
                    name={MFA_OTP_INPUT_NAME}
                    inputMode="numeric"
                    autoComplete={MFA_AUTOFILL.enrollOtp}
                    pattern="[0-9]*"
                    autoCapitalize="off"
                    autoCorrect="off"
                    spellCheck={false}
                    enterKeyHint="done"
                    maxLength={8}
                    value={totpEnrollCode}
                    onInputChange={(e) => setTotpEnrollCode(e.target.value.replace(/\D/g, '').slice(0, 8))}
                    placeholder="123456"
                    disabled={submitting}
                  
                colorVariant="secondary"
              />
                  <BUTWideButton
                    type="submit"
                    width="fill"
                    className="login-submit"
                    disabled={submitting || totpEnrollLoading}
                  >
                    {submitting ? 'Saving…' : 'Verify and continue'}
                  </BUTWideButton>
                </form>
              </>
            )}
          </div>
        )}

        {showVerifyEmail && (
          <div className="login-mfa-flow">
            {error && (
              <p className="login-error" role="alert">
                {error}
              </p>
            )}
            {info && <p className="login-info-banner">{info}</p>}
            <p className="login-subtitle" style={{ textAlign: 'left', marginBottom: '1rem' }}>
              You must verify your email before continuing.
            </p>
            <div className="login-phone-verify-actions">
              <BUTWideButton type="button" width="fill" disabled={submitting} onClick={() => void handleResendVerification()}>
                Resend email
              </BUTWideButton>
              <BUTWideButton
                type="button"
                width="fill"
                className="login-submit"
                disabled={submitting}
                onClick={() => void handleReloadAfterVerify()}
              >
                I’ve verified — continue
              </BUTWideButton>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default LoginPage
