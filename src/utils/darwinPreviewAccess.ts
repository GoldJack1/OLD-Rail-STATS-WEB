import type { User } from 'firebase/auth'
import { isMasterPublishUser } from './masterPublishPolicy'

/** Signed-in viewers who may use Darwin departures / units, but not station admin tools. */
export const DARWIN_PREVIEW_EMAILS = [
  'hello@emilychomicz.com',
  '04richwin@gmail.com',
] as const

export function normalizeEmail(email: string | null | undefined): string {
  return String(email || '').trim().toLowerCase()
}

export function isDarwinPreviewUser(user: User | null | undefined): boolean {
  const email = normalizeEmail(user?.email)
  return email.length > 0 && (DARWIN_PREVIEW_EMAILS as readonly string[]).includes(email)
}

/** Full station/admin UI (owner). */
export function isFullSiteAdmin(user: User | null | undefined): boolean {
  return isMasterPublishUser(user ?? null)
}

export function postLoginPath(user: User | null | undefined): string {
  if (isDarwinPreviewUser(user)) return '/departures'
  return '/stations'
}

export function canUseDarwinTools(user: User | null | undefined): boolean {
  return isFullSiteAdmin(user) || isDarwinPreviewUser(user)
}
