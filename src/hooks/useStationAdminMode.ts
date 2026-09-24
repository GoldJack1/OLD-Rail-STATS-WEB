import { useCallback, useSyncExternalStore } from 'react'
import { useLocation } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'
import { isDarwinPreviewUser } from '../utils/darwinPreviewAccess'
import {
  isStationAdminModeActive,
  STATION_ADMIN_MODE_CHANGED_EVENT,
} from '../utils/stationAdminModeStorage'

/** True when a signed-in user has admin mode enabled (persisted or `?admin=1`). */
export function useStationAdminMode(): boolean {
  const { user, loading } = useAuth()
  const { search } = useLocation()

  const subscribe = useCallback((onStoreChange: () => void) => {
    window.addEventListener(STATION_ADMIN_MODE_CHANGED_EVENT, onStoreChange)
    return () => window.removeEventListener(STATION_ADMIN_MODE_CHANGED_EVENT, onStoreChange)
  }, [])

  const getSnapshot = useCallback(() => isStationAdminModeActive(search), [search])

  const adminActive = useSyncExternalStore(subscribe, getSnapshot, () => false)

  if (loading || !user) return false
  if (isDarwinPreviewUser(user)) return false
  return adminActive
}
