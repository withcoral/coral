import { useEffect, useRef } from 'react'
import { useLocation, useNavigate } from 'react-router'
import { addToast, type ToastValues } from '@/wax/components/toast'

/**
 * A redirect that finishes an action lands with a flag in the query. Say so
 * once in a success toast, then drop the flags so a reload or Back does not
 * repeat it. `toast` returns null for a flag the page does not announce.
 */
export function useArrivalToast<Flag extends string>(
  flags: readonly Flag[],
  toast: (flag: Flag) => ToastValues | null,
) {
  const location = useLocation()
  const navigate = useNavigate()
  const announced = useRef<string | null>(null)
  const params = new URLSearchParams(location.search)
  const arrival = flags.find((flag) => params.has(flag)) ?? null
  useEffect(() => {
    if (!arrival || announced.current === location.key) return
    announced.current = location.key
    const values = toast(arrival)
    if (values) addToast('success', values)
    const search = new URLSearchParams(location.search)
    for (const flag of flags) search.delete(flag)
    const rest = search.toString()
    void navigate(
      { pathname: location.pathname, search: rest ? `?${rest}` : '', hash: location.hash },
      { replace: true, preventScrollReset: true },
    )
  }, [arrival, flags, toast, location, navigate])
}
