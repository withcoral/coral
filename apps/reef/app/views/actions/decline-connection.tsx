import { useEffect } from 'react'
import { useFetcher } from 'react-router'
import { Button } from '@/wax/components'
import { addToast } from '@/wax/components/toast'
import { useShellCsrf } from '@/lib/use-shell-csrf'
import type { action as resolve } from '@/routes/activity-resolve'

/** Declines one exact Account Connection Request. A failure shows in a toast. */
export function DeclineConnection({
  request,
  ariaLabel,
  disabled = false,
}: {
  request: string
  /** Names the request when "Decline" alone does not, as in a list. */
  ariaLabel?: string
  disabled?: boolean
}) {
  const csrf = useShellCsrf()
  const decline = useFetcher<typeof resolve>()
  // Each finished attempt is a new object, so a repeated failure toasts again.
  const finished = decline.state === 'idle' ? decline.data : undefined
  useEffect(() => {
    if (finished && 'error' in finished) addToast('error', { title: finished.error })
  }, [finished])
  return (
    <decline.Form method="post" action="/activity/resolve">
      <input type="hidden" name="csrf" value={csrf} />
      <input type="hidden" name="name" value={request} />
      <input type="hidden" name="intent" value="decline_connection" />
      <Button.TextButton
        type="submit"
        variant="secondary"
        ariaLabel={ariaLabel}
        disabled={disabled || decline.state !== 'idle'}
      >
        Decline
      </Button.TextButton>
    </decline.Form>
  )
}
