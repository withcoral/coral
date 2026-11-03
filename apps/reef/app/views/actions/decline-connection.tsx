import { useFetcher } from 'react-router'
import { Button } from '@/wax/components'
import { useShellCsrf } from '@/lib/use-shell-csrf'
import type { action as resolve } from '@/routes/activity-resolve'

/** One fetcher per request, so a view can show a decline's outcome away from its button. */
const useDecline = (request: string) => useFetcher<typeof resolve>({ key: `decline:${request}` })

/** Declines one exact Account Connection Request. */
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
  const decline = useDecline(request)
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

/** Why the last decline of `request` failed, or null. */
export function useDeclineFailure(request: string): string | null {
  const decline = useDecline(request)
  return decline.state === 'idle' && decline.data && 'error' in decline.data
    ? decline.data.error
    : null
}
