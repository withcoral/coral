import type { ReactNode } from 'react'
import { useFetcher } from 'react-router'
import { Button, Typography } from '@/wax/components'
import { Deadline } from '@/wax/components/deadline'
import { useShellCsrf } from '@/lib/use-shell-csrf'
import type { action as resolve } from '@/routes/activity-resolve'
import { ConnectButton } from '../accounts/connect-button'
import { ApprovalReview } from './review'
import { PanelFooter } from '../activity/panel'
import { DetailHeader } from '../activity/detail-header'
import { useActivityArchived, useActivityConnected, restoreFirst } from '../activity/live'
import type { ActionPage, RequestDetail, RunDetail } from '../activity/types'
import type { ConnectionState } from '../accounts/types'
import * as styles from './actions.css'

/** Your own account page for a provider, returning to `returnTo` after you connect. */
export function connectHref(provider: string, returnTo: string) {
  const [, workspace, , id] = provider.split('/')
  return `/workspaces/${workspace}/accounts/${id}?${new URLSearchParams({ returnTo })}`
}

/** The review of one exact request. When no current request is selected, the Run lists
 * its current actions, each opening its own review. */
export function RunActions({
  context,
  runControls,
  page,
  request,
  unavailable,
  href,
  returnTo,
  firstHref,
  nextHref,
}: {
  /** The Run the actions belong to; an approval review shows it as context. */
  context: RunDetail
  /** Run-level links and controls an approval review shows in its technical details. */
  runControls?: ReactNode
  page?: ActionPage
  request?: RequestDetail | null
  unavailable?: boolean
  href: (name: string) => string
  returnTo: string
  firstHref?: string
  nextHref?: string
}) {
  const actions = page?.actions ?? []
  // A settled or unrelated selection still shows what the Run waits on now.
  const current = request ? actions.some((item) => item.name === request.name) : false
  if ((!page || page.total === '0') && !request && !unavailable) return null
  return (
    <section className={styles.stack} aria-label="Run actions">
      {unavailable && <Typography.Body role="status">This request is unavailable.</Typography.Body>}
      {request?.kind === 'approval' && (
        <ApprovalReview
          key={request.name}
          detail={request}
          context={context}
          runControls={runControls}
        />
      )}
      {request?.kind === 'account_connection' && (
        <ConnectionAction
          request={request}
          providerLabel={actions.find((item) => item.name === request.name)?.providerLabel}
          returnTo={returnTo}
        />
      )}
      {!current && page && page.total !== '0' && (
        <nav className={styles.stack} aria-label="Actions in this run">
          <Typography.BodyStrong>
            {`${page.total} ${page.total === '1' ? 'action needs' : 'actions need'} you.`}
          </Typography.BodyStrong>
          {actions.map((item) => (
            <div key={item.name} className={styles.controls}>
              <Button.InternalLink variant="secondary" to={href(item.name)} preventScrollReset>
                {item.kind === 'approval'
                  ? `Review ${item.callPath || 'operation'}`
                  : `Connect your ${item.providerLabel || 'provider'} account`}
              </Button.InternalLink>
              <Deadline at={item.deadline} />
            </div>
          ))}
          <div className={styles.controls}>
            {firstHref && (
              <Button.InternalLink variant="bare" to={firstHref} preventScrollReset>
                First actions
              </Button.InternalLink>
            )}
            {nextHref && (
              <Button.InternalLink variant="bare" to={nextHref} preventScrollReset>
                More actions in this run
              </Button.InternalLink>
            )}
          </div>
        </nav>
      )}
    </section>
  )
}

const settled: Record<Exclude<ConnectionState, 'pending'>, string> = {
  connected: 'Account connected. Coral continues after its other checks.',
  declined: 'Connection declined. Coral does not send the waiting calls.',
  expired: 'This connection expired. Coral does not send the waiting calls.',
  route_changed: 'The account setting changed. Coral does not switch accounts for waiting calls.',
  authorization_changed:
    'The account authorization changed. Coral does not switch accounts for waiting calls.',
}

/** Connect in one click, or decline to fail the waiting calls now. */
function ConnectionAction({
  request,
  providerLabel,
  returnTo,
}: {
  request: Extract<RequestDetail, { kind: 'account_connection' }>
  providerLabel?: string
  returnTo: string
}) {
  const csrf = useShellCsrf()
  const connected = useActivityConnected()
  const decline = useFetcher<typeof resolve>({ key: request.name })
  const provider = providerLabel || 'provider'
  const archived = useActivityArchived()
  if (request.state !== 'pending')
    return <Typography.Body role="status">{settled[request.state]}</Typography.Body>
  return (
    <div className={styles.stack}>
      {/* A shared Account Connection Request is not this one call's Approval. */}
      <DetailHeader
        title={`Connect your ${provider} account`}
        status={request.state}
        kind="account connection request"
        sticky={false}
        heading="h3"
      />
      {archived ? (
        <Typography.Body role="status">
          {restoreFirst(archived, `connect your ${provider} account`)}
        </Typography.Body>
      ) : (
        <>
          <Typography.BodySmall>
            Connect your {provider} account to continue. One connection can unblock several waiting
            calls.
          </Typography.BodySmall>
          {/* Like an approval: the deadline and the secondary choice lead, the primary one ends. */}
          <PanelFooter>
            {request.expiresAt && <Deadline at={request.expiresAt} />}
            <decline.Form method="post" action="/activity/resolve">
              <input type="hidden" name="csrf" value={csrf} />
              <input type="hidden" name="name" value={request.name} />
              <input type="hidden" name="intent" value="decline_connection" />
              <Button.TextButton
                type="submit"
                variant="secondary"
                disabled={!connected || decline.state !== 'idle'}
              >
                Decline
              </Button.TextButton>
            </decline.Form>
            <ConnectButton
              action={connectHref(request.provider, returnTo)}
              csrf={csrf}
              label={`Connect ${provider}`}
              disabled={!connected}
            />
          </PanelFooter>
        </>
      )}
    </div>
  )
}
