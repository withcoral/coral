import { useState, type ReactNode } from 'react'
import { useFetcher, useRevalidator } from 'react-router'
import { Button, List, Typography } from '@/wax/components'
import { CodeChip } from '@/wax/components/code-chip'
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

/** Actions the Run's list shows before you ask for more. */
const FIRST_ACTIONS = 5

/** The review of one exact request. When no current request is selected, the Run lists
 * its first five current actions, each opening its own review. */
export function RunActions({
  context,
  callStartedAt,
  connectionContext,
  page,
  request,
  unavailable,
  href,
  pageHref,
  returnTo,
}: {
  /** The Run the actions belong to; an approval review shows it as context. */
  context: RunDetail
  /** When the call an approval review decides started, if that call is loaded. */
  callStartedAt?: string
  /** The Run, as context under a selected connection request's header. */
  connectionContext?: ReactNode
  page?: ActionPage
  request?: RequestDetail | null
  unavailable?: boolean
  href: (name: string) => string
  /** The Run's list at the page after `cursor`, or at its first page. */
  pageHref: (cursor: string | null) => string
  returnTo: string
}) {
  const [expanded, setExpanded] = useState(false)
  const actions = page?.actions ?? []
  const shown = expanded ? actions : actions.slice(0, FIRST_ACTIONS)
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
          callStartedAt={callStartedAt}
        />
      )}
      {request?.kind === 'account_connection' && (
        <ConnectionAction
          request={request}
          providerLabel={actions.find((item) => item.name === request.name)?.providerLabel}
          returnTo={returnTo}
        >
          {connectionContext}
        </ConnectionAction>
      )}
      {!current && page && page.total !== '0' && (
        <nav className={styles.stack} aria-label="Actions in this run">
          <Typography.BodyStrong>
            {`${page.total} ${page.total === '1' ? 'action needs' : 'actions need'} you.`}
          </Typography.BodyStrong>
          {/* This page of actions, in the cards a search result uses. */}
          <List.Container as="ul">
            {shown.map((item) => {
              const approval = item.kind === 'approval'
              const target = approval
                ? item.callPath || 'tool'
                : `Connect your ${item.providerLabel || 'provider'} account`
              return (
                <List.Item as="li" key={item.name}>
                  <div className={styles.action}>
                    {approval ? <CodeChip>{target}</CodeChip> : <List.Title>{target}</List.Title>}
                    <Button.InternalLink
                      variant="secondary"
                      size="small"
                      to={href(item.name)}
                      preventScrollReset
                      aria-label={approval ? `Review ${target}` : target}
                    >
                      {approval ? 'Review' : 'Connect'}
                    </Button.InternalLink>
                  </div>
                  <div className={styles.actionMeta}>
                    <Typography.BodySmall variant="tertiary">
                      {approval ? 'approval' : 'account connection'}
                    </Typography.BodySmall>
                    <Deadline at={item.deadline} variant="tertiary" />
                  </div>
                </List.Item>
              )
            })}
          </List.Container>
          {(page.after || shown.length < Number(page.total)) && (
            <Typography.BodySmall variant="secondary">
              {page.after
                ? `Showing ${shown.length} later actions of ${page.total}.`
                : `Showing ${shown.length} of ${page.total} actions.`}
            </Typography.BodySmall>
          )}
          {(actions.length > FIRST_ACTIONS || page.after || page.next) && (
            <div className={styles.controls}>
              {actions.length > FIRST_ACTIONS && (
                <Button.TextButton
                  variant="secondary"
                  size="32"
                  aria-expanded={expanded}
                  onClick={() => setExpanded(!expanded)}
                >
                  {expanded ? 'Show fewer actions' : 'Show more actions'}
                </Button.TextButton>
              )}
              {page.after && (
                <Button.InternalLink variant="bare" to={pageHref(null)} preventScrollReset>
                  First actions
                </Button.InternalLink>
              )}
              {/* The next page follows the last action on this one, so it waits until all show. */}
              {shown.length === actions.length && page.next && (
                <Button.InternalLink variant="bare" to={pageHref(page.next)} preventScrollReset>
                  More actions in this run
                </Button.InternalLink>
              )}
            </div>
          )}
          {page.incomplete && <MissingActions loaded={actions.length} total={page.total} />}
        </nav>
      )}
    </section>
  )
}

/** A page of the Run's actions failed to load: say how many are missing, and read them again. */
function MissingActions({ loaded, total }: { loaded: number; total: string }) {
  const revalidator = useRevalidator()
  return (
    <div className={styles.controls}>
      <Typography.BodySmall role="status">
        {loaded} of {total} actions loaded. The rest did not load.
      </Typography.BodySmall>
      <Button.TextButton
        variant="secondary"
        size="32"
        disabled={revalidator.state !== 'idle'}
        onClick={() => revalidator.revalidate()}
      >
        Try again
      </Button.TextButton>
    </div>
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

/** A selected connection request leads the panel, as an approval does: its header with the
 * deadline, then `children` (its Run, as context), then Connect or Decline. One connection can
 * unblock several waiting calls, so it is never labelled as one call's approval. */
function ConnectionAction({
  request,
  providerLabel,
  returnTo,
  children,
}: {
  request: Extract<RequestDetail, { kind: 'account_connection' }>
  providerLabel?: string
  returnTo: string
  children?: ReactNode
}) {
  const csrf = useShellCsrf()
  const connected = useActivityConnected()
  const decline = useFetcher<typeof resolve>({ key: request.name })
  const provider = providerLabel || 'provider'
  const archived = useActivityArchived()
  const pending = request.state === 'pending'
  const header = (
    <DetailHeader
      // A settled request has left the pending list, which names its provider.
      title={providerLabel ? `Connect your ${providerLabel} account` : 'Connect your account'}
      status={request.state}
      kind="account connection request"
      heading="h3"
      meta={pending && request.expiresAt && <Deadline at={request.expiresAt} variant="tertiary" />}
    />
  )
  if (request.state !== 'pending')
    return (
      <div className={styles.stack}>
        {header}
        {children}
        <Typography.Body role="status">{settled[request.state]}</Typography.Body>
      </div>
    )
  return (
    <div className={styles.stack}>
      {/* A shared Account Connection Request is not this one call's Approval. */}
      {header}
      {children}
      {archived ? (
        <Typography.Body role="status">
          {restoreFirst(archived, `connect your ${provider} account`)}
        </Typography.Body>
      ) : (
        <>
          <Typography.BodySmall>Connect your {provider} account to continue.</Typography.BodySmall>
          {/* Like an approval: the secondary choice leads, the primary one ends. */}
          <PanelFooter>
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
