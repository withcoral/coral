import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { isRouteErrorResponse, useNavigate, useRouteError } from 'react-router'
import { EmptyPage } from '@/components/empty-page'
import { useFollowRequiredActions } from '@/lib/use-action-signal'
import { useRevalidateWhileVisible } from '@/lib/use-revalidate-while-visible'
import { Button, Dialog, Table, Typography } from '@/wax/components'
import { CodeChip } from '@/wax/components/code-chip'
import { Deadline } from '@/wax/components/deadline'
import { Logo, type LogoIcons } from '@/wax/components/logo'
import { PageLayout } from '@/wax/components/page-layout'
import { toolOwner } from '@/lib/call-root'
import {
  ActionReviewSession,
  ApprovalReview,
  QuickDecision,
  REVIEW_PARAM,
  useCommentGuard,
  useDecisionOutcome,
  useUnsentComment,
} from '../actions/review'
import { ActivityConnectedContext } from '../activity/live'
import { ExecutionSourceSplit } from '../activity/execution-source-split'
import { revealHref } from '../activity/navigation'
import { Panel } from '../activity/panel'
import * as panelStyles from '../activity/panel.css'
import { pendingQuestion } from '../activity/pending'
import type { ApprovalDetail, ApprovalReading, PendingAction } from '../activity/types'
import { trail } from '../management/layout'
import { ManagementErrorBoundary } from '../management/management-error'
import * as styles from './approvals.css'
import type { ProviderMark } from './types'

const RETRY_MS = 10_000

const reviewHref = (name: string) => `?${new URLSearchParams({ [REVIEW_PARAM]: name })}`

/** The list's most urgent request after `decided`, else its nearest before it. */
function nextPending(approvals: PendingAction[], decided: ApprovalDetail) {
  const others = approvals.filter(({ action }) => action.name !== decided.name)
  const deadline = Date.parse(decided.deadline)
  return (others.find(({ action }) => Date.parse(action.deadline) >= deadline) ?? others.at(-1))
    ?.action.name
}

/** One row per pending Approval Request, most urgent first. Arguments stay in the review,
 * which the address opens beside the list.
 * The page follows the shell's approval revision. While that signal is down, it may be stale,
 * so the page says so and takes no decision. */
export function ApprovalsPage({
  approvals,
  workspacePath,
  revision,
  selected = null,
  review = null,
  providers = [],
}: {
  approvals: PendingAction[]
  /** Keys the Workspace's revision. */
  workspacePath: string
  /** The approval revision read before `approvals`, if it was readable. */
  revision?: string
  /** The request the address opens. It grants nothing. */
  selected?: string | null
  /** Coral's read of `selected`, if Coral shows it to you. */
  review?: ApprovalReading | null
  /** The workspace's providers, which name each call by its logo and tool. */
  providers?: ProviderMark[]
}) {
  const table = Table.useRowCursor()
  const live = useFollowRequiredActions(workspacePath, 'approvals', revision)
  return (
    <ActionReviewSession request={review?.detail}>
      <PageLayout variant="split" breadcrumbs={trail([], 'Approvals')}>
        <ActivityConnectedContext value={live}>
          <div className={styles.page}>
            <div className={styles.list}>
              {!live && (
                <Typography.BodySmall role="status">
                  Approvals are unavailable.
                </Typography.BodySmall>
              )}
              <Table.Container
                ariaLabel="Pending approvals"
                ref={table}
                variant="card"
                layout="fixed"
                columns={[
                  {
                    label: approvals.length
                      ? `Waiting on you · ${approvals.length}`
                      : 'Waiting on you',
                    width: { fill: 1 },
                  },
                ]}
              >
                <Table.Head />
                {approvals.length === 0 && live && (
                  <Table.Status>
                    <EmptyPage
                      fullSize={false}
                      iconName="ShieldCheck"
                      title="Nothing to approve."
                      description="Requests that need your approval appear here."
                    />
                  </Table.Status>
                )}
                <Table.Body>
                  {approvals.map((pending) => (
                    <ApprovalRow
                      key={pending.action.name}
                      pending={pending}
                      owner={toolOwner(pending.action.callPath, providers)}
                      open={pending.action.name === selected}
                    />
                  ))}
                </Table.Body>
              </Table.Container>
            </div>
            <ReviewRail
              approvals={approvals}
              selected={selected}
              review={review}
              providerIcons={
                review ? toolOwner(review.detail.callPath, providers)?.provider.icons : undefined
              }
            />
          </div>
        </ActivityConnectedContext>
      </PageLayout>
    </ActionReviewSession>
  )
}

/** The review of the request the address opens. After your decision it moves down the list;
 * a request that someone else settles stays open, showing its outcome. */
function ReviewRail({
  approvals,
  selected,
  review,
  providerIcons,
}: {
  approvals: PendingAction[]
  selected: string | null
  review: ApprovalReading | null
  providerIcons?: LogoIcons
}) {
  const navigate = useNavigate()
  const close = () => void navigate({ search: '' }, { preventScrollReset: true })
  const drawer = Dialog.useCloseAfterExit(selected, close)
  const unsent = useUnsentComment()
  const guarded = useCommentGuard()
  const name = review?.detail.name
  const [decided, setDecided] = useState<{ name?: string; awaits?: string }>()
  useDecisionOutcome(name ?? 'approvals:no-review', ({ state, recorded }) =>
    setDecided({ name, awaits: recorded ? undefined : state }),
  )
  // It holds only for the request you decided: another selection, or coming back, starts over.
  if (decided && decided.name !== name) setDecided(undefined)
  // A decision Coral only holds moves on once the read shows that it won.
  const settled = review && decided && (!decided.awaits || decided.awaits === review.detail.state)
  const moving = Boolean(settled) && !guarded
  const next = review && moving ? nextPending(approvals, review.detail) : undefined
  useEffect(() => {
    if (!moving) return
    // Moving on adds no history entry: Back returns to the list, not a decided request.
    void navigate(next ? reviewHref(next) : { search: '' }, {
      replace: true,
      preventScrollReset: true,
    })
  }, [moving, next, navigate])
  // The rail plays its exit after the address closes it, still showing what it showed.
  const [shown, setShown] = useState(review)
  if (selected && shown !== review) setShown(review)
  const popup = useRef<HTMLDivElement>(null)
  // Another request starts at its top. Its controls replace the ones that held focus.
  useLayoutEffect(() => {
    const rail = popup.current
    if (!selected || !rail) return
    rail.scrollTop = 0
    if (document.activeElement === document.body) rail.focus({ preventScroll: true })
  }, [selected])
  return (
    <Dialog.Rail
      {...drawer}
      docked
      // With an unsent comment, the rail stays while the review asks whether to discard it.
      onOpenChange={(open) => (!open && unsent ? close() : drawer.onOpenChange(open))}
    >
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup
          size="xl"
          placement="right"
          fullWidthOnMobile
          ref={popup}
          // As in Activity: focus goes to the close control, so Escape closes the review.
          initialFocus={() =>
            popup.current?.querySelector<HTMLElement>('button[aria-label="Close review"]') ?? true
          }
        >
          {shown ? (
            <>
              <Dialog.Title visuallyHidden>Review approval</Dialog.Title>
              {/* As in Activity, the Run's source shows where the call came from. */}
              <ExecutionSourceSplit
                key={shown.context.run.name}
                context={shown.context}
                locatable={false}
                selectedRow={shown.call}
                close={
                  <span className={panelStyles.railControls}>
                    {/* The call among its Run's records, where an Agent's call shows on its page. */}
                    <Button.IconLink
                      name="Activity"
                      to={revealHref(shown.call, { request: shown.detail.name })}
                      tooltipText="Open in activity"
                      size="22"
                    />
                    <Dialog.Close ariaLabel="Close review" inline />
                  </span>
                }
              >
                <Panel label={`Review ${shown.detail.callPath}`}>
                  <ApprovalReview
                    key={shown.detail.name}
                    detail={shown.detail}
                    context={shown.context}
                    callStartedAt={shown.call.startedAt}
                    variant="approvals"
                    providerIcons={providerIcons}
                  />
                </Panel>
              </ExecutionSourceSplit>
            </>
          ) : (
            <>
              <Dialog.Header>
                <Dialog.Title>Review approval</Dialog.Title>
                <Dialog.Close ariaLabel="Close review" />
              </Dialog.Header>
              <Typography.Body role="status">This request is unavailable.</Typography.Body>
            </>
          )}
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Rail>
  )
}

function RetryWhileVisible() {
  useRevalidateWhileVisible(RETRY_MS)
  return null
}

/** A failed refresh replaces the page, and with it the page's subscription. An outage may
 * end at the same revision, so this rereads on a timer until the page returns. A refusal
 * (a 4xx) is an answer, so it is not retried. */
export function ApprovalsErrorBoundary() {
  const error = useRouteError()
  const outage = !isRouteErrorResponse(error) || error.status >= 500
  return (
    <>
      {outage && <RetryWhileVisible />}
      <ManagementErrorBoundary />
    </>
  )
}

function ApprovalRow({
  pending,
  owner,
  open,
}: {
  pending: PendingAction
  /** The provider that owns the call, if it is still in the workspace. */
  owner: { provider: ProviderMark; relativePath: string } | null
  open: boolean
}) {
  const { action, run } = pending
  const question = pendingQuestion(pending)
  const href = reviewHref(action.name)
  return (
    <Table.Row to={href} selected={open} preventScrollReset>
      {/* One cell, so the actions wrap under the request on a narrow screen. */}
      <Table.Cell wrap>
        <span className={styles.row}>
          <span className={styles.request}>
            <Table.RowLink to={href}>{question}</Table.RowLink>
            <Typography.BodySmall as="span" variant="secondary" className={styles.meta}>
              {owner ? (
                <>
                  <span role="img" aria-label={owner.provider.displayName} className={styles.logo}>
                    <Logo
                      name={owner.provider.displayName}
                      icons={owner.provider.icons}
                      size="18"
                    />
                  </span>
                  <CodeChip>{owner.relativePath}</CodeChip>
                  {' · '}
                </>
              ) : (
                action.callPath && (
                  <>
                    <CodeChip>{action.callPath}</CodeChip>
                    {' · '}
                  </>
                )
              )}
              {/* The page names the Workspace. Show who only when an Agent started the Run. */}
              {run.agent && `${run.actorLabel || 'Unknown agent'} · `}
              <Deadline at={action.deadline} />
            </Typography.BodySmall>
          </span>
          <span className={styles.actions}>
            {/* The open review decides this request, and keeps your comment. */}
            {!open && <QuickDecision name={action.name} label={question} />}
            <Button.InternalLink
              variant="secondary"
              to={href}
              preventScrollReset
              aria-label={`Review: ${question}`}
            >
              Review
            </Button.InternalLink>
          </span>
        </span>
      </Table.Cell>
    </Table.Row>
  )
}
