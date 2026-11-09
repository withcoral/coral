import { useEffect, useLayoutEffect, useRef, useState, type ReactNode } from 'react'
import { isRouteErrorResponse, useFetcher, useNavigate, useRouteError } from 'react-router'
import { EmptyPage } from '@/components/empty-page'
import { useFollowRequiredActions } from '@/lib/use-action-signal'
import { useRevalidateWhileVisible } from '@/lib/use-revalidate-while-visible'
import { Button, Dialog, Table, Typography } from '@/wax/components'
import { Icon, type IconName } from '@/wax/components/icon'
import { Pill, type PillColor } from '@/wax/components/pill'
import { Deadline } from '@/wax/components/deadline'
import { PageLayout } from '@/wax/components/page-layout'
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
import { dayLabel, UtcTime } from '../activity/instant'
import { utcDays } from '@/lib/activity-list'
import type {
  ActivityRow,
  ApprovalDetail,
  ApprovalReading,
  PendingAction,
  SettledApproval,
  SettledPage,
} from '../activity/types'
import { trail } from '../management/layout'
import { ManagementErrorBoundary } from '../management/management-error'
import * as styles from './approvals.css'
import { CallTool, ProviderMarksContext, type ProviderMark } from '../activity/call-tool'

const RETRY_MS = 10_000
/** What an approval request of `run` asks: the Run's intent as a question, worded like the MCP
 * prompt for the same request. */
function approvalQuestion(run: Pick<PendingAction['run'], 'intent'>) {
  const intent = run.intent?.replace(/[.!?]+$/, '')
  return intent ? `${intent}?` : 'Approve this tool call?'
}

/** One value, so the page without history keeps one identity across renders. */
const NO_HISTORY: SettledPage = { approvals: [], next: '' }

/** A later page of settled approvals, or the outage that kept it from loading. */
type OlderPage = SettledPage | { unavailable: true }

/** What the history shows of one workspace, and what it was built from. */
interface History {
  workspacePath: string
  /** The newest page the rows include. */
  read: SettledPage | null
  /** The last later page this history took in, so each one joins the rows once. */
  loaded: OlderPage | undefined
  /** Where the later pages loaded here end. Before one loads, the newest page's end. */
  next?: string
  rows: SettledApproval[]
}

/** Settled approvals: the newest page the loader reads, then the older pages loaded here.
 * A refresh replaces the newest page. Every row loaded stays, since a settled request does
 * not change, so the list does not jump while you read older rows. The rows keep Coral's
 * order. A newest page Coral could not read (`null`) keeps the rows already shown. */
function useHistory(read: SettledPage | null, workspacePath: string) {
  const fetcher = useFetcher<OlderPage>()
  const loaded = fetcher.state === 'idle' ? fetcher.data : undefined
  const [history, setHistory] = useState(() => fresh(workspacePath, read, loaded))
  // Derived from the reads during render, so a refresh never shows the old rows first.
  const current = advance(history, workspacePath, read, loaded)
  if (current !== history) setHistory(current)
  const next = current.next ?? read?.next
  return {
    rows: current.rows,
    unavailable: read === null,
    more: Boolean(next) && read !== null,
    loading: fetcher.state !== 'idle',
    failed: Boolean(current.loaded && 'unavailable' in current.loaded),
    load: () =>
      void fetcher.load(
        `${workspacePath}/approvals/history?${new URLSearchParams({ after: next ?? '' })}`,
      ),
  }
}

/** A workspace's history from its newest page. A page the fetcher already holds belongs to
 * the workspace before, so it never joins these rows. */
function fresh(
  workspacePath: string,
  read: SettledPage | null,
  loaded: OlderPage | undefined,
): History {
  return { workspacePath, read, loaded, rows: read?.approvals ?? [] }
}

/** `history` after the newest page `read` and the later page `loaded`, or `history` itself
 * when neither is new. */
function advance(
  history: History,
  workspacePath: string,
  read: SettledPage | null,
  loaded: OlderPage | undefined,
): History {
  if (history.workspacePath !== workspacePath) return fresh(workspacePath, read, loaded)
  let next = history
  if (read && read !== history.read)
    // The newest rows go first. Each row they move down stays, after them.
    next = { ...next, read, rows: [...read.approvals, ...notIn(next.rows, read.approvals)] }
  if (loaded && loaded !== history.loaded)
    // An outage keeps the place to retry from.
    next =
      'unavailable' in loaded
        ? { ...next, loaded }
        : {
            ...next,
            loaded,
            next: loaded.next,
            rows: [...next.rows, ...notIn(loaded.approvals, next.rows)],
          }
  return next
}

/** `rows` that `shown` does not hold yet. */
function notIn(rows: SettledApproval[], shown: SettledApproval[]) {
  const names = new Set(shown.map((row) => row.name))
  return rows.filter((row) => !names.has(row.name))
}

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
  settled = NO_HISTORY,
  workspacePath,
  revision,
  selected = null,
  review = null,
  providers = [],
}: {
  approvals: PendingAction[]
  /** The newest page of settled approvals, read with `approvals`. Null when Coral could not
   * read it, which leaves the pending rows. */
  settled?: SettledPage | null
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
  // One cursor around both tables, so the arrows move through them as one list.
  const list = Table.useRowCursor()
  const live = useFollowRequiredActions(workspacePath, 'approvals', revision)
  const history = useHistory(settled, workspacePath)
  return (
    <ActionReviewSession request={review?.detail}>
      <ProviderMarksContext value={providers}>
        <PageLayout variant="split" breadcrumbs={trail([], 'Approvals')}>
          <ActivityConnectedContext value={live}>
            <div className={styles.page}>
              <div className={styles.list} ref={list}>
                {!live && (
                  <Typography.BodySmall role="status">
                    Approvals are unavailable.
                  </Typography.BodySmall>
                )}
                <Table.Container
                  ariaLabel="Pending approvals"
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
                      {history.rows.length || history.more || history.unavailable ? (
                        <Typography.Body>Nothing to approve.</Typography.Body>
                      ) : (
                        <EmptyPage
                          fullSize={false}
                          iconName="ShieldCheck"
                          title="Nothing to approve."
                          description="Requests that need your approval appear here. Decided and expired requests appear below them."
                        />
                      )}
                    </Table.Status>
                  )}
                  <Table.Body>
                    {approvals.map((pending) => (
                      <ApprovalRow
                        key={pending.action.name}
                        pending={pending}
                        open={pending.action.name === selected}
                      />
                    ))}
                  </Table.Body>
                </Table.Container>
                {history.unavailable && (
                  <Typography.BodySmall role="status">
                    Approval history is unavailable.
                  </Typography.BodySmall>
                )}
                {/* A page can hold no row this Reef shows and still lead to older ones. */}
                {(history.rows.length > 0 || history.more) && (
                  // As in Activity and Providers, the history scrolls inside its own frame.
                  <div className={styles.history}>
                    <Table.Container
                      ariaLabel="Approval history"
                      density="compact"
                      layout="scroll"
                      columns={HISTORY_COLUMNS}
                    >
                      <Table.Head />
                      {/* Each row sits under the UTC day it settled, as Activity's records do. */}
                      {utcDays(
                        history.rows,
                        (row) => row.settledAt,
                        (row) => row.name,
                      ).map(({ day, key, rows }) => (
                        <Table.Group
                          key={key}
                          label={dayLabel(day)}
                          heading={
                            <Table.Cell fullWidth>
                              <Typography.BodySmall color="secondary">
                                {dayLabel(day)}
                              </Typography.BodySmall>
                            </Table.Cell>
                          }
                        >
                          {rows.map((row) => (
                            <SettledRow key={row.name} settled={row} open={row.name === selected} />
                          ))}
                        </Table.Group>
                      ))}
                      {history.more && (
                        <Table.LoadMore
                          onLoad={history.load}
                          loading={history.loading}
                          loadingLabel="Loading older approvals…"
                          error={history.failed && 'Older approvals are unavailable.'}
                          retryLabel="Try again"
                        >
                          Load older approvals
                        </Table.LoadMore>
                      )}
                    </Table.Container>
                  </div>
                )}
              </div>
              <ReviewRail approvals={approvals} selected={selected} review={review} />
            </div>
          </ActivityConnectedContext>
        </PageLayout>
      </ProviderMarksContext>
    </ActionReviewSession>
  )
}

/** The review of the request the address opens. After your decision it moves down the list;
 * a request that someone else settles stays open, showing its outcome. */
function ReviewRail({
  approvals,
  selected,
  review,
}: {
  approvals: PendingAction[]
  selected: string | null
  review: ApprovalReading | null
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
                    calledFrom={shown.call.parent}
                    variant="approvals"
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

function ApprovalRow({ pending, open }: { pending: PendingAction; open: boolean }) {
  const { action, run } = pending
  const question = approvalQuestion(run)
  const href = reviewHref(action.name)
  return (
    <Table.Row to={href} selected={open} preventScrollReset>
      {/* One cell, so the actions wrap under the request on a narrow screen. */}
      <Table.Cell wrap>
        <span className={styles.row}>
          <span className={styles.request}>
            <Table.RowLink to={href}>{question}</Table.RowLink>
            <CallMeta callPath={action.callPath} run={run}>
              <Deadline at={action.deadline} />
            </CallMeta>
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

const outcomes: Record<
  SettledApproval['outcome'],
  { label: string; icon: IconName; color: PillColor }
> = {
  approved: { label: 'Approved', icon: 'ShieldCheck', color: 'green' },
  declined: { label: 'Declined', icon: 'ShieldX', color: 'red' },
  expired: { label: 'Expired', icon: 'Hourglass', color: 'gray' },
}

// The request and tool share what is left, but never shrink past a readable width: a
// narrow screen scrolls the table sideways instead, as Activity's does.
const HISTORY_COLUMNS: Table.Column[] = [
  { label: 'TIME · UTC', width: 104 },
  { label: 'REQUEST', width: 'minmax(220px, 2fr)' },
  { label: 'TOOL', width: 'minmax(160px, 1fr)' },
  { label: 'ASKED BY', width: 'content' },
  { label: 'OUTCOME', width: 120 },
  { label: 'DECIDED BY', width: 'content' },
]

/** One settled request on one line: when, what it asked, the tool, who asked, the outcome
 * with an icon and text, and who decided. An expired request has no approver; its time is
 * its deadline. */
function SettledRow({ settled, open }: { settled: SettledApproval; open: boolean }) {
  const question = approvalQuestion(settled.run)
  const href = reviewHref(settled.name)
  const outcome = outcomes[settled.outcome]
  const { run } = settled
  return (
    <Table.Row to={href} selected={open} preventScrollReset>
      <Table.Cell mono>
        <UtcTime value={settled.settledAt} />
      </Table.Cell>
      <Table.Cell title={question}>
        <Table.RowLink to={href}>{question}</Table.RowLink>
      </Table.Cell>
      <Table.Cell>
        <CallTool callPath={settled.callPath} />
      </Table.Cell>
      <Table.Cell>{run.agent ? run.actorLabel || 'Unknown agent' : 'You'}</Table.Cell>
      <Table.Cell>
        {/* An icon and a word, so the outcome does not rest on colour. */}
        <Pill color={outcome.color} size="small">
          <Icon name={outcome.icon} size="14" />
          {outcome.label}
        </Pill>
      </Table.Cell>
      <Table.Cell>{settled.approverLabel || '—'}</Table.Cell>
    </Table.Row>
  )
}

/** The call's provider logo and tool, the Agent that started the Run, then `children`, on
 * one line that wraps between them. */
function CallMeta({
  callPath,
  run,
  children,
}: {
  callPath: string
  run: Extract<ActivityRow, { kind: 'run' }>
  children: ReactNode
}) {
  return (
    <Typography.BodySmall as="span" variant="secondary" className={styles.meta}>
      {callPath && (
        <>
          <CallTool callPath={callPath} size="small" />
          {' · '}
        </>
      )}
      {/* The page names the Workspace. Show who only when an Agent started the Run. */}
      {run.agent && `${run.actorLabel || 'Unknown agent'} · `}
      {children}
    </Typography.BodySmall>
  )
}
