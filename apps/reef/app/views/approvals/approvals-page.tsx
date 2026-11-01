import { isRouteErrorResponse, useRouteError } from 'react-router'
import { EmptyPage } from '@/components/empty-page'
import { useFollowRequiredActions } from '@/lib/use-action-signal'
import { useRevalidateWhileVisible } from '@/lib/use-revalidate-while-visible'
import { Button, Table, Typography } from '@/wax/components'
import { CodeChip } from '@/wax/components/code-chip'
import { Deadline } from '@/wax/components/deadline'
import { QuickDecision } from '../actions/review'
import { ActivityConnectedContext } from '../activity/live'
import { revealHref } from '../activity/navigation'
import { pendingQuestion } from '../activity/pending'
import type { PendingAction } from '../activity/types'
import { ManagementLayout, trail } from '../management/layout'
import { ManagementErrorBoundary } from '../management/management-error'
import * as styles from './approvals.css'

const RETRY_MS = 10_000

/** One row per pending Approval Request, most urgent first. Arguments stay in the review.
 * The rows follow the shell's approval revision. While that signal is down, they may be stale,
 * so the page says so and takes no decision. */
export function ApprovalsPage({
  approvals,
  workspacePath,
  revision,
}: {
  approvals: PendingAction[]
  /** Keys the Workspace's revision. */
  workspacePath: string
  /** The approval revision read before `approvals`, if it was readable. */
  revision?: string
}) {
  const table = Table.useRowCursor()
  const live = useFollowRequiredActions(workspacePath, 'approvals', revision)
  return (
    <ManagementLayout breadcrumbs={trail([], 'Approvals')}>
      {!live && (
        <Typography.BodySmall role="status">Approvals are unavailable.</Typography.BodySmall>
      )}
      <ActivityConnectedContext value={live}>
        <Table.Container
          ariaLabel="Pending approvals"
          ref={table}
          variant="card"
          layout="fixed"
          columns={[{ label: 'Request', width: { fill: 1 } }]}
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
              <ApprovalRow key={pending.action.name} pending={pending} />
            ))}
          </Table.Body>
        </Table.Container>
      </ActivityConnectedContext>
    </ManagementLayout>
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

function ApprovalRow({ pending }: { pending: PendingAction }) {
  const { action, run } = pending
  const question = pendingQuestion(pending)
  const review = revealHref(run, { request: action.name })
  return (
    <Table.Row to={review}>
      {/* One cell, so the actions wrap under the request on a narrow screen. */}
      <Table.Cell wrap>
        <span className={styles.row}>
          <span className={styles.request}>
            <Table.RowLink to={review}>{question}</Table.RowLink>
            <Typography.BodySmall as="span" variant="secondary">
              {action.callPath && (
                <>
                  <CodeChip>{action.callPath}</CodeChip>
                  {' · '}
                </>
              )}
              {/* The page names the Workspace. Show who only when an Agent started the Run. */}
              {run.agent && `${run.actorLabel || 'Unknown agent'} · `}
              <Deadline at={action.deadline} />
            </Typography.BodySmall>
          </span>
          <span className={styles.actions}>
            <QuickDecision name={action.name} label={question} />
            <Button.InternalLink variant="secondary" to={review} aria-label={`Review: ${question}`}>
              Review
            </Button.InternalLink>
          </span>
        </span>
      </Table.Cell>
    </Table.Row>
  )
}
