import { EmptyPage } from '@/components/empty-page'
import { Button, Table, Typography } from '@/wax/components'
import { CodeChip } from '@/wax/components/code-chip'
import { Deadline } from '@/wax/components/deadline'
import { QuickDecision } from '../actions/review'
import { revealHref } from '../activity/navigation'
import { pendingQuestion } from '../activity/pending'
import type { PendingAction } from '../activity/types'
import { ManagementLayout, trail } from '../management/layout'
import * as styles from './approvals.css'

/** One row per pending Approval Request, most urgent first. Arguments stay in the review. */
export function ApprovalsPage({ approvals }: { approvals: PendingAction[] }) {
  const table = Table.useRowCursor()
  return (
    <ManagementLayout breadcrumbs={trail([], 'Approvals')}>
      <Table.Container
        ariaLabel="Pending approvals"
        ref={table}
        variant="card"
        layout="fixed"
        columns={[{ label: 'Request', width: { fill: 1 } }]}
      >
        <Table.Head />
        {approvals.length === 0 && (
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
    </ManagementLayout>
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
