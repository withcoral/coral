import { Button, Typography } from '@/wax/components'
import { CodeChip } from '@/wax/components/code-chip'
import { Deadline } from '@/wax/components/deadline'
import * as SettingRow from '@/wax/components/setting-row'
import * as ScrollArea from '@/wax/components/scroll-area'
import type { ActivityPage, PendingAction } from './types'
import { QuickDecision } from '../actions/review'
import { useActivityArchived, useActivityConnected } from './live'
import { useActivityNavigation } from './navigation'
import * as styles from './task-activity.css'

/** The question a pending request asks, worded like the MCP prompt for the same request. */
export function pendingQuestion({ action, run }: PendingAction) {
  if (action.kind === 'account_connection')
    return `Connect your ${action.providerLabel || 'provider'} account to continue.`
  const intent = run.intent?.replace(/[.!?]+$/, '')
  return intent ? `${intent}?` : 'Approve this tool call?'
}

/** One row per pending request, most urgent first. Arguments stay in the review. */
export function PendingActions({ page }: { page: ActivityPage }) {
  const nav = useActivityNavigation(page)
  const connected = useActivityConnected()
  const archived = useActivityArchived()
  const pending = page.pending
  if (!pending) return null
  if (pending.unavailable)
    return <Typography.Body role="status">Pending actions are unavailable.</Typography.Body>
  if (pending.actionCount === '0') return null
  return (
    <section className={styles.pending} aria-labelledby="pending-actions-heading">
      <Typography.HeadingSmall as="h2" id="pending-actions-heading">
        Needs your action
      </Typography.HeadingSmall>
      <ScrollArea.Container
        maxHeight="min(40vh, 420px)"
        fade="none"
        scrollDirection="vertical"
        constrainWidth
        aria-label="Pending actions"
      >
        <SettingRow.List>
          {pending.actions.map((pendingAction) => {
            const { action, run } = pendingAction
            const question = pendingQuestion(pendingAction)
            // The page names the Workspace. Show who only when an Agent started the Run.
            const context = run.agent ? [run.actorLabel || 'Unknown agent'] : []
            return (
              <SettingRow.Row
                key={action.name}
                label={question}
                description={
                  <>
                    {action.callPath && (
                      <>
                        <CodeChip>{action.callPath}</CodeChip>
                        {' · '}
                      </>
                    )}
                    {context.map((part) => `${part} · `)}
                    <Deadline at={action.deadline} />
                  </>
                }
                action={
                  <>
                    {action.kind === 'approval' && !archived && (
                      <QuickDecision name={action.name} label={question} />
                    )}
                    <Button.InternalLink
                      variant="secondary"
                      to={nav.revealHref(run, { request: action.name })}
                      disabled={!connected}
                      preventScrollReset
                    >
                      {action.kind === 'approval' ? 'Review' : 'Connect account'}
                    </Button.InternalLink>
                  </>
                }
              />
            )
          })}
        </SettingRow.List>
        <div className={styles.stack}>
          {!pending.actions.length && (
            <Typography.Body>
              No actions remain on this page. Open another page to continue.
            </Typography.Body>
          )}
          <div className={styles.filters}>
            {nav.query.has('pendingAfter') && (
              <Button.InternalLink
                variant="bare"
                to={nav.href({ pendingAfter: null })}
                preventScrollReset
              >
                First actions
              </Button.InternalLink>
            )}
            {pending.next && (
              <Button.InternalLink
                variant="bare"
                to={nav.href({ pendingAfter: pending.next })}
                preventScrollReset
              >
                More actions
              </Button.InternalLink>
            )}
          </div>
        </div>
      </ScrollArea.Container>
    </section>
  )
}
