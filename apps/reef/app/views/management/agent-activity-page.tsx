import { useId, type ReactNode } from 'react'
import { useSearchParams } from 'react-router'
import { Button, Typography } from '@/wax/components'
import { PageLayout } from '@/wax/components/page-layout'
import { Status } from '@/wax/components/status'
import { AgentProviders, type AgentProviderView } from './agent-providers'
import { AgentQuickStart } from './quick-start'
import { ArchivedBanner, trail } from './layout'
import type { AgentView, FormContext, WorkspaceView } from './types'
import * as styles from './management.css'

export interface AgentActivityPageProps {
  workspace: WorkspaceView
  agent: AgentView
  /** Null when Coral could not read them. */
  providers: AgentProviderView[] | null
  form: FormContext
  /** The complete Activity reader, used on the dedicated Activity page. */
  activity: ReactNode
  /** The latest records for the agent dashboard. Defaults to the complete reader. */
  recentActivity?: ReactNode
  /** The agent has called nothing yet, so the page shows how to connect it. */
  idle?: boolean
}

/**
 * An agent's own page: how to connect it, the accounts it needs, and its
 * activity. Settings hold what you change about the agent itself.
 */
export function AgentActivityPage({
  workspace: w,
  agent: a,
  providers,
  form,
  activity,
  recentActivity = activity,
  idle = false,
}: AgentActivityPageProps) {
  const [search] = useSearchParams()
  const recentHeading = useId()
  const active = w.state === 'active'
  // A record opened from Recent activity sets `tree`, and closing it keeps `tree`.
  const fullActivity =
    search.get('view') === 'activity' || search.has('record') || search.has('tree')
  if (fullActivity)
    return (
      <PageLayout
        variant="split"
        breadcrumbs={trail(
          [
            { label: 'Agents', to: `${w.path}/agents` },
            { label: a.displayName, to: a.path },
          ],
          'Activity',
        )}
        actions={
          <Button.InternalLink to={a.path} variant="secondary" size="small">
            Back to agent
          </Button.InternalLink>
        }
      >
        {activity}
      </PageLayout>
    )

  const showAccounts = providers === null || providers.length > 0
  return (
    <PageLayout
      variant="wide"
      breadcrumbs={trail([{ label: 'Agents', to: `${w.path}/agents` }], a.displayName)}
      actions={
        <Button.InternalLink to={a.settingsPath} variant="secondary" size="small">
          Settings
        </Button.InternalLink>
      }
      header={
        <>
          <div className={styles.heading}>
            <Typography.HeadingLarge as="h2">{a.displayName}</Typography.HeadingLarge>
            <Status blockers={a.accessBlockers} />
          </div>
          {!active && (
            <ArchivedBanner workspace={w}>
              Restore the workspace before you start new agent work.
            </ArchivedBanner>
          )}
        </>
      }
    >
      <div className={styles.agentDashboard}>
        {idle && active && w.mcpEndpoint && (
          <AgentQuickStart endpoint={w.mcpEndpoint} settingsPath={a.settingsPath} />
        )}
        <div className={showAccounts ? styles.agentDashboardGrid : styles.agentDashboardSingle}>
          {showAccounts && <AgentProviders providers={providers} form={form} canConnect={active} />}
          <section className={styles.agentRecentActivity} aria-labelledby={recentHeading}>
            <Typography.HeadingSmall as="h2" id={recentHeading}>
              Recent activity
            </Typography.HeadingSmall>
            <div className={styles.agentRecentActivityBody}>{recentActivity}</div>
          </section>
        </div>
      </div>
    </PageLayout>
  )
}
