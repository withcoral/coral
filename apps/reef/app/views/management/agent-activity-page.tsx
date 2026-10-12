import type { ReactNode } from 'react'
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
  activity: ReactNode
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
  idle = false,
}: AgentActivityPageProps) {
  const active = w.state === 'active'
  return (
    <PageLayout
      variant="split"
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
          <div className={styles.agentSetup}>
            {idle && active && w.mcpEndpoint && (
              <AgentQuickStart endpoint={w.mcpEndpoint} settingsPath={a.settingsPath} />
            )}
            <AgentProviders providers={providers} form={form} canConnect={active} />
          </div>
        </>
      }
    >
      {activity}
    </PageLayout>
  )
}
