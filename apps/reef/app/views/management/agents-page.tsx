import { useRef, useState } from 'react'
import { Button, Table } from '@/wax/components'
import { EmptyPage } from '@/components/empty-page'
import { Status } from '@/wax/components/status'
import { AgentKeyDialog } from './agent-key-dialog'
import { ActionDialog, ActionNotice, NameInput } from './forms'
import { ArchivedBanner, ManagementLayout, trail } from './layout'
import { Person } from './person'
import type { ActionResult, AgentView, FormContext, WorkspaceView } from './types'
import * as styles from './management.css'

export interface AgentsPageProps {
  workspace: WorkspaceView
  agents: AgentView[]
  nextPage: string
  form: FormContext
  pending?: boolean
  result?: ActionResult
}

export function AgentsPage({ workspace: w, agents, nextPage, form, result }: AgentsPageProps) {
  const active = w.state === 'active'
  const [dialogResult, setDialogResult] = useState<ActionResult>()
  const [createOpen, setCreateOpen] = useState(false)
  const createTrigger = useRef<HTMLButtonElement>(null)
  const notice = dialogResult ?? result
  const agentsPath = `${w.path}/agents`
  // The key shows once, in a dialog whose only exit is copying it.
  const [savedKey, setSavedKey] = useState<string | null>(null)
  const key = notice?.credential && notice.credential !== savedKey ? notice.credential : null
  // One dialog stays mounted whichever button opened it: the list revalidates
  // while the fetcher settles, and an empty state that unmounts with its dialog
  // would lose the one-time credential.
  const createButton = (
    <Button.TextButton ref={createTrigger} disabled={!active} onClick={() => setCreateOpen(true)}>
      Create agent
    </Button.TextButton>
  )
  return (
    <ManagementLayout
      breadcrumbs={trail([], 'Agents')}
      actions={agents.length > 0 ? createButton : undefined}
    >
      <ActionDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        finalFocus={createTrigger}
        title="Create agent"
        description="Use an agent for automations that run without you."
        form={form}
        intent="createAgent"
        disabled={!active}
        onSuccess={setDialogResult}
      >
        <NameInput label="Agent name" />
      </ActionDialog>
      <AgentKeyDialog
        credential={key}
        agentName={notice?.agentDisplayName}
        endpoint={w.mcpEndpoint}
        onClose={() => setSavedKey(key)}
      />
      {!active && (
        <ArchivedBanner workspace={w}>
          You cannot create or change agents until you restore the workspace.
        </ArchivedBanner>
      )}
      {notice?.error && <ActionNotice result={notice} />}
      {agents.length === 0 ? (
        <EmptyPage
          iconName="Bot"
          title="No agents yet"
          description="Create an agent to get its API key. Then connect the agent to this workspace."
          action={active ? createButton : undefined}
        />
      ) : (
        <Table.Container
          ariaLabel="Agents"
          layout="fixed"
          columns={[
            { label: 'Name', width: { fill: 3 } },
            { label: 'Status', width: { fill: 1.2 } },
            { label: 'Created by', width: { fill: 1.6 } },
            { ariaLabel: 'Settings', width: 48 },
          ]}
        >
          <Table.Head />
          <Table.Body>
            {agents.map((a) => (
              <Table.Row key={a.name}>
                <Table.Cell wrap>
                  <Button.InternalLink to={a.path} variant="link" size="small" wrap>
                    {a.displayName}
                  </Button.InternalLink>
                </Table.Cell>
                <Table.Cell wrap>
                  <Status blockers={a.accessBlockers} />
                </Table.Cell>
                <Table.Cell wrap>
                  <Person person={a.creator} currentUser={w.currentUser} compact />
                </Table.Cell>
                <Table.Cell actions>
                  <Button.IconLink
                    name="Settings"
                    to={a.settingsPath}
                    tooltipText={`${a.displayName} settings`}
                  />
                </Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Container>
      )}
      {nextPage && (
        <div className={styles.row}>
          <Button.InternalLink to={`${agentsPath}?page=${encodeURIComponent(nextPage)}`}>
            Next page
          </Button.InternalLink>
        </div>
      )}
    </ManagementLayout>
  )
}
