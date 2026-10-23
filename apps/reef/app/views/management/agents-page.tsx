import { useDeferredValue, useRef, useState } from 'react'
import { Button, Table } from '@/wax/components'
import {
  CatalogFilterMenu,
  CatalogToolbar,
  toolbarActionLabel,
  type CatalogFilterOptions,
} from '@/components/catalog'
import { EmptyPage } from '@/components/empty-page'
import { Status } from '@/wax/components/status'
import { AgentKeyDialog } from './agent-key-dialog'
import { ActionDialog, ActionNotice, NameInput } from './forms'
import { ArchivedBanner, ManagementLayout, trail } from './layout'
import { Person } from './person'
import type { ActionResult, AgentView, FormContext, WorkspaceView } from './types'
import * as styles from './management.css'
import { filterQuery } from '@/utils/filter-query'

type StatusFilter = 'all' | 'ready' | 'blocked'

const STATUS_FILTERS: CatalogFilterOptions<StatusFilter> = [
  { value: 'all', label: 'All statuses', icon: 'ListFilter' },
  { value: 'ready', label: 'Ready', icon: 'CircleCheck' },
  // The same shape a blocked row's status shows.
  { value: 'blocked', label: 'Blocked', icon: 'TriangleAlert' },
]

const FILTER_ID = 'agents-filter'

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
  const [input, setInput] = useState('')
  const [status, setStatus] = useState<StatusFilter>('all')
  const query = filterQuery(useDeferredValue(input))
  const shown = agents.filter(
    (a) =>
      query.matches(a.displayName) &&
      (status === 'all' || (status === 'blocked') === a.accessBlockers.length > 0),
  )
  const table = Table.useRowCursor({ filterId: FILTER_ID })
  // The key shows once, in a dialog whose only exit is copying it.
  const [savedKey, setSavedKey] = useState<string | null>(null)
  const key = notice?.credential && notice.credential !== savedKey ? notice.credential : null
  // One dialog stays mounted whichever button opened it: the list revalidates
  // while the fetcher settles, and an empty state that unmounts with its dialog
  // would lose the one-time credential.
  const createButton = (labelClassName?: string) => (
    <Button.Container ref={createTrigger} disabled={!active} onClick={() => setCreateOpen(true)}>
      <Button.Icon name="Plus" />
      <Button.Text className={labelClassName}>Create agent</Button.Text>
    </Button.Container>
  )
  return (
    <ManagementLayout breadcrumbs={trail([], 'Agents')} titled compact bounded>
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
      {/* The empty state sits in the table, so the page keeps its shape at the first agent. */}
      <CatalogToolbar
        title="Agents"
        // With no agents, the empty state offers the button instead.
        action={agents.length > 0 ? createButton(toolbarActionLabel) : undefined}
        filter={{
          id: FILTER_ID,
          label: 'Filter agents',
          shortcut: '$mod+f',
          value: input,
          onChange: setInput,
        }}
      >
        <CatalogFilterMenu
          ariaLabel="Filter agents by status"
          options={STATUS_FILTERS}
          value={status}
          onChange={setStatus}
        />
      </CatalogToolbar>
      <Table.Container
        ref={table}
        ariaLabel="Agents"
        variant="card"
        layout="fixed"
        // The rows scroll under the heading, so the table never outgrows the page.
        bounded
        columns={[
          { label: 'Name', width: { fill: 3 } },
          { label: 'Status', width: { fill: 1.2 } },
          { label: 'Created by', width: { fill: 1.6 } },
          { ariaLabel: 'Settings', width: 48 },
        ]}
      >
        <Table.Head />
        {agents.length === 0 ? (
          <Table.Status>
            <EmptyPage
              fullSize={false}
              iconName="Bot"
              title="No agents yet"
              description="Create an agent to get its API key. Then connect the agent to this workspace."
              action={active ? createButton() : undefined}
            />
          </Table.Status>
        ) : (
          shown.length === 0 && (
            <Table.Status>
              {query.text ? `No agents match “${query.text}”.` : 'No agents match.'}
            </Table.Status>
          )
        )}
        <Table.Body>
          {shown.map((a) => (
            <Table.Row key={a.name} to={a.path}>
              <Table.Cell title={a.displayName}>
                <Table.RowLink to={a.path}>{a.displayName}</Table.RowLink>
              </Table.Cell>
              <Table.Cell>
                <Status blockers={a.accessBlockers} />
              </Table.Cell>
              <Table.Cell>
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
