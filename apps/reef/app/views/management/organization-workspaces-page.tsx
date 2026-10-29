import { useDeferredValue, useRef, useState } from 'react'
import { CatalogFilterMenu, CatalogToolbar, type CatalogFilterOptions } from '@/components/catalog'
import { Button, Menu, Table } from '@/wax/components'
import { Pill } from '@/wax/components/pill'
import { TruncatedList } from '@/components/truncated-list'
import { StatePill } from './access'
import { ActionDialog } from './forms'
import { ManagementLayout, trail } from './layout'
import { PeopleField } from './people-field'
import { personLabel, personName } from './person'
import { filterQuery } from '@/utils/filter-query'
import type {
  FormContext,
  OrganizationPersonView,
  OrganizationWorkspaceView,
  OwnerView,
} from './types'

type StateFilter = 'all' | OrganizationWorkspaceView['state']

const STATE_FILTERS: CatalogFilterOptions<StateFilter> = [
  { value: 'all', label: 'All states', icon: 'ListFilter' },
  { value: 'active', label: 'Active', icon: 'CircleCheck' },
  { value: 'archived', label: 'Archived', icon: 'Archive' },
]

const FILTER_ID = 'workspaces-filter'

/**
 * Every workspace in the organization, for an organization owner. It shows
 * who owns each workspace, and nothing inside one.
 */
export function OrganizationWorkspacesPage({
  workspaces,
  candidates,
  currentUser,
  nextPage,
  form,
}: {
  workspaces: OrganizationWorkspaceView[]
  /** Every enabled organization member: the people who can own a workspace. */
  candidates: OrganizationPersonView[]
  currentUser: string
  nextPage: string
  form: FormContext
}) {
  const [input, setInput] = useState('')
  const [state, setState] = useState<StateFilter>('all')
  const query = filterQuery(useDeferredValue(input))
  // The filter matches the workspace name and its owners' names and emails.
  const shown = workspaces.filter(
    (w) =>
      (state === 'all' || w.state === state) &&
      query.matches(
        w.displayName,
        ...w.owners.flatMap((o) => [personLabel(o.profile), o.profile.email]),
      ),
  )
  return (
    <ManagementLayout breadcrumbs={trail([], 'Workspaces')} titled compact>
      {/* The toolbar holds the page's title, so it stays when no rows do. */}
      <CatalogToolbar
        title="Workspaces"
        filter={{
          id: FILTER_ID,
          label: 'Filter workspaces',
          shortcut: '$mod+f',
          value: input,
          onChange: setInput,
        }}
      >
        <CatalogFilterMenu
          ariaLabel="Filter workspaces by state"
          options={STATE_FILTERS}
          value={state}
          onChange={setState}
        />
      </CatalogToolbar>
      <Table.Container
        ariaLabel="Workspaces"
        variant="card"
        layout="fixed"
        // Organization owners manage a workspace from its actions, not a page.
        staticRows
        columns={[
          { label: 'Name', width: { fill: 2 } },
          { label: 'State', width: 'fill' },
          { label: 'Owners', width: { fill: 2 } },
          { ariaLabel: 'Actions', width: 48 },
        ]}
      >
        <Table.Head />
        {workspaces.length === 0 && <Table.Status>No workspaces on this page.</Table.Status>}
        {workspaces.length > 0 && shown.length === 0 && (
          <Table.Status>
            {query.text ? `No workspaces match “${query.text}”.` : 'No workspaces match.'}
          </Table.Status>
        )}
        <Table.Body>
          {shown.map((w) => (
            <Table.Row key={w.name}>
              <Table.Cell title={w.displayName}>{w.displayName}</Table.Cell>
              <Table.Cell>
                <StatePill workspace={w} />
              </Table.Cell>
              <Table.Cell>
                {w.owners.length === 0 ? (
                  'No owner'
                ) : (
                  <OwnerPills owners={w.owners} currentUser={currentUser} />
                )}
              </Table.Cell>
              <Table.Cell actions>
                <WorkspaceActions
                  workspace={w}
                  candidates={candidates}
                  currentUser={currentUser}
                  form={form}
                />
              </Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Container>
      {nextPage && (
        <Button.InternalLink to={`?page=${encodeURIComponent(nextPage)}`}>
          Next workspaces
        </Button.InternalLink>
      )}
    </ManagementLayout>
  )
}

/** The owners on one line. Owners that do not fit move to a "+N" tooltip. */
function OwnerPills({ owners, currentUser }: { owners: OwnerView[]; currentUser: string }) {
  const pill = (o: OwnerView) => {
    const label = personName(o.profile, currentUser)
    return (
      <Pill key={o.user} title={o.profile.email || undefined}>
        {o.enabled ? label : `${label} · Disabled`}
      </Pill>
    )
  }
  return (
    <TruncatedList
      items={owners}
      getKey={(o) => o.user}
      renderItem={pill}
      renderOverflowContent={(hidden) => hidden.map(pill)}
      renderOverflowTrigger={(count) => (
        <Pill as="button" aria-label={`${count} more owners`}>
          +{count}
        </Pill>
      )}
    />
  )
}

function WorkspaceActions({
  workspace: w,
  candidates,
  currentUser,
  form,
}: {
  workspace: OrganizationWorkspaceView
  candidates: OrganizationPersonView[]
  currentUser: string
  form: FormContext
}) {
  const [action, setAction] = useState<'assign' | 'archive' | 'restore' | null>(null)
  const [owners, setOwners] = useState<string[]>([])
  const trigger = useRef<HTMLButtonElement>(null)
  const dialog = {
    assign: {
      title: 'Assign owners',
      intent: 'assignOwner',
      description: `Choose people to own ${w.displayName}. They join the workspace as owners.`,
      submit: owners.length > 1 ? `Assign ${owners.length} owners` : 'Assign owner',
    },
    archive: {
      title: 'Archive workspace',
      intent: 'archiveWorkspace',
      description: `Archiving ${w.displayName} blocks new runs, unsent tool calls and settings changes. Coral keeps the history. You can restore the workspace.`,
    },
    restore: {
      title: 'Restore workspace',
      intent: 'restoreWorkspace',
      description: `Restore ${w.displayName} to allow new runs and settings changes. Other access restrictions still apply.`,
    },
  }[action ?? 'assign']
  return (
    <>
      <Menu.Container>
        <Menu.Trigger
          render={
            <Button.IconButton
              ref={trigger}
              name="Ellipsis"
              ariaLabel={`Actions for ${w.displayName}`}
              variant="bare"
            />
          }
        >
          {null}
        </Menu.Trigger>
        <Menu.Content>
          {w.state === 'active' ? (
            <>
              <Menu.Item
                onClick={() => {
                  setOwners([])
                  setAction('assign')
                }}
              >
                Assign owners
              </Menu.Item>
              <Menu.Item onClick={() => setAction('archive')}>Archive workspace</Menu.Item>
            </>
          ) : (
            <Menu.Item onClick={() => setAction('restore')}>Restore workspace</Menu.Item>
          )}
        </Menu.Content>
      </Menu.Container>
      <ActionDialog
        open={action !== null}
        onOpenChange={(open) => {
          if (!open) setAction(null)
        }}
        finalFocus={trigger}
        size={action === 'assign' ? 'l' : undefined}
        title={dialog.title}
        description={dialog.description}
        submit={'submit' in dialog ? dialog.submit : undefined}
        intent={dialog.intent}
        name={w.name}
        etag={w.etag}
        form={form}
        destructive={action === 'archive'}
        disabled={action === 'assign' && owners.length === 0}
        // A partial assignment keeps only the refused people, so a retry assigns no one twice.
        onError={(result) => {
          if (result.refused?.length) setOwners(result.refused)
        }}
      >
        {action === 'assign' && (
          <PeopleField
            name="owner"
            // People who already own the workspace are not offered.
            people={candidates
              .filter((c) => !w.owners.some((o) => o.user === c.profile.user))
              .map((c) => ({ value: c.name, person: c.profile }))}
            currentUser={currentUser}
            placeholder="Search people"
            value={owners}
            onChange={setOwners}
          />
        )}
      </ActionDialog>
    </>
  )
}
