import { useDeferredValue, useId, useRef, useState } from 'react'
import { CatalogFilterMenu, CatalogToolbar, type CatalogFilterOptions } from '@/components/catalog'
import { Button, Combobox, Menu, Table, Typography } from '@/wax/components'
import { Pill } from '@/wax/components/pill'
import { TruncatedList } from '@/components/truncated-list'
import { StatePill } from './access'
import { ActionDialog } from './forms'
import { ManagementLayout, trail } from './layout'
import { personLabel, personName } from './person'
import type {
  FormContext,
  OrganizationPersonView,
  OrganizationWorkspaceView,
  OwnerView,
} from './types'
import * as styles from './management.css'

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
  const query = useDeferredValue(input).trim()
  // The filter matches the workspace name and its owners' names and emails.
  const shown = workspaces.filter(
    (w) =>
      (state === 'all' || w.state === state) &&
      [w.displayName, ...w.owners.flatMap((o) => [personLabel(o.profile), o.profile.email])].some(
        (text) => text.toLowerCase().includes(query.toLowerCase()),
      ),
  )
  return (
    <ManagementLayout breadcrumbs={trail([], 'Workspaces')}>
      {workspaces.length > 0 && (
        <CatalogToolbar
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
      )}
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
            {query ? `No workspaces match “${query}”.` : 'No workspaces match.'}
          </Table.Status>
        )}
        <Table.Body>
          {shown.map((w) => (
            <Table.Row key={w.name}>
              <Table.Cell wrap>{w.displayName}</Table.Cell>
              <Table.Cell wrap>
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
      description: `Archiving ${w.displayName} blocks new runs, unsent operation calls and settings changes. Coral keeps the history. You can restore the workspace.`,
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
          <OwnersField
            // People who already own the workspace are not offered.
            people={candidates.filter((c) => !w.owners.some((o) => o.user === c.profile.user))}
            currentUser={currentUser}
            value={owners}
            onChange={setOwners}
          />
        )}
      </ActionDialog>
    </>
  )
}

/**
 * Picks organization members by name or email. The value is their
 * organization membership names.
 */
function OwnersField({
  people,
  currentUser,
  value,
  onChange,
}: {
  people: OrganizationPersonView[]
  currentUser: string
  value: string[]
  onChange: (owners: string[]) => void
}) {
  const id = useId()
  const [query, setQuery] = useState('')
  // The combobox filters on its item strings, so each holds the name and email.
  const item = (p: OrganizationPersonView) =>
    p.profile.email
      ? `${personName(p.profile, currentUser)} (${p.profile.email})`
      : personName(p.profile, currentUser)
  const byItem = new Map(people.map((p) => [item(p), p]))
  const byName = new Map(people.map((p) => [p.name, p]))
  return (
    <div className={styles.addField}>
      <Typography.BodySmallStrong as="label" htmlFor={id}>
        People
      </Typography.BodySmallStrong>
      {value.map((owner) => (
        <input key={owner} type="hidden" name="owner" value={owner} />
      ))}
      <Combobox.Root
        autoHighlight
        hideWhenEmpty
        items={[...byItem.keys()]}
        multiple
        inputValue={query}
        onInputValueChange={setQuery}
        value={value.flatMap((owner) => {
          const p = byName.get(owner)
          return p ? [item(p)] : []
        })}
        onValueChange={(next) => {
          onChange((Array.isArray(next) ? next : []).flatMap((i) => byItem.get(i)?.name ?? []))
          setQuery('')
        }}
      >
        <Combobox.InputGroup>
          <Combobox.Chips>
            <Combobox.Value>
              {(chosen) =>
                Array.isArray(chosen)
                  ? chosen.map((i) => {
                      const p = byItem.get(i)
                      const name = p ? personName(p.profile, currentUser) : i
                      return (
                        <Combobox.Chip key={i}>
                          <Combobox.ChipLabel>{name}</Combobox.ChipLabel>
                          <Combobox.ChipRemove aria-label={`Remove ${name}`} />
                        </Combobox.Chip>
                      )
                    })
                  : null
              }
            </Combobox.Value>
            <Combobox.Input id={id} bare placeholder={value.length > 0 ? '' : 'Search people'} />
          </Combobox.Chips>
        </Combobox.InputGroup>
        <Combobox.Content>
          <Combobox.List>
            {(i: string) => (
              <Combobox.Item key={i} value={i}>
                {i}
              </Combobox.Item>
            )}
          </Combobox.List>
        </Combobox.Content>
      </Combobox.Root>
    </div>
  )
}
