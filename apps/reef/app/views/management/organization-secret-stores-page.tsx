import { useDeferredValue, useRef, useState } from 'react'
import { CatalogFilterMenu, CatalogToolbar, type CatalogFilterOptions } from '@/components/catalog'
import { EmptyPage } from '@/components/empty-page'
import { filterQuery } from '@/utils/filter-query'
import { Button, Combobox, Table, Typography } from '@/wax/components'
import { Tooltip } from '@/wax/components/tooltip'
import { Pill } from '@/wax/components/pill'
import { Status } from '@/wax/components/status'
import { ActionDialog } from './forms'
import { ManagementLayout, trail } from './layout'
import type { FormContext } from './types'
import * as styles from './management.css'

/** How to define a secret store in config.toml. */
const secretStoresDocsUrl = 'https://withcoral.com/docs/reference/configuration#secret_stores'

/** A workspace that holds a grant. */
interface GrantedWorkspace {
  name: string
  displayName: string
  archived: boolean
}

/** A store config defines, or that a grant still names after config removed it. */
export interface SecretStoreRow {
  name: string
  /** Null when config does not define the store, so its grants give nothing. */
  kind: string | null
  /** Why Coral cannot read from the store now. Empty when it can. */
  blockers: string[]
  workspaces: GrantedWorkspace[]
}

/** A workspace that can take a change. */
interface ActiveWorkspace {
  name: string
  displayName: string
}

/** The kind a row shows. A store that config no longer defines has none. */
function kindLabel(store: SecretStoreRow) {
  return store.kind ?? 'Unknown'
}

const SORT_COLUMNS = {
  name: Table.byText((store: SecretStoreRow) => store.name),
  kind: Table.byText(kindLabel),
  // The stores Coral cannot read from come first, then each status by the label you read.
  status: {
    compare: (a: SecretStoreRow, b: SecretStoreRow) =>
      Number(b.blockers.length > 0) - Number(a.blockers.length > 0) ||
      Table.compareText(a.blockers[0] ?? '', b.blockers[0] ?? ''),
    first: 'ascending',
  },
} satisfies Record<string, Table.SortColumn<SecretStoreRow>>

type StatusFilter = 'all' | 'available' | 'blocked'

const STATUS_FILTERS: CatalogFilterOptions<StatusFilter> = [
  { value: 'all', label: 'All statuses', icon: 'ListFilter' },
  { value: 'available', label: 'Available', icon: 'CircleCheck' },
  // The same shape a blocked row's status shows.
  { value: 'blocked', label: 'Blocked', icon: 'TriangleAlert' },
]

const FILTER_ID = 'secret-stores-filter'

/** The secret stores the operator defined in deployment config, and their grants. */
export function OrganizationSecretStoresPage({
  stores,
  workspaces,
  form,
}: {
  stores: SecretStoreRow[]
  workspaces: ActiveWorkspace[]
  form: FormContext
}) {
  const [input, setInput] = useState('')
  const [status, setStatus] = useState<StatusFilter>('all')
  const query = filterQuery(useDeferredValue(input))
  // Stores with equal values stay in name order, whichever way the column runs.
  const sort = Table.useSort({
    columns: SORT_COLUMNS,
    initial: 'name',
    tiebreak: SORT_COLUMNS.name.compare,
  })
  // The filter matches the text a row shows: the store's name, its kind and the labels of
  // the workspaces that hold a grant.
  const shown = stores.filter(
    (store) =>
      (status === 'all' || (status === 'blocked') === store.blockers.length > 0) &&
      query.matches(
        store.name,
        kindLabel(store),
        ...workspaceOptions(store.workspaces).map((option) => option.label),
      ),
  )
  return (
    <ManagementLayout breadcrumbs={trail([], 'Secret stores')} titled compact>
      {/* The toolbar holds the page's title, so it stays when no rows do. */}
      <CatalogToolbar
        title="Secret stores"
        titleInfo={
          <Button.ExternalLink href={secretStoresDocsUrl}>Read the docs</Button.ExternalLink>
        }
        filter={{
          id: FILTER_ID,
          label: 'Filter secret stores',
          shortcut: '$mod+f',
          value: input,
          onChange: setInput,
        }}
      >
        <CatalogFilterMenu
          ariaLabel="Filter secret stores by status"
          options={STATUS_FILTERS}
          value={status}
          onChange={setStatus}
        />
      </CatalogToolbar>
      <Table.Container
        ariaLabel="Secret stores"
        variant="card"
        layout="fixed"
        staticRows
        columns={[{ width: 'fill' }, { width: 'fill' }, { width: 'fill' }, { width: { fill: 2 } }]}
      >
        <Table.Head>
          <Table.Row>
            <Table.SortHeading sort={sort} column="name">
              Name
            </Table.SortHeading>
            <Table.SortHeading sort={sort} column="kind">
              Kind
            </Table.SortHeading>
            <Table.SortHeading sort={sort} column="status">
              Status
            </Table.SortHeading>
            <Table.Heading>Workspaces</Table.Heading>
          </Table.Row>
        </Table.Head>
        {stores.length === 0 ? (
          <Table.Status>
            <EmptyPage
              fullSize={false}
              iconName="KeyRound"
              title="No secret stores yet"
              description="Define secret stores in config.toml to see them here."
            />
          </Table.Status>
        ) : (
          shown.length === 0 && (
            <Table.Status>
              {query.text ? `No secret stores match “${query.text}”.` : 'No secret stores match.'}
            </Table.Status>
          )
        )}
        <Table.Body>
          {sort.rows(shown).map((store) => (
            <Table.Row key={store.name}>
              <Table.Cell title={store.name}>{store.name}</Table.Cell>
              <Table.Cell>{kindLabel(store)}</Table.Cell>
              <Table.Cell>
                <Status blockers={store.blockers} ready="Available" />
              </Table.Cell>
              <Table.Cell wrap>
                <WorkspaceGrants store={store} workspaces={workspaces} form={form} />
              </Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Container>
    </ManagementLayout>
  )
}

/**
 * One option per workspace. Names need not be unique, so a name shared with another
 * choice adds the start of the workspace's ID.
 */
function workspaceOptions(workspaces: { name: string; displayName: string }[]) {
  const seen = new Map<string, number>()
  for (const w of workspaces) seen.set(w.displayName, (seen.get(w.displayName) ?? 0) + 1)
  return workspaces.map((w) => ({
    value: w.name,
    label:
      (seen.get(w.displayName) ?? 0) > 1
        ? `${w.displayName} (${w.name.replace(/^workspaces\//, '').slice(0, 8)})`
        : w.displayName,
  }))
}

/** How many workspaces a cell shows before it counts the rest. */
const SHOWN_GRANTS = 2

/**
 * The workspaces that hold a grant, then a button that edits them when a grant can
 * change. The cell shows the first few and counts the rest.
 */
function WorkspaceGrants({
  store,
  workspaces,
  form,
}: {
  store: SecretStoreRow
  workspaces: ActiveWorkspace[]
  form: FormContext
}) {
  const [editing, setEditing] = useState(false)
  const [chosen, setChosen] = useState<string[]>([])
  const [query, setQuery] = useState('')
  const editButton = useRef<HTMLButtonElement>(null)
  const granted = store.workspaces.map((w) => w.name)
  const archived = new Set(store.workspaces.filter((w) => w.archived).map((w) => w.name))
  // Coral refuses a store config does not define, and any change to an archived workspace.
  // So a store config removed offers only its own grants, and an archived grant stays.
  const offered = store.kind !== null ? workspaces : store.workspaces.filter((w) => !w.archived)
  const labels = new Map(
    workspaceOptions([
      ...store.workspaces,
      ...offered.filter((w) => !granted.includes(w.name)),
    ]).map((o) => [o.value, o.label]),
  )
  const label = (name: string) => {
    const text = labels.get(name) ?? name
    return archived.has(name) ? `${text} · Archived` : text
  }
  const added = chosen.filter((name) => !granted.includes(name))
  const removed = granted.filter((name) => !chosen.includes(name))

  const shown = store.workspaces.slice(0, SHOWN_GRANTS)
  const hidden = store.workspaces.slice(SHOWN_GRANTS)
  const pills = (
    <>
      {store.workspaces.length === 0 && <span className={styles.grantsEmpty}>No workspaces</span>}
      {shown.map((w) => (
        <Pill key={w.name}>{label(w.name)}</Pill>
      ))}
      {hidden.length > 0 && (
        <Tooltip content={hidden.map((w) => label(w.name)).join(', ')}>
          <Pill
            as="button"
            type="button"
            aria-label={`${hidden.length} more: ${hidden.map((w) => label(w.name)).join(', ')}`}
          >
            {`+${hidden.length}`}
          </Pill>
        </Tooltip>
      )}
    </>
  )
  if (offered.length === 0) return <div className={styles.grants}>{pills}</div>

  return (
    <div className={styles.grants}>
      {pills}
      <Button.IconButton
        ref={editButton}
        className={styles.grantsEdit}
        name="Pencil"
        size="22"
        variant="bare"
        tooltipText={`Edit the workspaces of ${store.name}`}
        onClick={() => {
          setChosen(granted)
          setQuery('')
          setEditing(true)
        }}
      />
      <ActionDialog
        open={editing}
        onOpenChange={setEditing}
        finalFocus={editButton}
        title="Edit workspaces"
        description={`Choose the workspaces that can use ${store.name}.`}
        submit="Save changes"
        intent="update"
        name={`secretStores/${store.name}`}
        form={form}
        disabled={added.length === 0 && removed.length === 0}
        confirm={
          removed.length === 0
            ? null
            : {
                title: 'Revoke access?',
                description:
                  removed.length === 1
                    ? `${label(removed[0])} loses access to ${store.name}.`
                    : `${removed.map(label).join(', ')} lose access to ${store.name}.`,
                submit: 'Revoke and save',
              }
        }
      >
        {added.map((name) => (
          <input key={name} type="hidden" name="grant" value={name} />
        ))}
        {removed.map((name) => (
          <input key={name} type="hidden" name="revoke" value={name} />
        ))}
        <div className={styles.addField}>
          <Typography.BodySmallStrong as="label" htmlFor={`${store.name}-workspaces`}>
            Workspaces
          </Typography.BodySmallStrong>
          <Combobox.Root
            autoHighlight
            hideWhenEmpty
            items={offered.map((w) => w.name)}
            itemToStringLabel={label}
            multiple
            inputValue={query}
            onInputValueChange={setQuery}
            value={chosen}
            onValueChange={(next) => {
              const picked = Array.isArray(next) ? next : []
              // An archived workspace keeps its grant: Coral refuses to revoke it.
              setChosen([...archived].filter((n) => !picked.includes(n)).concat(picked))
              setQuery('')
            }}
          >
            <Combobox.InputGroup>
              <Combobox.Chips>
                <Combobox.Value>
                  {(value) =>
                    Array.isArray(value)
                      ? value.map((name: string) => (
                          <Combobox.Chip key={name}>
                            <Combobox.ChipLabel>{label(name)}</Combobox.ChipLabel>
                            {!archived.has(name) && (
                              <Combobox.ChipRemove aria-label={`Remove ${label(name)}`} />
                            )}
                          </Combobox.Chip>
                        ))
                      : null
                  }
                </Combobox.Value>
                <Combobox.Input
                  id={`${store.name}-workspaces`}
                  bare
                  placeholder={chosen.length > 0 ? '' : 'Find a workspace'}
                />
              </Combobox.Chips>
            </Combobox.InputGroup>
            <Combobox.Content>
              <Combobox.List>
                {(name: string) => (
                  <Combobox.Item key={name} value={name}>
                    {label(name)}
                  </Combobox.Item>
                )}
              </Combobox.List>
            </Combobox.Content>
          </Combobox.Root>
        </div>
      </ActionDialog>
    </div>
  )
}
