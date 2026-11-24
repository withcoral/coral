import { useDeferredValue, useRef, useState } from 'react'
import { CatalogFilterMenu, CatalogToolbar, type CatalogFilterOptions } from '@/components/catalog'
import { EmptyPage } from '@/components/empty-page'
import { filterQuery } from '@/utils/filter-query'
import { Button, Table, Typography } from '@/wax/components'
import { Picker } from '@/wax/components/picker'
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

const SORT_COLUMNS = {
  name: Table.byText((store: SecretStoreRow) => store.name),
  kind: Table.byText((store: SecretStoreRow) => store.kind ?? 'Unknown'),
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
  // The filter matches the store's name, its kind and the workspaces that hold a grant.
  const shown = stores.filter(
    (store) =>
      (status === 'all' || (status === 'blocked') === store.blockers.length > 0) &&
      query.matches(store.name, store.kind, ...store.workspaces.map((w) => w.displayName)),
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
              description="The secret stores that you define in config.toml show here."
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
              <Table.Cell>{store.kind ?? 'Unknown'}</Table.Cell>
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

type Change = { kind: 'grant' } | { kind: 'revoke'; workspace: GrantedWorkspace }

/**
 * The workspaces that hold a grant, each with a button that revokes it unless the
 * workspace is archived. Then, when a workspace can still take the store, a button
 * that grants it.
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
  const [change, setChange] = useState<Change | null>(null)
  const [chosen, setChosen] = useState('')
  const grantButton = useRef<HTMLButtonElement>(null)
  const granted = new Set(store.workspaces.map((w) => w.name))
  // Coral refuses a store config does not define, and any change to an archived workspace.
  const grantable = store.kind !== null ? workspaces.filter((w) => !granted.has(w.name)) : []
  const labels = new Map(workspaceOptions(store.workspaces).map((o) => [o.value, o.label]))
  const label = (w: GrantedWorkspace) => labels.get(w.name) ?? w.displayName
  const revoking = change?.kind === 'revoke' ? change.workspace : null
  if (store.workspaces.length === 0 && grantable.length === 0) return 'No workspaces'
  return (
    <div className={styles.grants}>
      {store.workspaces.map((w) =>
        w.archived ? (
          <Pill key={w.name}>{`${label(w)} · Archived`}</Pill>
        ) : (
          <Pill
            key={w.name}
            remove={{
              label: `Revoke ${store.name} from ${label(w)}`,
              onClick: () => setChange({ kind: 'revoke', workspace: w }),
            }}
          >
            {label(w)}
          </Pill>
        ),
      )}
      {grantable.length > 0 && (
        <Button.Container
          ref={grantButton}
          ariaLabel={`Grant ${store.name} to a workspace`}
          size="22"
          variant="bare"
          onClick={() => {
            setChosen('')
            setChange({ kind: 'grant' })
          }}
        >
          <Button.Icon name="Plus" />
          <Button.Text>Grant</Button.Text>
        </Button.Container>
      )}
      <ActionDialog
        open={change !== null}
        onOpenChange={(next) => {
          if (!next) setChange(null)
        }}
        finalFocus={revoking ? undefined : grantButton}
        title={revoking ? 'Revoke grant' : 'Grant secret store'}
        description={
          revoking
            ? `${label(revoking)} loses access to ${store.name}.`
            : `Choose a workspace to grant ${store.name} to.`
        }
        submit={revoking ? 'Revoke' : 'Grant'}
        intent={revoking ? 'revoke' : 'grant'}
        name={`secretStores/${store.name}`}
        form={form}
        destructive={revoking !== null}
        disabled={revoking === null && chosen === ''}
      >
        {revoking ? (
          <input type="hidden" name="workspace" value={revoking.name} />
        ) : (
          <div className={styles.field}>
            <Typography.BodySmallStrong as="span">Workspace</Typography.BodySmallStrong>
            <input type="hidden" name="workspace" value={chosen} />
            <Picker
              ariaLabel="Workspace"
              placeholder="Choose a workspace"
              options={workspaceOptions(grantable)}
              value={chosen}
              onChange={setChosen}
            />
          </div>
        )}
      </ActionDialog>
    </div>
  )
}
