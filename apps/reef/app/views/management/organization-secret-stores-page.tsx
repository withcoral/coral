import { useRef, useState } from 'react'
import { Button, Table, Typography } from '@/wax/components'
import { Picker } from '@/wax/components/picker'
import { Pill } from '@/wax/components/pill'
import { Status } from '@/wax/components/status'
import { ActionDialog } from './forms'
import { ManagementLayout, trail } from './layout'
import type { FormContext } from './types'
import * as styles from './management.css'

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
  return (
    <ManagementLayout breadcrumbs={trail([], 'Secret stores')} compact>
      <Table.Container
        ariaLabel="Secret stores"
        variant="card"
        layout="fixed"
        staticRows
        columns={[
          { label: 'Name', width: 'fill' },
          { label: 'Kind', width: 'fill' },
          { label: 'Status', width: 'fill' },
          { label: 'Workspaces', width: { fill: 2 } },
        ]}
      >
        <Table.Head />
        {stores.length === 0 && (
          <Table.Status>No secret stores. To define one, edit config.toml.</Table.Status>
        )}
        <Table.Body>
          {stores.map((store) => (
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
