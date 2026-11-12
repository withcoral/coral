import { useRef, useState } from 'react'
import { TruncatedList } from '@/components/truncated-list'
import { Button, Menu, Table, Typography } from '@/wax/components'
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
          { ariaLabel: 'Actions', width: 48 },
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
              <Table.Cell>
                {store.workspaces.length === 0 ? (
                  'No workspaces'
                ) : (
                  <WorkspacePills workspaces={store.workspaces} />
                )}
              </Table.Cell>
              <Table.Cell actions>
                <GrantActions store={store} workspaces={workspaces} form={form} />
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

/** The workspaces on one line. Workspaces that do not fit move to a "+N" tooltip. */
function WorkspacePills({ workspaces }: { workspaces: GrantedWorkspace[] }) {
  const pill = (w: GrantedWorkspace) => (
    <Pill key={w.name}>{w.archived ? `${w.displayName} · Archived` : w.displayName}</Pill>
  )
  return (
    <TruncatedList
      items={workspaces}
      getKey={(w) => w.name}
      renderItem={pill}
      renderOverflowContent={(hidden) => hidden.map(pill)}
      renderOverflowTrigger={(count) => (
        <Pill as="button" aria-label={`${count} more workspaces`}>
          +{count}
        </Pill>
      )}
    />
  )
}

function GrantActions({
  store,
  workspaces,
  form,
}: {
  store: SecretStoreRow
  workspaces: ActiveWorkspace[]
  form: FormContext
}) {
  const [action, setAction] = useState<'grant' | 'revoke' | null>(null)
  const [chosen, setChosen] = useState('')
  const trigger = useRef<HTMLButtonElement>(null)
  const granted = new Set(store.workspaces.map((w) => w.name))
  // Coral refuses a store config does not define, and any change to an archived workspace.
  const grantable = store.kind !== null ? workspaces.filter((w) => !granted.has(w.name)) : []
  const revocable = store.workspaces.filter((w) => !w.archived)
  if (grantable.length === 0 && revocable.length === 0) return null
  const open = (next: 'grant' | 'revoke') => {
    setChosen('')
    setAction(next)
  }
  const dialog = {
    grant: {
      title: 'Grant secret store',
      intent: 'grant',
      description: `Choose a workspace to grant ${store.name} to.`,
      submit: 'Grant',
      choices: grantable,
    },
    revoke: {
      title: 'Revoke grant',
      intent: 'revoke',
      description: `Choose a workspace to revoke ${store.name} from.`,
      submit: 'Revoke',
      choices: revocable,
    },
  }[action ?? 'grant']
  return (
    <>
      <Menu.Container>
        <Menu.Trigger
          render={
            <Button.IconButton
              ref={trigger}
              name="Ellipsis"
              ariaLabel={`Actions for ${store.name}`}
              variant="bare"
            />
          }
        >
          {null}
        </Menu.Trigger>
        <Menu.Content>
          {grantable.length > 0 && (
            <Menu.Item onClick={() => open('grant')}>Grant to a workspace</Menu.Item>
          )}
          {revocable.length > 0 && (
            <Menu.Item onClick={() => open('revoke')}>Revoke from a workspace</Menu.Item>
          )}
        </Menu.Content>
      </Menu.Container>
      <ActionDialog
        open={action !== null}
        onOpenChange={(next) => {
          if (!next) setAction(null)
        }}
        finalFocus={trigger}
        title={dialog.title}
        description={dialog.description}
        submit={dialog.submit}
        intent={dialog.intent}
        name={`secretStores/${store.name}`}
        form={form}
        destructive={action === 'revoke'}
        disabled={chosen === ''}
      >
        <div className={styles.field}>
          <Typography.BodySmallStrong as="span">Workspace</Typography.BodySmallStrong>
          <input type="hidden" name="workspace" value={chosen} />
          <Picker
            ariaLabel="Workspace"
            placeholder="Choose a workspace"
            options={workspaceOptions(dialog.choices)}
            value={chosen}
            onChange={setChosen}
          />
        </div>
      </ActionDialog>
    </>
  )
}
