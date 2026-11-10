import { useDeferredValue, useState } from 'react'
import { Button, Table } from '@/wax/components'
import type { IconName } from '@/wax/components/icon'
import { Deadline } from '@/wax/components/deadline'
import { Logo } from '@/wax/components/logo'
import { Pill } from '@/wax/components/pill'
import type { PillColor } from '@/wax/components/pill/pill'
import { CatalogFilterMenu, CatalogToolbar, type CatalogFilterOptions } from '@/components/catalog'
import { EmptyPage } from '@/components/empty-page'
import { ConnectAccountDialog } from '@/views/management/connect-account-dialog'
import { useAccountAudience } from '@/views/providers/account-labels'
import { ManagementLayout, trail } from '@/views/management/layout'
import type {
  PersonalAccountState,
  UserAccountRow,
  UserAccountsView,
  ConnectionRequest,
} from './types'
import { accountStates } from './account-states'
import { ConnectButton } from './connect-button'
import { DisconnectAccount } from './disconnect-account'
import * as styles from './accounts.css'
import { filterQuery } from '@/utils/filter-query'
import { useFollowRequiredActions } from '@/lib/use-action-signal'
import { DeclineConnection } from '@/views/actions/decline-connection'

const states: Record<
  PersonalAccountState,
  { label: string; color: PillColor; action: string; icon: IconName }
> = {
  connected: { ...accountStates.connected, action: 'Connect', icon: 'CircleCheck' },
  not_connected: { ...accountStates.not_connected, action: 'Connect', icon: 'CircleDashed' },
  needs_reconnect: { ...accountStates.needs_reconnect, action: 'Reconnect', icon: 'CircleAlert' },
}

const STATUS_FILTERS: CatalogFilterOptions<PersonalAccountState | 'all'> = [
  { value: 'all', label: 'All statuses', icon: 'ListFilter' },
  ...(Object.keys(states) as PersonalAccountState[]).map((value) => ({
    value,
    label: states[value].label,
    icon: states[value].icon,
  })),
]

const FILTER_ID = 'accounts-filter'

/** Your own account for each provider that uses individual accounts, in one workspace. */
export function UserAccountsPage({
  view: v,
  pending = false,
}: {
  view: UserAccountsView
  pending?: boolean
}) {
  const [input, setInput] = useState('')
  const [state, setState] = useState<PersonalAccountState | 'all'>('all')
  const query = filterQuery(useDeferredValue(input))
  const shown = v.accounts.filter(
    (row) => query.matches(row.name) && (state === 'all' || row.state === state),
  )
  const table = Table.useRowCursor({ filterId: FILTER_ID })
  // A new or ended connection request refreshes the rows in place, so the filter and an
  // open connect dialog stay.
  useFollowRequiredActions(v.workspace.path, 'connections', v.revision)

  return (
    <ManagementLayout breadcrumbs={trail([], 'Accounts')} titled compact>
      {/* The empty state sits in the table, so the page keeps its shape at the first account. */}
      <CatalogToolbar
        title="Accounts"
        filter={{
          id: FILTER_ID,
          label: 'Filter accounts',
          shortcut: '$mod+f',
          value: input,
          onChange: setInput,
        }}
      >
        <CatalogFilterMenu
          ariaLabel="Filter accounts by status"
          options={STATUS_FILTERS}
          value={state}
          onChange={setState}
        />
      </CatalogToolbar>
      <Table.Container
        ariaLabel="Accounts"
        ref={table}
        variant="card"
        layout="fixed"
        columns={[
          { label: 'Provider', width: { fill: 2 } },
          { label: 'Status', width: { fill: 2 } },
          // Fixed for a screen width, so filtering out the widest buttons does not move the
          // other columns. It fits a request's deadline, Decline, and Connect on one line; on a
          // narrow screen the deadline shortens, then hides, and the buttons keep their width.
          {
            ariaLabel: 'Actions',
            width: 'clamp(176px, calc(100% - 300px), 448px)',
            align: 'right',
          },
        ]}
      >
        <Table.Head />
        {v.accounts.length === 0 ? (
          <Table.Status>
            <EmptyPage
              fullSize={false}
              iconName="KeyRound"
              title="No accounts yet"
              description="Your accounts show here when a provider uses individual accounts."
              action={
                <Button.InternalLink to={`${v.workspace.path}/tools`} variant="secondary">
                  View tools
                </Button.InternalLink>
              }
            />
          </Table.Status>
        ) : (
          shown.length === 0 && (
            <Table.Status>
              {query.text ? `No accounts match “${query.text}”.` : 'No accounts match.'}
            </Table.Status>
          )
        )}
        <Table.Body>
          {shown.map((row) => (
            <Table.Row key={row.provider} to={row.detailPath}>
              <Table.Cell>
                <span className={styles.provider}>
                  <Logo name={row.name} icons={row.icons} size="20" />
                  <Table.RowLink to={row.detailPath} aria-label={`${row.name} account details`}>
                    {row.name}
                  </Table.RowLink>
                </span>
              </Table.Cell>
              <Table.Cell>
                <Pill color={states[row.state].color}>{states[row.state].label}</Pill>
              </Table.Cell>
              <Table.Cell>
                <AccountAction row={row} view={v} pending={pending} />
              </Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Container>
    </ManagementLayout>
  )
}

/**
 * A waiting connection request's deadline. A narrower row drops the subject, and a
 * phone-width row drops the deadline.
 */
function WaitingRequest({ request }: { request: ConnectionRequest }) {
  return (
    <span className={styles.requestNote}>
      <span className={styles.wideNote}>
        <Deadline at={request.expiresAt} subject="Connection request" variant="secondary" />
      </span>
      <span className={styles.narrowNote}>
        <Deadline at={request.expiresAt} variant="secondary" />
      </span>
    </span>
  )
}

/**
 * Disconnect for a connected account. Otherwise OAuth signs in at the provider
 * in one click, and a secret opens a dialog on this page.
 */
function AccountAction({
  row,
  view,
  pending,
}: {
  row: UserAccountRow
  view: UserAccountsView
  pending: boolean
}) {
  const audience = useAccountAudience()
  const connected = row.state === 'connected'
  const action = states[row.state].action
  const disconnect =
    connected && row.canDisconnect && row.accountId ? (
      <DisconnectAccount form={view.form} action={view.path} quiet>
        <input type="hidden" name="provider" value={row.provider} />
        <input type="hidden" name="account" value={row.accountId} />
      </DisconnectAccount>
    ) : null
  const waiting = row.connectionRequest
  const connect = row.oauth ? (
    connected || !row.canConnect ? (
      disconnect
    ) : (
      <ConnectButton
        action={row.connectPath}
        csrf={view.form.csrf}
        requestId={view.form.requestId}
        label={action}
        ariaLabel={`${action} ${row.name}`}
        primary={Boolean(waiting)}
        disabled={pending}
      />
    )
  ) : (
    // Mounted while connected too, so an open dialog sees the account land.
    <ConnectAccountDialog
      providerName={row.name}
      namespace={row.name}
      form={view.form}
      connectable={!connected && row.canConnect}
      reconnect={row.state === 'needs_reconnect'}
      ready={connected}
      fallback={disconnect}
      trigger={{
        label: action,
        ariaLabel: `${action} ${row.name}`,
        variant: waiting ? 'primary' : 'secondary',
        size: '32',
      }}
      action={row.connectPath}
      description={audience.onlyYou}
    />
  )
  // The deadline leads. Like an approval, Decline comes next and the primary choice ends.
  // Connect keeps its place as a request comes and goes, so an open dialog stays open.
  return (
    <span className={styles.rowActions}>
      {waiting && (
        <>
          <WaitingRequest request={waiting} />
          <DeclineConnection
            request={waiting.name}
            ariaLabel={`Decline ${row.name}`}
            disabled={pending}
          />
        </>
      )}
      {connect}
    </span>
  )
}
