import { useDeferredValue, useState } from 'react'
import { Button, Menu, Table, Typography } from '@/wax/components'
import type { IconName } from '@/wax/components/icon'
import { Deadline } from '@/wax/components/deadline'
import { Pill } from '@/wax/components/pill'
import type { PillColor } from '@/wax/components/pill/pill'
import { CatalogToolbar } from '@/components/catalog'
import { ManagementLayout, trail } from '@/views/management/layout'
import type { PersonalAccountState, UserAccountRow, UserAccountsView } from './types'
import { accountStates } from './account-states'
import { ConnectButton } from './connect-button'
import { DisconnectAccount } from './disconnect-account'
import * as styles from './accounts.css'

const states: Record<
  PersonalAccountState,
  { label: string; color: PillColor; action: string; icon: IconName }
> = {
  connected: { ...accountStates.connected, action: 'Connect', icon: 'CircleCheck' },
  not_connected: { ...accountStates.not_connected, action: 'Connect', icon: 'CircleDashed' },
  needs_reconnect: { ...accountStates.needs_reconnect, action: 'Reconnect', icon: 'CircleAlert' },
}

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
  const [state, setState] = useState<PersonalAccountState | null>(null)
  const query = useDeferredValue(input).trim()
  const shown = v.accounts.filter(
    (row) =>
      row.name.toLowerCase().includes(query.toLowerCase()) && (!state || row.state === state),
  )
  const table = Table.useRowCursor({ filterId: FILTER_ID })

  return (
    <ManagementLayout breadcrumbs={trail([], 'Accounts')}>
      {v.accounts.length ? (
        <>
          <CatalogToolbar
            filter={{
              id: FILTER_ID,
              label: 'Filter accounts',
              shortcut: '$mod+f',
              value: input,
              onChange: setInput,
            }}
          >
            <StatusFilterMenu value={state} onChange={setState} />
          </CatalogToolbar>
          <Table.Container
            ariaLabel="Accounts"
            ref={table}
            variant="card"
            layout="fixed"
            columns={[
              { label: 'Provider', width: { fill: 2 } },
              { label: 'Status', width: { fill: 2 } },
              // Fixed, so filtering out the widest button does not move the other columns.
              { ariaLabel: 'Actions', width: 104, align: 'right' },
            ]}
          >
            <Table.Head />
            {shown.length === 0 && <Table.Status>No accounts match.</Table.Status>}
            <Table.Body>
              {shown.map((row) => (
                <Table.Row key={row.provider} to={row.detailPath}>
                  <Table.Cell>
                    <Table.RowLink to={row.detailPath} aria-label={`${row.name} account details`}>
                      {row.name}
                    </Table.RowLink>
                  </Table.Cell>
                  <Table.Cell>
                    <span className={styles.accountStatus}>
                      <Pill color={states[row.state].color}>{states[row.state].label}</Pill>
                      {row.waitingUntil && (
                        <span className={styles.actions}>
                          <Typography.BodySmallStrong>
                            A connection request is waiting.
                          </Typography.BodySmallStrong>
                          <Deadline at={row.waitingUntil} />
                        </span>
                      )}
                    </span>
                  </Table.Cell>
                  <Table.Cell control>
                    <AccountAction row={row} view={v} pending={pending} />
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Container>
        </>
      ) : (
        <Typography.BodySmall as="p">
          No provider in this workspace uses individual accounts.
        </Typography.BodySmall>
      )}
    </ManagementLayout>
  )
}

function AccountAction({
  row,
  view,
  pending,
}: {
  row: UserAccountRow
  view: UserAccountsView
  pending: boolean
}) {
  if (row.state === 'connected')
    return row.canDisconnect && row.accountId ? (
      <DisconnectAccount form={view.form} action={view.path} size="22">
        <input type="hidden" name="provider" value={row.provider} />
        <input type="hidden" name="account" value={row.accountId} />
      </DisconnectAccount>
    ) : null
  if (!row.canConnect) return null
  return (
    <ConnectButton
      action={row.connectPath}
      csrf={view.form.csrf}
      requestId={view.form.requestId}
      label={states[row.state].action}
      ariaLabel={`${states[row.state].action} ${row.name}`}
      primary={Boolean(row.waitingUntil)}
      disabled={pending}
      size="22"
    />
  )
}

function StatusFilterMenu({
  value,
  onChange,
}: {
  value: PersonalAccountState | null
  onChange: (value: PersonalAccountState | null) => void
}) {
  const active = value ? states[value] : undefined
  return (
    <Menu.Container>
      <Menu.Trigger
        render={<Button.Container variant="secondary" ariaLabel="Filter accounts by status" />}
      >
        <Button.Icon name={active?.icon ?? 'ListFilter'} />
        <Button.Text>{active?.label ?? 'All statuses'}</Button.Text>
        <Button.Icon name="ChevronDown" />
      </Menu.Trigger>
      <Menu.Content>
        <Menu.RadioGroup
          value={value ?? 'all'}
          onValueChange={(next: string) =>
            onChange(next === 'all' ? null : (next as PersonalAccountState))
          }
        >
          <Menu.RadioItem value="all" iconName="ListFilter">
            All statuses
          </Menu.RadioItem>
          {(Object.keys(states) as PersonalAccountState[]).map((key) => (
            <Menu.RadioItem key={key} value={key} iconName={states[key].icon}>
              {states[key].label}
            </Menu.RadioItem>
          ))}
        </Menu.RadioGroup>
      </Menu.Content>
    </Menu.Container>
  )
}
