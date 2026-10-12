import { Button, Typography } from '@/wax/components'
import { Deadline } from '@/wax/components/deadline'
import { Pill } from '@/wax/components/pill'
import type { PillColor } from '@/wax/components/pill/pill'
import * as SettingRow from '@/wax/components/setting-row'
import { ManagementLayout, trail } from '@/views/management/layout'
import type { PersonalAccountState, UserAccountRow, UserAccountsView } from './types'
import { accountStates } from './account-states'
import { ConnectButton } from './connect-button'
import { DisconnectAccount } from './disconnect-account'
import * as styles from './accounts.css'

const states: Record<PersonalAccountState, { label: string; color: PillColor; action: string }> = {
  connected: { ...accountStates.connected, action: 'Connect' },
  not_connected: { ...accountStates.not_connected, action: 'Connect' },
  needs_reconnect: { ...accountStates.needs_reconnect, action: 'Reconnect' },
}

/** Your own account for each provider that uses individual accounts, in one workspace. */
export function UserAccountsPage({
  view: v,
  pending = false,
}: {
  view: UserAccountsView
  pending?: boolean
}) {
  return (
    <ManagementLayout breadcrumbs={trail([], 'Accounts')}>
      {v.accounts.length ? (
        <SettingRow.List>
          {v.accounts.map((row) => (
            <SettingRow.Row
              key={row.provider}
              label={row.name}
              value={<Pill color={states[row.state].color}>{states[row.state].label}</Pill>}
              description={
                row.waitingUntil && (
                  <span className={styles.actions}>
                    <Typography.BodySmallStrong>
                      A connection request is waiting.
                    </Typography.BodySmallStrong>
                    <Deadline at={row.waitingUntil} />
                  </span>
                )
              }
              action={
                <span className={styles.actions}>
                  <AccountAction row={row} view={v} pending={pending} />
                  <Button.InternalLink
                    to={row.detailPath}
                    variant="secondary"
                    size="small"
                    aria-label={`${row.name} account details`}
                  >
                    Details
                  </Button.InternalLink>
                </span>
              }
            />
          ))}
        </SettingRow.List>
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
      <DisconnectAccount form={view.form} action={view.path}>
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
    />
  )
}
