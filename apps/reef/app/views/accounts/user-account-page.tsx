import { Banner, Button, Inputs, Table, Typography } from '@/wax/components'
import { PageLayout } from '@/wax/components/page-layout'
import {
  SettingsAside,
  SettingsFact,
  SettingsFacts,
  SettingsLayout,
  SettingsRemoval,
} from '@/wax/components/settings-layout'
import { ConnectAccountDialog } from '@/views/management/connect-account-dialog'
import { CredentialFields } from '@/views/management/credential-fields'
import { Fields } from '@/views/management/forms'
import { Section, trail } from '@/views/management/layout'
import { SavedAt } from '@/views/functions/saved-at'
import { ConnectButton } from './connect-button'
import { DisconnectAccount } from './disconnect-account'
import { PermissionsTable, permissionRows } from './permissions-table'
import type { CredentialFieldView } from '@/views/providers/types'
import type { AccountView, UserAccountView } from './types'
import * as styles from './accounts.css'

/** Your own account for one provider. A waiting run, Accounts, and MCP clients open it. */
export function UserAccountPage({
  view: v,
  pending = false,
}: {
  view: UserAccountView
  pending?: boolean
}) {
  const connected = v.state === 'connected'
  const verb = v.state === 'needs_reconnect' ? 'Reconnect' : 'Connect'
  const returns = v.returnTo && v.returnTo !== v.accountsPath
  // A provider's settings and a run both open this page; the button names which.
  const returnLabel = v.returnTo?.split('?')[0].endsWith('/settings')
    ? 'Return to settings'
    : 'Return to run'
  const blocked = !v.canConnect && !connected
  const stored = v.account && v.account.state !== 'disconnected' ? v.account : undefined
  const permissions = permissionRows(v.scopes)
  const description = blocked ? undefined : guidance(v)
  return (
    <PageLayout
      variant="wide"
      breadcrumbs={trail([{ label: 'Accounts', to: v.accountsPath }], v.providerName)}
    >
      <SettingsLayout
        header={
          <div className={styles.heading}>
            <div className={styles.headingText}>
              <Typography.HeadingLarge as="h2">{`${v.providerName} account`}</Typography.HeadingLarge>
              {description && <Typography.Body as="p">{description}</Typography.Body>}
            </div>
            {/* OAuth signs in at the provider in one click. A secret uses the form
                below until it connects; after that, Reconnect opens a dialog. */}
            {v.canConnect && v.oauth && (
              <ConnectButton
                action={v.path}
                csrf={v.form.csrf}
                requestId={v.form.requestId}
                label={connected ? 'Reconnect' : verb}
                primary={!connected}
                disabled={pending}
              />
            )}
            {v.canConnect && !v.oauth && connected && (
              <ConnectAccountDialog
                providerName={v.providerName}
                namespace={v.providerName}
                form={v.form}
                connectable
                reconnect
                ready
                fallback={null}
                trigger={{ label: 'Reconnect', variant: 'secondary', size: '32' }}
                action={v.path}
                description="Only you use this account."
              />
            )}
          </div>
        }
        aside={stored && <AccountDetails account={stored} />}
        removal={
          // You can remove stored credentials whether or not they still work.
          v.canDisconnect &&
          v.account && (
            <SettingsRemoval
              title="Disconnect"
              description="Remove your stored credentials. Coral stops unsent calls that use this account."
              action={<DisconnectAccount form={v.form} action={v.path} />}
            />
          )
        }
      >
        {blocked && (
          <Banner variant="info">
            You cannot connect this account now. Check the workspace and provider state.
          </Banner>
        )}
        {!blocked && !v.oauth && !connected && (
          // The button sits beside the last input at its height, so a one-field form reads as one line.
          <form method="post" action={v.path} className={styles.credentialForm}>
            <Fields form={v.form} intent="secret" />
            <div className={styles.credentialInputs}>
              <CredentialFields fields={v.credentialFields}>
                {(input) => <Inputs.CredentialInput {...input} />}
              </CredentialFields>
            </div>
            <Button.TextButton type="submit" size="36">
              {verb}
            </Button.TextButton>
          </form>
        )}
        {stored && !v.oauth && v.credentialFields.length > 0 && (
          <StoredCredentials fields={v.credentialFields} />
        )}
        {/* OAuth always says what it asks for. An empty request (a GitHub App) gets a sentence, not an empty table. */}
        {(permissions.length > 0 || v.oauth) && (
          <Section
            title="Permissions"
            description={
              permissions.length > 0
                ? `Coral asks for ${permissions.length} ${permissions.length === 1 ? 'permission' : 'permissions'}.`
                : `Coral asks for no permissions. ${v.providerName} decides what this account can access.`
            }
          >
            {permissions.length > 0 && <PermissionsTable rows={permissions} />}
          </Section>
        )}
        {returns && (
          <div className={styles.actions}>
            <Button.InternalLink to={v.returnTo!} variant="secondary">
              {returnLabel}
            </Button.InternalLink>
          </div>
        )}
      </SettingsLayout>
    </PageLayout>
  )
}

/** What you do next. A working account needs nothing, so it gets no sentence. */
function guidance(v: UserAccountView): string | undefined {
  if (v.state === 'connected') return undefined
  const lead = v.state === 'needs_reconnect' ? 'Coral cannot use this account. ' : ''
  return v.oauth
    ? `${lead}Sign in to ${v.providerName} to ${v.state === 'needs_reconnect' ? 'reconnect' : 'connect'} it.`
    : `${lead}Enter the credentials from ${v.providerName}.`
}

/** What Coral knows about your stored account, beside the permissions. */
function AccountDetails({ account }: { account: AccountView }) {
  return (
    <SettingsAside title="Details">
      <SettingsFacts>
        {account.connectedAt && (
          <SettingsFact label="Last connected">
            <SavedAt at={account.connectedAt} />
          </SettingsFact>
        )}
        {account.automaticRefresh === false && (
          <SettingsFact label="Refresh">Manual. Reconnect when access expires.</SettingsFact>
        )}
        {/* Exactly as the service reported it. Services name scopes in their own ways. */}
        {account.grantedScope && (
          <SettingsFact label="Granted permissions">
            <span className={styles.reference}>{account.grantedScope}</span>
          </SettingsFact>
        )}
        {/* Coral's own record, for support. It is not your identity at the provider. */}
        <SettingsFact label="Coral account ID">
          <Typography.BodySmall as="span" variant="tertiary" className={styles.reference}>
            {account.id}
          </Typography.BodySmall>
        </SettingsFact>
      </SettingsFacts>
    </SettingsAside>
  )
}

/** Names what Coral stores for a secret account. Coral never shows the values. */
function StoredCredentials({ fields }: { fields: CredentialFieldView[] }) {
  return (
    <Section title="Credentials" description="Reconnect to replace these credentials.">
      <Table.Container
        ariaLabel="Credentials"
        variant="card"
        density="compact"
        staticRows
        columns={[
          { label: 'Credential', width: 'minmax(160px, 1fr)' },
          { label: 'Status', width: 'minmax(120px, 1fr)' },
        ]}
      >
        <Table.Head />
        <Table.Body>
          {fields.map((field) => (
            <Table.Row key={field.name}>
              <Table.Cell>{field.label}</Table.Cell>
              <Table.Cell>Stored</Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Container>
    </Section>
  )
}
