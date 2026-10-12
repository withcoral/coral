import { Banner, Button, Inputs, Typography } from '@/wax/components'
import { CredentialFields } from '@/views/management/credential-fields'
import { Fields } from '@/views/management/forms'
import { ManagementLayout, trail } from '@/views/management/layout'
import { AccountSummary } from './accounts-page'
import { ConnectButton } from './connect-button'
import { DisconnectAccount } from './disconnect-account'
import type { UserAccountView } from './types'
import * as styles from './accounts.css'

/** Your own account for one provider. A waiting run, Accounts, and MCP clients open it. */
export function UserAccountPage({
  view: v,
  pending = false,
}: {
  view: UserAccountView
  pending?: boolean
}) {
  const verb = v.state === 'needs_reconnect' ? 'Reconnect' : 'Connect'
  const returnsToRun = v.returnTo && v.returnTo !== v.accountsPath
  return (
    <ManagementLayout
      breadcrumbs={trail([{ label: 'Accounts', to: v.accountsPath }], v.providerName)}
    >
      <Typography.BodySmall as="p">
        Upstream account name and identity are unavailable.
      </Typography.BodySmall>
      {v.scopes.length > 0 && (
        <Typography.BodySmall as="p">
          Requested permissions: {v.scopes.join(', ')}
        </Typography.BodySmall>
      )}
      {v.state === 'not_connected' && (
        <Typography.BodySmall as="p">No connected account.</Typography.BodySmall>
      )}
      {v.account && (
        <div role="group" aria-label={`Account ${v.account.id}`} className={styles.account}>
          <AccountSummary account={v.account} personal />
        </div>
      )}
      {!v.canConnect && v.state !== 'connected' ? (
        <Banner variant="info">
          You cannot connect this account now. Check the workspace and provider state.
        </Banner>
      ) : v.state === 'connected' ? (
        <div className={styles.actions}>
          {v.canConnect && (
            <ConnectButton
              action={v.path}
              csrf={v.form.csrf}
              requestId={v.form.requestId}
              label="Reconnect"
              primary={false}
              disabled={pending}
            />
          )}
        </div>
      ) : v.oauth ? (
        <>
          <Typography.BodySmall as="p">
            Sign in to {v.providerName} to connect your account.
          </Typography.BodySmall>
          <ConnectButton
            action={v.path}
            csrf={v.form.csrf}
            requestId={v.form.requestId}
            label={`${verb} ${v.providerName}`}
            disabled={pending}
          />
        </>
      ) : (
        <form method="post" action={v.path} className={styles.stack}>
          <Fields form={v.form} intent="secret" />
          <CredentialFields fields={v.credentialFields}>
            {(input) => <Inputs.CredentialInput {...input} />}
          </CredentialFields>
          <Typography.BodySmall as="p">
            Enter the credentials from {v.providerName}. Coral does not show them again.
          </Typography.BodySmall>
          <Button.TextButton type="submit">{`${verb} ${v.providerName}`}</Button.TextButton>
        </form>
      )}
      {/* You can remove stored credentials whether or not they still work. */}
      {v.canDisconnect && v.account && (
        <div className={styles.actions}>
          <DisconnectAccount form={v.form} action={v.path} />
        </div>
      )}
      {returnsToRun && (
        <Button.InternalLink to={v.returnTo!} variant="secondary">
          Return to run
        </Button.InternalLink>
      )}
    </ManagementLayout>
  )
}
