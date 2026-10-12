import { Form } from 'react-router'
import { Banner, Button, Typography } from '@/wax/components'
import { Pill } from '@/wax/components/pill'
import { ActionDialog, ActionNotice, Fields } from '@/views/management/forms'
import { ManagementLayout, Section, trail } from '@/views/management/layout'
import { useAccountAudience } from '@/views/providers/account-labels'
import type { AccountView, AccountsView, ActionResult } from './types'
import * as styles from './accounts.css'

export function AccountsPage({
  view: v,
  result,
  pending = false,
}: {
  view: AccountsView
  result?: ActionResult
  pending?: boolean
}) {
  const audience = useAccountAudience()
  const account = v.account
  return (
    <ManagementLayout breadcrumbs={trail(v.parents, v.title)}>
      <ActionNotice result={result} />
      {!v.active && (
        <Banner variant="warning">
          This workspace is archived. Restore it before you change account settings.
        </Banner>
      )}
      <Section
        title={v.scope === 'agent' ? 'Agent account' : 'Shared account'}
        description={
          v.scope === 'agent'
            ? 'You connect this account for the agent. The agent uses it for calls.'
            : audience.sharedAccount
        }
      >
        <Typography.BodySmall as="p">
          Upstream account name and identity are unavailable.
        </Typography.BodySmall>
        {v.scopes.length > 0 && (
          <Typography.BodySmall as="p">
            Requested permissions: {v.scopes.join(', ')}
          </Typography.BodySmall>
        )}
        {!v.needsCredential && (
          <Typography.BodySmall as="p">This provider needs no account.</Typography.BodySmall>
        )}
        {v.needsCredential && !v.usesScope && (
          <Banner variant="info">This provider does not use this account scope.</Banner>
        )}
        {(!account || account.state === 'disconnected') && (
          <Typography.BodySmall as="p">No connected account.</Typography.BodySmall>
        )}
        {v.canConnect && (
          <Form method="post" action={v.path}>
            <Fields form={v.form} intent="start" />
            <Button.TextButton type="submit" disabled={pending}>
              Connect account
            </Button.TextButton>
          </Form>
        )}
        {account && (
          <div role="group" aria-label={`Account ${account.id}`} className={styles.account}>
            <AccountSummary account={account} />
            {!account.canManage && (
              <Typography.BodySmall as="p">You cannot change this account.</Typography.BodySmall>
            )}
            {account.canManage && account.state !== 'disconnected' && (
              <div className={styles.actions}>
                {v.canReconnect && (
                  <ActionDialog
                    title="Reconnect"
                    form={v.form}
                    intent="reconnect"
                    action={v.path}
                    description="Reconnect keeps your stored credentials until sign-in succeeds. Success replaces them and stops unsent calls that use the old authorization. Coral cannot verify the upstream account."
                  >
                    <input type="hidden" name="account" value={account.id} />
                  </ActionDialog>
                )}
                <ActionDialog
                  title="Disconnect"
                  form={v.form}
                  intent="disconnect"
                  action={v.path}
                  destructive
                  description="Disconnect removes this account's stored credentials immediately. Coral stops unsent calls that use this account."
                >
                  <input type="hidden" name="account" value={account.id} />
                </ActionDialog>
              </div>
            )}
          </div>
        )}
      </Section>
    </ManagementLayout>
  )
}

export function AccountSummary({
  account,
  personal = false,
}: {
  account: AccountView
  /** Your own account: you configured it, so the page does not say who did. */
  personal?: boolean
}) {
  return (
    <>
      <Typography.BodySmallStrong as="p" className={styles.reference}>
        Account {account.id}
      </Typography.BodySmallStrong>
      <Pill color={account.state === 'ready' ? 'green' : 'amber'}>
        {
          { ready: 'Ready', reconnect: 'Reconnect needed', disconnected: 'Disconnected' }[
            account.state
          ]
        }
      </Pill>
      {account.connectedAt && (
        <Typography.BodySmall as="p">
          Last connected: <time dateTime={account.connectedAt}>{account.connectedAt}</time>
        </Typography.BodySmall>
      )}
      {!personal && account.configuredByUser && (
        <Typography.BodySmall as="p" className={styles.reference}>
          Configured by Coral user: {account.configuredByUser}
        </Typography.BodySmall>
      )}
      {account.state !== 'disconnected' && account.automaticRefresh === false && (
        <Typography.BodySmall as="p">
          This account cannot refresh automatically. Reconnect when access expires.
        </Typography.BodySmall>
      )}
      {account.grantedScope && (
        <Typography.BodySmall as="p">
          Granted permissions: {account.grantedScope}
        </Typography.BodySmall>
      )}
    </>
  )
}
