import { Form } from 'react-router'

import { Banner, Button, Typography } from '@/wax/components'
import { PageLayout } from '@/wax/components/page-layout'
import { Card } from '@/wax/components/card/card'
import { Pill } from '@/wax/components/pill'
import { ActionDialog, Fields } from '@/views/management/forms'
import { ArchivedBanner, trail } from '@/views/management/layout'
import { NameHeading } from '@/views/management/name-heading'
import {
  SettingsAside,
  SettingsFact,
  SettingsFacts,
  SettingsLayout,
  SettingsRemoval,
} from '@/wax/components/settings-layout'

import type {
  FormContext,
  ProviderSettingsPageProps,
  ProviderSettingsView,
  ProviderSourceView,
} from './types'
import { accountStates } from '@/views/accounts/account-states'
import { ConnectAccountDialog } from '@/views/management/connect-account-dialog'
import { accountLabels, useAccountAudience } from './account-labels'
import { SourceUrl, sourceKinds } from './source-facts'
import * as styles from './providers.css'

/**
 * One provider's settings as a page of its own. It shows only what existing
 * reads return; configuration stays in the staged update flow, which only an
 * explicit edit opens.
 */
export function ProviderSettingsPage({
  workspace: w,
  provider,
  source,
  form,
  pending = false,
}: ProviderSettingsPageProps) {
  const owner = w.effectiveRole === 'owner'
  const active = w.state === 'active'
  // Coral manages the Built-in Provider: owners read it and set its operations' policy only.
  const canChange = owner && active && !provider.builtin
  return (
    <PageLayout
      variant="wide"
      breadcrumbs={trail(
        [
          { label: 'Providers', to: `${w.path}/providers` },
          { label: provider.displayName, to: provider.path },
        ],
        'Settings',
      )}
    >
      <SettingsLayout
        header={
          <>
            <ProviderNameHeading provider={provider} form={form} canRename={canChange} />
            {!active && (
              <ArchivedBanner workspace={w}>
                You can read these settings. Restore the workspace to change them.
              </ArchivedBanner>
            )}
          </>
        }
        aside={source && <SourcePanel provider={provider} source={source} canChange={canChange} />}
        removal={canChange && <Removal provider={provider} form={form} />}
      >
        <AccountSection
          workspacePath={w.path}
          provider={provider}
          form={form}
          owner={owner}
          active={active}
          canChange={canChange}
          pending={pending}
        />
        {canChange && (
          <section aria-label="Configuration">
            <Card title="Configuration" description="Change destinations or sign-in settings.">
              <div className={styles.rowActions}>
                <UpdateEntry
                  provider={provider}
                  form={form}
                  task="configuration"
                  label="Edit configuration"
                  pending={pending}
                />
              </div>
            </Card>
          </section>
        )}
      </SettingsLayout>
    </PageLayout>
  )
}

/** The provider name heads the settings under the breadcrumb, which titles the page. */
function ProviderNameHeading({
  provider,
  form,
  canRename,
}: {
  provider: ProviderSettingsView
  form: FormContext
  canRename: boolean
}) {
  return (
    <NameHeading
      name={provider.displayName}
      noun="provider"
      canRename={canRename}
      form={form}
      intent="renameProvider"
      etag={provider.etag}
      fields={<input type="hidden" name="provider" value={provider.id} />}
    >
      {!provider.enabled && (
        <Pill color="amber" data-provider-state="disabled">
          Disabled
        </Pill>
      )}
      {!provider.operationsReady && (
        <Pill color="amber" data-provider-state="no-operations">
          No operations ready
        </Pill>
      )}
    </NameHeading>
  )
}

/**
 * One card for the account calls use: whose it is, its connection, and its
 * actions. Changing the setting and managing credentials stay separate
 * actions: their consequences differ.
 */
function AccountSection({
  workspacePath,
  provider,
  form,
  owner,
  active,
  canChange,
  pending,
}: {
  workspacePath: string
  provider: ProviderSettingsView
  form: FormContext
  owner: boolean
  active: boolean
  /** The reader may change the account setting. */
  canChange: boolean
  pending: boolean
}) {
  const audience = useAccountAudience()
  // Whose account calls use, named as the account setting names it.
  const card = {
    caller: { title: accountLabels.caller, description: audience.individualAccounts },
    fixed: { title: accountLabels.fixed, description: audience.sharedAccount },
    none: { title: 'No account is required.', description: undefined },
  }[provider.account]
  const required = provider.account !== 'none'
  const shared = provider.account === 'fixed'
  const connected = provider.connection === 'connected'
  // Your own account has its page under Accounts, which brings you back here;
  // owners manage the shared one under the provider.
  const accountsPath = shared
    ? `${provider.path}/accounts?scope=shared`
    : `${workspacePath}/accounts/${provider.id}?${new URLSearchParams({
        returnTo: `${provider.path}/settings`,
      })}`
  const whose = shared ? 'shared account' : 'your account'
  // Members cannot open the shared account; only its owners connect it.
  const canOpen = required && (!shared || owner)
  const reconnect = provider.connection === 'needs_reconnect' && active
  const manage = (
    <Button.InternalLink to={accountsPath} variant="secondary" size="small">
      {`Manage ${whose}`}
    </Button.InternalLink>
  )
  // What the reader may do with the account: connect it, reconnect it, or neither.
  const verb =
    !canOpen || provider.connection === 'connected' || provider.connection === 'unknown'
      ? null
      : reconnect
        ? provider.canReconnect
          ? 'Reconnect'
          : null
        : active && provider.canConnect
          ? 'Connect'
          : null
  const link = verb && (
    <Button.InternalLink to={accountsPath} variant="primary" size="small">
      {`${verb} ${whose}`}
    </Button.InternalLink>
  )
  // Your own account connects in a dialog on this page, as an agent's does.
  // Mounted while it is connected too, so an open dialog sees the account land.
  const connection = !canOpen ? null : shared ? (
    (link ?? (provider.connection === 'not_connected' && !reconnect ? null : manage))
  ) : (
    <ConnectAccountDialog
      providerName={provider.displayName}
      namespace={provider.namespace}
      form={form}
      connectable={verb !== null}
      reconnect={verb === 'Reconnect'}
      ready={connected}
      fallback={provider.connection === 'not_connected' && !reconnect ? null : manage}
      trigger={{ label: `${verb ?? 'Connect'} ${whose}`, variant: 'primary', size: '32' }}
      // Your account page starts it, and its `returnTo` brings sign-in back here.
      action={accountsPath}
      description={audience.onlyYou}
    />
  )
  return (
    <section aria-label="Account">
      <Card
        role="region"
        aria-label={card.title}
        title={card.title}
        description={card.description}
        headerPill={
          required && provider.connection !== 'unknown'
            ? accountStates[provider.connection]
            : undefined
        }
      >
        {provider.accountSettingIncompatible && (
          <Banner variant="warning">
            The sign-in method cannot use this account setting. Change the account setting.
          </Banner>
        )}
        {provider.connection === 'unknown' && (
          <Banner
            variant="warning"
            title={`Cannot read ${shared ? 'the shared account' : 'your account'}`}
          >
            Open it to see its state.
          </Banner>
        )}
        {shared && !owner && !connected && (
          <Banner variant="info">Ask a workspace owner to connect the shared account.</Banner>
        )}
        {(connection || canChange) && (
          <div className={styles.rowActions}>
            {connection}
            {canChange && (
              <UpdateEntry
                provider={provider}
                form={form}
                task="accounts"
                label="Change account setting"
                pending={pending}
              />
            )}
          </div>
        )}
      </Card>
    </section>
  )
}

function SourcePanel({
  provider,
  source,
  canChange,
}: {
  provider: ProviderSettingsView
  source: ProviderSourceView
  canChange: boolean
}) {
  return (
    <SettingsAside title="Source">
      {source.state === 'failed' ? (
        <Banner variant="error">
          Coral could not read the source. Reload the page to try again.
        </Banner>
      ) : (
        <SettingsFacts>
          <SettingsFact label="Type">{sourceKinds[source.kind]}</SettingsFact>
          <SettingsFact label="URL">
            {source.location.type === 'url' ? (
              <SourceUrl url={source.location.url} host={source.location.host} />
            ) : source.location.type === 'upload' ? (
              'Uploaded document'
            ) : (
              'Coral cannot show this URL.'
            )}
          </SettingsFact>
        </SettingsFacts>
      )}
      {canChange && (
        <div className={styles.settingsSourceActions}>
          <Button.InternalLink to={`${provider.path}/update`} variant="secondary" size="small">
            Check for updates
          </Button.InternalLink>
          <Button.InternalLink
            to={`${provider.path}/update?replaceSource=1`}
            variant="secondary"
            size="small"
          >
            Replace source
          </Button.InternalLink>
        </div>
      )}
    </SettingsAside>
  )
}

function Removal({ provider, form }: { provider: ProviderSettingsView; form: FormContext }) {
  return (
    <SettingsRemoval
      title="Remove provider"
      description="Remove this provider and its connected accounts."
      action={
        <ActionDialog
          title="Remove provider"
          submit="Remove provider"
          form={form}
          intent="removeProvider"
          etag={provider.etag}
          destructive
        >
          <input type="hidden" name="provider" value={provider.id} />
          <div className={styles.removalDetails}>
            <Typography.Body as="p">This removes:</Typography.Body>
            <ul className={styles.bulletList}>
              <Typography.Body as="li">{provider.displayName}</Typography.Body>
              <Typography.Body as="li">
                Every operation under{' '}
                <Typography.CodeInline>{provider.callRoot}</Typography.CodeInline>
              </Typography.Body>
              <Typography.Body as="li">Every connected account</Typography.Body>
            </ul>
            <Typography.Body as="p">
              Calls to this provider fail after you remove it. Its run history stays.
            </Typography.Body>
            <Typography.Body as="p">You cannot undo this.</Typography.Body>
          </div>
        </ActionDialog>
      }
    />
  )
}

/** Opening an editor prepares a draft through an action, never a mutating loader. */
function UpdateEntry({
  provider,
  form,
  task,
  label,
  pending,
}: {
  provider: ProviderSettingsView
  form: FormContext
  task: 'configuration' | 'accounts'
  label: string
  pending: boolean
}) {
  return (
    <Form method="post" action={`${provider.path}/update`}>
      <Fields form={form} intent="prepare" />
      <input type="hidden" name="source" value="current" />
      <input type="hidden" name="etag" value={provider.etag} />
      <input type="hidden" name="task" value={task} />
      <Button.TextButton type="submit" variant="secondary" size="32" disabled={pending}>
        {label}
      </Button.TextButton>
    </Form>
  )
}
