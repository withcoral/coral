import { useEffect, useId, useRef, useState } from 'react'
import { Form, useFetcher } from 'react-router'

import { Banner, Button, Inputs, Typography } from '@/wax/components'
import { ErrorMessage } from '@/wax/components/error-message/error-message'
import { PageLayout } from '@/wax/components/page-layout'
import { Card } from '@/wax/components/card/card'
import { Pill } from '@/wax/components/pill'
import { ActionDialog, Fields } from '@/views/management/forms'
import { ArchivedBanner, trail } from '@/views/management/layout'
import {
  SettingsAside,
  SettingsFact,
  SettingsFacts,
  SettingsLayout,
  SettingsRemoval,
} from '@/wax/components/settings-layout'

import type {
  ActionResult,
  FormContext,
  ProviderSettingsPageProps,
  ProviderSettingsView,
  ProviderSourceView,
  CredentialFieldView,
} from './types'
import { accountStates } from '@/views/accounts/account-states'
import { ConnectButton } from '@/views/accounts/connect-button'
import { CredentialFields } from '@/views/management/credential-fields'
import * as Dialog from '@/wax/components/dialog'
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
  const canChange = owner && active
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

/**
 * The provider name heads the settings under the breadcrumb, which titles the
 * page. A pencil beside it swaps the text for
 * an input: Enter or Save saves, Escape or Cancel discards, and leaving the
 * field does neither. A failed save keeps the draft beside its error.
 */
function ProviderNameHeading({
  provider,
  form,
  canRename,
}: {
  provider: ProviderSettingsView
  form: FormContext
  canRename: boolean
}) {
  const fetcher = useFetcher<ActionResult>()
  const [open, setOpen] = useState(false)
  // The result the last edit started from: anything newer belongs to this edit.
  const [earlier, setEarlier] = useState<ActionResult | undefined>(undefined)
  const [draft, setDraft] = useState(provider.displayName)
  const pencil = useRef<HTMLButtonElement>(null)
  const formRef = useRef<HTMLFormElement>(null)
  const saving = fetcher.state !== 'idle'
  const result = !saving && fetcher.data !== earlier ? fetcher.data : undefined
  // A saved name closes the editor; a refused one keeps the draft open.
  const editing = open && !(result && !result.error)
  // Closing the editor, by saving or cancelling, returns focus to the pencil.
  const wasEditing = useRef(false)
  useEffect(() => {
    if (wasEditing.current && !editing) pencil.current?.focus()
    wasEditing.current = editing
  }, [editing])
  const cancel = () => setOpen(false)
  if (!editing)
    return (
      <div className={styles.nameHeading}>
        <Typography.HeadingLarge as="h2">{provider.displayName}</Typography.HeadingLarge>
        {canRename && (
          <Button.IconButton
            ref={pencil}
            name="Pencil"
            variant="bare"
            size="32"
            tooltipText="Edit provider name"
            onClick={() => {
              setDraft(provider.displayName)
              setEarlier(fetcher.data)
              setOpen(true)
            }}
          />
        )}
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
      </div>
    )
  const error = result?.error
  return (
    <fetcher.Form method="post" ref={formRef} className={styles.nameForm} aria-busy={saving}>
      <Fields form={form} intent="renameProvider" etag={provider.etag} />
      <input type="hidden" name="provider" value={provider.id} />
      <div className={styles.nameRow}>
        <div className={styles.nameInput}>
          <Inputs.TextInput
            ariaLabel="Provider name"
            name="displayName"
            value={draft}
            onChange={setDraft}
            autoFocus
            required
            maxLength={256}
            invalid={Boolean(error)}
            onKeyDown={(event) => {
              if (event.key === 'Escape') {
                event.preventDefault()
                cancel()
              } else if (event.key === 'Enter') {
                event.preventDefault()
                if (!saving) formRef.current?.requestSubmit()
              }
            }}
          />
        </div>
        <Button.TextButton type="submit" size="32" disabled={saving}>
          {saving ? 'Saving…' : 'Save'}
        </Button.TextButton>
        <Button.TextButton
          type="button"
          variant="secondary"
          size="32"
          disabled={saving}
          onClick={cancel}
        >
          Cancel
        </Button.TextButton>
      </div>
      {error && <ErrorMessage message={error} />}
    </fetcher.Form>
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
  pending,
}: {
  workspacePath: string
  provider: ProviderSettingsView
  form: FormContext
  owner: boolean
  active: boolean
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
  // Your own account connects here: a sign-in in one click, or a secret in a
  // dialog. Both come back to these settings. The shared account opens its page.
  const connect = (verb: 'Connect' | 'Reconnect') => {
    const label = `${verb} ${whose}`
    if (shared || !provider.yourAccount)
      return (
        <Button.InternalLink to={accountsPath} variant="primary" size="small">
          {label}
        </Button.InternalLink>
      )
    if (provider.yourAccount.oauth)
      return (
        <ConnectButton
          action={accountsPath}
          csrf={form.csrf}
          requestId={form.requestId}
          label={label}
          disabled={pending}
          size="32"
        />
      )
    return (
      <SecretConnect
        label={label}
        action={accountsPath}
        providerName={provider.displayName}
        fields={provider.yourAccount.credentialFields}
        form={form}
        pending={pending}
      />
    )
  }
  const connection = !canOpen
    ? null
    : provider.connection === 'connected' || provider.connection === 'unknown'
      ? manage
      : reconnect && provider.canReconnect
        ? connect('Reconnect')
        : reconnect
          ? // Coral refuses this reader a reconnect; the account can still be opened.
            manage
          : active && provider.canConnect
            ? connect('Connect')
            : null
  const canChange = owner && active
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
          <Typography.BodySmall as="p">
            {`Coral could not read ${whose}. Open it to see its state.`}
          </Typography.BodySmall>
        )}
        {shared && !owner && !connected && (
          <Typography.BodySmall as="p">
            Ask a workspace owner to connect the shared account.
          </Typography.BodySmall>
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

/**
 * Connects your own account with a secret, as Add provider does: the fields in
 * a dialog. Your account page takes the entry and brings you back here.
 */
function SecretConnect({
  label,
  action,
  providerName,
  fields,
  form,
  pending,
}: {
  label: string
  action: string
  providerName: string
  fields: CredentialFieldView[]
  form: FormContext
  pending: boolean
}) {
  const formId = useId()
  const [open, setOpen] = useState(false)
  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger render={<Button.Container variant="primary" size="32" />}>
        <Button.Text>{label}</Button.Text>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup>
          <Dialog.Title>{label}</Dialog.Title>
          <Dialog.Description>
            {`Enter the credentials from ${providerName}. Coral does not show them again.`}
          </Dialog.Description>
          {/* A full page post: your account page answers it and returns here. */}
          <form id={formId} method="post" action={action} className={styles.stack}>
            <Fields form={form} intent="secret" />
            <CredentialFields fields={fields}>
              {(input) => <Inputs.CredentialInput {...input} />}
            </CredentialFields>
          </form>
          <Dialog.Actions>
            <Button.TextButton type="button" variant="secondary" onClick={() => setOpen(false)}>
              Cancel
            </Button.TextButton>
            <Button.TextButton type="submit" form={formId} disabled={pending}>
              Connect
            </Button.TextButton>
          </Dialog.Actions>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
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
                The namespace{' '}
                <Typography.CodeInline>coral.providers.{provider.namespace}</Typography.CodeInline>
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
