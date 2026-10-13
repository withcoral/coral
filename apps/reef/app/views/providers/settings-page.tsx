import { useEffect, useRef, useState } from 'react'
import { Form, Link, useFetcher } from 'react-router'

import { Banner, Button, Inputs, Typography } from '@/wax/components'
import { ErrorMessage } from '@/wax/components/error-message/error-message'
import { PageLayout } from '@/wax/components/page-layout'
import * as CardFrame from '@/wax/components/card'
import { Card } from '@/wax/components/card/card'
import { Tooltip } from '@/wax/components/tooltip'
import { Pill } from '@/wax/components/pill'
import { ActionDialog, Fields } from '@/views/management/forms'
import { ArchivedBanner, trail } from '@/views/management/layout'
import { SettingsLayout, SettingsRemoval } from '@/wax/components/settings-layout'

import type {
  ActionResult,
  FormContext,
  ProviderSettingsPageProps,
  ProviderSettingsView,
  ProviderSourceView,
} from './types'
import { accountStates } from '@/views/accounts/account-states'
import { countLabel } from './configuration-details'
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
        <OperationsSummary operations={provider.operations} path={provider.path} />
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

const formatCount = (count: number) => count.toLocaleString('en-US')

/** How many operations the provider offers, what they can do, and how many are blocked. */
function OperationsSummary({
  operations: o,
  path,
}: {
  operations: ProviderSettingsView['operations']
  /** The provider's operations view, which the card opens. */
  path: string
}) {
  // Only what the catalog has: a count of nothing says nothing.
  const parts = [
    o.groups > 0 && countLabel(o.groups, 'group'),
    o.readOnly > 0 && `${formatCount(o.readOnly)} read-only`,
    o.destructive > 0 && `${formatCount(o.destructive)} destructive`,
    o.blocked > 0 && `${formatCount(o.blocked)} blocked`,
  ].filter(Boolean)
  return (
    <Card
      as={Link}
      to={path}
      title={countLabel(o.total, 'operation')}
      description={parts.join(' · ') || undefined}
    />
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

/** Whose account calls use, as the Add provider account step names it. */
const accountCards = {
  caller: { title: 'Your account', description: 'Each caller uses their own account.' },
  fixed: { title: 'Shared account', description: 'All callers use one shared account.' },
  none: { title: 'No account is required.', description: undefined },
} as const

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
  const card = accountCards[provider.account]
  const required = provider.account !== 'none'
  const shared = provider.account === 'fixed'
  const connected = provider.connection === 'connected'
  // Your own account has its page under Accounts; owners manage the shared one here.
  const accountsPath = shared
    ? `${provider.path}/accounts?scope=shared`
    : `${workspacePath}/accounts/${provider.id}`
  const whose = shared ? 'shared account' : 'your account'
  // Members cannot open the shared account; only its owners connect it.
  const canOpen = required && (!shared || owner)
  const reconnect = provider.connection === 'needs_reconnect' && active
  const manage = (
    <Button.InternalLink to={accountsPath} variant="secondary" size="small">
      {`Manage ${whose}`}
    </Button.InternalLink>
  )
  const connection = !canOpen ? null : provider.connection === 'connected' ||
    provider.connection === 'unknown' ? (
    manage
  ) : reconnect && provider.canReconnect ? (
    <Button.InternalLink to={accountsPath} variant="primary" size="small">
      {`Reconnect ${whose}`}
    </Button.InternalLink>
  ) : reconnect ? (
    // Coral refuses this reader a reconnect; the account can still be opened.
    manage
  ) : active && provider.canConnect ? (
    <Button.InternalLink to={accountsPath} variant="primary" size="small">
      {`Connect ${whose}`}
    </Button.InternalLink>
  ) : null
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

const sourceKinds = { openapi: 'OpenAPI document', mcp: 'MCP server' } as const

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
    <CardFrame.Container as="aside" className={styles.settingsSource} aria-label="Source">
      <Typography.BodyLargeStrong as="h2">Source</Typography.BodyLargeStrong>
      {source.state === 'failed' ? (
        <Banner variant="error">
          Coral could not read the source. Reload the page to try again.
        </Banner>
      ) : (
        <dl className={styles.settingsFacts}>
          <div className={styles.settingsFact}>
            <Typography.BodySmallStrong as="dt">Type</Typography.BodySmallStrong>
            <Typography.BodySmall as="dd">{sourceKinds[source.kind]}</Typography.BodySmall>
          </div>
          <div className={styles.settingsFact}>
            <Typography.BodySmallStrong as="dt">URL</Typography.BodySmallStrong>
            {source.location.type === 'url' ? (
              <SourceUrl url={source.location.url} host={source.location.host} />
            ) : (
              <Typography.BodySmall as="dd">
                {source.location.type === 'upload'
                  ? 'Uploaded document'
                  : 'Coral cannot show this URL.'}
              </Typography.BodySmall>
            )}
          </div>
        </dl>
      )}
      {canChange && (
        <div className={styles.settingsSourceActions}>
          <div className={styles.settingsFact}>
            <Typography.BodySmall as="p">
              Read the source again to update operations.
            </Typography.BodySmall>
            <div className={styles.rowActions}>
              <Button.InternalLink to={`${provider.path}/update`} variant="secondary" size="small">
                Refresh operations
              </Button.InternalLink>
            </div>
          </div>
          <div className={styles.settingsFact}>
            <Typography.BodySmall as="p">
              Use a different source for this provider.
            </Typography.BodySmall>
            <div className={styles.rowActions}>
              <Button.InternalLink
                to={`${provider.path}/update?replaceSource=1`}
                variant="secondary"
                size="small"
              >
                Replace source
              </Button.InternalLink>
            </div>
          </div>
        </div>
      )}
    </CardFrame.Container>
  )
}

/**
 * The host reads at a glance; the whole URL stays on one line, shortened when
 * it does not fit, with its full text in a tooltip and a copy button.
 */
function SourceUrl({ url, host }: { url: string; host: string }) {
  return (
    <dd className={styles.settingsUrl}>
      <Typography.BodySmall as="span">{host}</Typography.BodySmall>
      <span className={styles.settingsUrlRow}>
        <Tooltip content={url} showOnlyWhenTruncated maxWidth={520}>
          <span className={styles.settingsUrlText}>
            <Typography.BodySmall as="span" variant="tertiary">
              {url}
            </Typography.BodySmall>
          </span>
        </Tooltip>
        <Button.CopyButton textToCopy={url} ariaLabel="Copy source URL" size="22" variant="bare" />
      </span>
    </dd>
  )
}

function Removal({ provider, form }: { provider: ProviderSettingsView; form: FormContext }) {
  return (
    <SettingsRemoval
      title="Remove provider"
      description="Remove this provider and its connected accounts. You cannot undo this."
      action={
        <ActionDialog
          title="Remove provider"
          submit="Remove provider"
          form={form}
          intent="removeProvider"
          etag={provider.etag}
          destructive
          description={`Coral removes ${provider.displayName}, its namespace ${provider.callRoot}, and every connected account. Unsent calls stop. You cannot undo this. Coral keeps the run history.`}
        >
          <input type="hidden" name="provider" value={provider.id} />
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
