import { useId } from 'react'
import { Button, Table, Typography } from '@/wax/components'
import { Logo } from '@/wax/components/logo'
import { StatusDot } from '@/wax/components/status'
import type { RecipeIcons } from '@/lib/provider-recipes/choice'
import type { AccountView } from '@/views/accounts/types'
import type { FormContext } from './types'
import { ConnectAccountDialog } from './connect-account-dialog'
import * as styles from './management.css'

export interface AgentProviderView {
  provider: string
  name: string
  namespace: string
  account:
    | { kind: 'none' }
    | { kind: 'shared'; path: string }
    | { kind: 'separate'; path: string; state: AccountView['state']; canConnect: boolean }
  blockers: string[]
  icons?: RecipeIcons
}

const ACCOUNT_KIND = { none: 'None', shared: 'Shared', separate: 'Individual' } as const

/** The accounts this agent uses, one row per provider, those that need you first. */
export function AgentProviders({
  providers,
  form,
  canConnect,
}: {
  /** Null when Coral could not read them; the table then says so. */
  providers: AgentProviderView[] | null
  form: FormContext
  /** False in an archived workspace, where the table only links to each account. */
  canConnect: boolean
}) {
  const heading = useId()
  // A stable sort keeps the server's order within each half.
  const rows = (providers ?? []).toSorted((a, b) => Number(needsYou(b)) - Number(needsYou(a)))
  return (
    <section className={styles.agentAccounts} aria-labelledby={heading}>
      <div className={styles.intro}>
        <Typography.HeadingSmall as="h2" id={heading}>
          Accounts
        </Typography.HeadingSmall>
      </div>
      <Table.Container
        ariaLabel="Provider accounts"
        layout="fixed"
        // A long list scrolls instead of stretching the dashboard row.
        bounded
        staticRows
        variant="card"
        columns={[
          { label: 'Provider', width: { fill: 2 } },
          { label: 'Account', width: 'fill' },
          { label: 'Status', width: { fill: 1.5 } },
          { ariaLabel: 'Actions', width: 112, align: 'right' },
        ]}
      >
        <Table.Head />
        <Table.Body>
          {providers === null ? (
            <Table.Status>
              Coral could not load the accounts. Reload the page to try again.
            </Table.Status>
          ) : (
            rows.length === 0 && <Table.Status>This workspace has no providers yet.</Table.Status>
          )}
          {rows.map((provider) => (
            <Table.Row key={provider.provider}>
              <Table.Cell>
                <span className={styles.cellInline}>
                  <Logo name={provider.name} icons={provider.icons} size="20" />
                  <span className={styles.cellInlineText}>{provider.name}</span>
                </span>
              </Table.Cell>
              <Table.Cell>{ACCOUNT_KIND[provider.account.kind]}</Table.Cell>
              <Table.Cell>
                <ProviderStatus provider={provider} />
              </Table.Cell>
              {/* Not an `actions` cell: what the agent needs stays in view, not only on hover. */}
              <Table.Cell align="right">
                <span className={styles.cellAction}>
                  <ProviderAction provider={provider} form={form} canConnect={canConnect} />
                </span>
              </Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Container>
    </section>
  )
}

/** What you must do with the agent's own account, or null when nothing. */
function accountAction(account: AgentProviderView['account']) {
  if (account.kind !== 'separate' || account.state === 'ready') return null
  return account.state === 'reconnect' ? 'Reconnect' : 'Not connected'
}

function needsYou(provider: AgentProviderView) {
  return provider.blockers.length > 0 || accountAction(provider.account) !== null
}

/**
 * The first blocker, or what the agent's account needs. The other blockers show
 * below it, so nobody clears one blocker expecting access.
 */
function ProviderStatus({ provider }: { provider: AgentProviderView }) {
  const action = accountAction(provider.account)
  // An agent account that is not ready is the one blocker Coral then reports, and
  // its state says more. Any other blocker comes first, since connecting won't help.
  const accountOnly = action !== null && provider.blockers.length <= 1
  const [first, ...rest] = accountOnly ? [] : provider.blockers
  return (
    <span className={styles.cellStack}>
      <span className={styles.cellInline}>
        <StatusDot blockers={provider.blockers} personalAccountAction={action} />
        <Typography.BodySmall as="span" className={styles.cellInlineText}>
          {first ?? action ?? 'Ready'}
        </Typography.BodySmall>
      </span>
      {rest.length > 0 && (
        <Typography.BodySmall as="span" variant="tertiary">
          Also blocked: {rest.join('; ')}
        </Typography.BodySmall>
      )}
    </span>
  )
}

function ProviderAction({
  provider,
  form,
  canConnect,
}: {
  provider: AgentProviderView
  form: FormContext
  canConnect: boolean
}) {
  const account = provider.account
  if (account.kind === 'none') return null
  const link = (
    <Button.IconLink
      name="ChevronRight"
      to={account.path}
      tooltipText={`${account.kind === 'shared' ? 'Inspect shared account' : 'Manage account'} for ${provider.namespace}`}
    />
  )
  if (account.kind === 'shared' || !canConnect) return link
  // Mounted while the account is ready too, so an open dialog sees the connection land.
  return (
    <ConnectAccountDialog
      provider={provider.provider}
      providerName={provider.name}
      namespace={provider.namespace}
      form={form}
      connectable={account.state !== 'ready' && account.canConnect}
      reconnect={account.state === 'reconnect'}
      ready={account.state === 'ready'}
      fallback={link}
    />
  )
}
