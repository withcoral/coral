import { useId } from 'react'
import { Button, Table, Typography } from '@/wax/components'
import { Logo } from '@/wax/components/logo'
import { StatusDot } from '@/wax/components/status'
import type { ProviderLogo } from '@/views/providers/types'
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
  icons?: ProviderLogo
}

const ACCOUNT_KIND = { none: 'None', shared: 'Shared', separate: 'Individual' } as const

const SORT_COLUMNS = {
  provider: Table.byText((row: AgentProviderView) => row.name),
  account: Table.byText((row: AgentProviderView) => ACCOUNT_KIND[row.account.kind]),
  // The providers that need you come first, then each status by the label you read.
  status: {
    compare: (a: AgentProviderView, b: AgentProviderView) =>
      Number(needsYou(b)) - Number(needsYou(a)) ||
      Table.compareText(statusLabel(a), statusLabel(b)),
    first: 'ascending',
  },
} satisfies Record<string, Table.SortColumn<AgentProviderView>>

/** The accounts this agent uses, one row per provider. By default, those that need you come first. */
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
  // Providers with equal values stay in name order, whichever way the column runs.
  const sort = Table.useSort({
    columns: SORT_COLUMNS,
    initial: 'status',
    tiebreak: SORT_COLUMNS.provider.compare,
  })
  const rows = sort.rows(providers ?? [])
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
          { width: { fill: 2 } },
          { width: 'fill' },
          { width: { fill: 1.5 } },
          { width: 112, align: 'right' },
        ]}
      >
        <Table.Head>
          <Table.Row>
            <Table.SortHeading sort={sort} column="provider">
              Provider
            </Table.SortHeading>
            <Table.SortHeading sort={sort} column="account">
              Account
            </Table.SortHeading>
            <Table.SortHeading sort={sort} column="status">
              Status
            </Table.SortHeading>
            <Table.Heading ariaLabel="Actions" />
          </Table.Row>
        </Table.Head>
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
 * What the status cell reads, and the blockers it lists below that. An agent account that
 * is not ready is the one blocker Coral then reports, and its state says more. Any other
 * blocker comes first, since connecting won't help.
 */
function status(provider: AgentProviderView) {
  const action = accountAction(provider.account)
  const accountOnly = action !== null && provider.blockers.length <= 1
  const [first, ...rest] = accountOnly ? [] : provider.blockers
  return { action, label: first ?? action ?? 'Ready', rest }
}

function statusLabel(provider: AgentProviderView) {
  return status(provider).label
}

/**
 * The first blocker, or what the agent's account needs. The other blockers show
 * below it, so nobody clears one blocker expecting access.
 */
function ProviderStatus({ provider }: { provider: AgentProviderView }) {
  const { action, label, rest } = status(provider)
  return (
    <span className={styles.cellStack}>
      <span className={styles.cellInline}>
        <StatusDot blockers={provider.blockers} personalAccountAction={action} />
        <Typography.BodySmall as="span" className={styles.cellInlineText}>
          {label}
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
