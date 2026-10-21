import { useId, useState } from 'react'
import { Banner, Button, Card, Inputs, Typography } from '@/wax/components'
import { Disclosure } from '@/wax/components/disclosure'
import type {
  ConfigurationReviewView,
  ConfigurationValues,
  CredentialMethod,
  SetupView,
} from './types'
import * as styles from './providers.css'
import { filterQuery } from '@/utils/filter-query'

/** The one label each credential method has, wherever a method is named. */
export const authenticationLabels: Record<CredentialMethod | '', string> = {
  '': 'Choose authentication',
  none: 'No authentication',
  bearer: 'Bearer token',
  header: 'API keys in headers',
  basic: 'Username and password',
  query: 'API keys in the query',
  device: 'OAuth device code',
  oauth: 'OAuth authorization code',
}

export function countLabel(count: number, noun: string) {
  return `${count.toLocaleString('en-US')} ${noun}${count === 1 ? '' : 's'}`
}

export interface AuthenticationOption {
  value: CredentialMethod
  label: string
  description: string
}

/**
 * The choices offered for authentication. Today one per supported method; a
 * recipe would supply named options that resolve to these same methods.
 */
export function authenticationOptions(review: ConfigurationReviewView): AuthenticationOption[] {
  const descriptions: Record<CredentialMethod, string> = {
    none: 'No credentials. Only public operations work.',
    bearer: 'A bearer token, sent in the Authorization header.',
    header: 'API keys, each sent in a named header.',
    basic: 'A username and password, sent as HTTP Basic.',
    query: 'API keys, each sent as a query parameter.',
    device: 'Enter a code to sign in with the service.',
    oauth: 'Sign in with the service.',
  }
  return review.authenticationMethods.map((value) => ({
    value,
    label: authenticationLabels[value],
    description: descriptions[value],
  }))
}

/**
 * The method the document or server declares, or null when it declares none
 * usable. A recorded difference wins because confirmed values replace the
 * document's in `values`.
 */
export function declaredAuthentication(
  review: ConfigurationReviewView,
): ConfigurationValues['authentication'] | null {
  const difference = review.differences.find((item) => item.field === 'authentication.method')
  if (difference?.document !== undefined) {
    const normalized: Record<string, ConfigurationValues['authentication']> = {
      none: 'none',
      bearer: 'bearer',
      header: 'header',
      header_credentials: 'header',
      basic: 'basic',
      basic_auth: 'basic',
      query: 'query',
      query_credentials: 'query',
      device: 'device',
      oauth_device_code: 'device',
      oauth: 'oauth',
      oauth_authorization_code: 'oauth',
    }
    return normalized[difference.document] ?? null
  }
  switch (review.authenticationStatus) {
    case 'explicit_anonymous':
    case 'detected_anonymous':
      return 'none'
    case 'detected_bearer':
      return 'bearer'
    case 'detected_oauth':
      return 'oauth'
    case 'supported':
      return review.values.authentication || null
    default:
      return null
  }
}

/** Presentation only: preserve the domain's complete resolved URL, including its base path. */
export function destinationGroups(setup: SetupView) {
  const groups = new Map<string, string[]>()
  for (const [operation, url] of Object.entries(setup.configuration?.destinations ?? {})) {
    const operations = groups.get(url) ?? []
    operations.push(operation)
    groups.set(url, operations)
  }
  return groups
}

/** A few words for the rail; the Review row carries the full summary. */
export function configurationShortSummary(setup: SetupView) {
  const review = setup.configuration
  if (!review?.confirmed) return undefined
  return authenticationLabels[review.values.authentication]
}

export function configurationSummary(setup: SetupView) {
  const review = setup.configuration
  if (!review?.confirmed) return undefined
  const destinations = destinationGroups(setup).size
  const servers = destinations || (setup.kind === 'mcp' ? 1 : review.values.servers.length)
  const declared = declaredAuthentication(review)
  const override = declared !== null && declared !== review.values.authentication
  return `${authenticationLabels[review.values.authentication]} · ${countLabel(servers, 'server')}${
    override ? ` · changed from the ${setup.kind === 'mcp' ? 'server' : 'document'}` : ''
  }`
}

/**
 * How Coral signs in, without the server count: Review lists the destinations
 * themselves beside the operations that use them, so a count only repeats them.
 */
export function signInSummary(setup: SetupView) {
  const review = setup.configuration
  if (!review?.confirmed) return undefined
  const declared = declaredAuthentication(review)
  const override = declared !== null && declared !== review.values.authentication
  return `${authenticationLabels[review.values.authentication]}${
    override ? ` · changed from the ${setup.kind === 'mcp' ? 'server' : 'document'}` : ''
  }`
}

/** Coral is still to read the operations, so no count can be trusted yet. */
export function awaitingDiscovery(setup: SetupView) {
  const review = setup.configuration
  return Boolean(
    review &&
    setup.kind === 'mcp' &&
    review.discoveryAuthorizationRequired &&
    (setup.state === 'awaiting_configuration' ||
      setup.state === 'awaiting_authorization' ||
      setup.state === 'awaiting_discovery'),
  )
}

/**
 * Prefer the exact catalog count. Before a preview is available, OpenAPI
 * declarations can still describe operations; absent MCP keys prove no count.
 */
export function operationCount(setup: SetupView) {
  const review = setup.configuration
  if (!review) return null
  if (review.operationCount !== null) return review.operationCount
  const counted = new Set([
    ...review.operations.map(({ key }) => key),
    ...Object.keys(review.destinations),
    ...review.unavailableOperations.map(({ key }) => key),
    ...review.servers.flatMap((server) => server.operations),
  ]).size
  return setup.kind === 'mcp' && counted === 0 ? null : counted
}

/**
 * What the document or server says about signing in: the method's own label
 * when it names one, otherwise why it does not.
 */
export function declaredSignInLabel(review: ConfigurationReviewView) {
  const declared = declaredAuthentication(review)
  const unnamed: Record<string, string> = {
    missing: 'Not specified',
    metadata_missing: 'Not specified',
    unused: 'Listed, none chosen',
    unsupported: 'Not usable by Coral',
    ambiguous: 'Mixed',
    supported: 'Specified',
  }
  return declared
    ? authenticationLabels[declared]
    : (unnamed[review.authenticationStatus] ?? 'Unclear')
}

/** Safe discovery facts, including operations that cannot yet run. Missing previews are not empty catalogs. */
export function DiscoverySummary({ setup }: { setup: SetupView }) {
  const review = setup.configuration
  if (!review) return null
  const waiting = awaitingDiscovery(setup)
  const operations = operationCount(setup)
  const signIn = declaredSignInLabel(review)
  return (
    <>
      <Card.Container>
        <div className={styles.stack}>
          <ConfigurationFact
            label={setup.kind === 'mcp' ? 'MCP server URL' : 'Source'}
            value={setup.sourceLabel}
          />
          <div className={styles.facts}>
            <ConfigurationFact
              label="Operations"
              value={
                waiting
                  ? 'Sign in to list operations'
                  : (operations?.toLocaleString('en-US') ?? 'Not counted yet')
              }
            />
            <ConfigurationFact
              label="Servers"
              value={countLabel(setup.kind === 'mcp' ? 1 : review.servers.length, 'server')}
            />
            <ConfigurationFact label="Sign-in" value={signIn} />
          </div>
        </div>
      </Card.Container>
      {!waiting && operations === 0 && (
        <Banner title="No operations found">
          Check the document or server, or change the source.
        </Banner>
      )}
    </>
  )
}

/** Read-only facts are text, not controls that look editable. */
export function ConfigurationFact({ label, value }: { label: string; value: string }) {
  return (
    <div className={styles.choices} role="group" aria-label={label}>
      <Typography.BodySmallStrong as="p">{label}</Typography.BodySmallStrong>
      <Typography.BodySmall as="p" className={styles.longValue}>
        {value || 'None'}
      </Typography.BodySmall>
    </div>
  )
}

/** A bounded, searchable list shared by declaration, destination, and unavailable previews. */
export function OperationDetails({
  operations,
  title = 'View operations',
  inline = false,
}: {
  operations: { key: string; reasons?: string[] }[]
  title?: string
  /** Under one line of text, without a rule of its own. */
  inline?: boolean
}) {
  const [query, setQuery] = useState('')
  const [page, setPage] = useState(0)
  const id = useId()
  const search = filterQuery(query)
  const matches = operations.filter((operation) =>
    search.matches(operation.key, ...(operation.reasons ?? [])),
  )
  const pageSize = 50
  const currentPage = Math.min(page, Math.max(0, Math.ceil(matches.length / pageSize) - 1))
  const start = currentPage * pageSize
  const shown = matches.slice(start, start + pageSize)
  return (
    <Disclosure title={title} variant={inline ? 'inline' : 'default'}>
      <div className={styles.field}>
        <Typography.BodySmallStrong as="label" htmlFor={id}>
          Find an operation
        </Typography.BodySmallStrong>
        <Inputs.TextInput
          id={id}
          type="search"
          value={query}
          onChange={(value) => {
            setQuery(value)
            setPage(0)
          }}
          onKeyDown={(event) => {
            // These details can be inside the configuration or publication form.
            if (event.key === 'Enter') event.preventDefault()
          }}
        />
      </div>
      <Typography.BodySmall as="p" role="status">
        {matches.length
          ? `Showing ${start + 1}–${start + shown.length} of ${countLabel(matches.length, 'operation')}.`
          : 'No operations match your search.'}
      </Typography.BodySmall>
      <ul className={styles.list}>
        {shown.map(({ key, reasons }) => (
          <Typography.BodySmall as="li" key={key} className={styles.longValue}>
            {key}
            {reasons?.length ? `: ${reasons.join(' ')}` : ''}
          </Typography.BodySmall>
        ))}
      </ul>
      {matches.length > pageSize && (
        <div className={styles.rowActions}>
          <Button.TextButton
            type="button"
            variant="secondary"
            disabled={currentPage === 0}
            onClick={() => setPage(currentPage - 1)}
          >
            Previous operations
          </Button.TextButton>
          <Button.TextButton
            type="button"
            variant="secondary"
            disabled={start + pageSize >= matches.length}
            onClick={() => setPage(currentPage + 1)}
          >
            Next operations
          </Button.TextButton>
        </div>
      )}
    </Disclosure>
  )
}

export function serverLabel(declaration: string) {
  if (declaration === '#/servers') return 'Main server'
  const parts = declaration.split('/').map((part) => part.replace(/~1/g, '/').replace(/~0/g, '~'))
  if (parts[1] === 'paths' && parts[2]) {
    const method = parts.length === 5 ? `${parts[3].toUpperCase()} ` : ''
    return `Server for ${method}${parts[2]}`
  }
  return 'Operation-specific server'
}

/** Every resolved destination with the operations that use it. */
export function DestinationList({
  setup,
  size = 'small',
}: {
  setup: SetupView
  /** Body matches a Review value; small suits a preview under a heading. */
  size?: 'body' | 'small'
}) {
  const groups = destinationGroups(setup)
  if (groups.size === 0) return null
  const Line = size === 'body' ? Typography.Body : Typography.BodySmall
  return (
    <div className={styles.destinationList}>
      {Array.from(groups, ([url, operations]) => (
        <section key={url} className={styles.destination} aria-label={url}>
          <Line as="p" className={styles.destinationLine}>
            <span className={styles.longValue}>{url}</span>
            <span>Used by {countLabel(operations.length, 'operation')}.</span>
          </Line>
          <OperationDetails operations={operations.map((key) => ({ key }))} inline />
        </section>
      ))}
    </div>
  )
}
