import classNames from 'classnames'
import { Link } from 'react-router'
import { useId } from 'react'
import { Banner, Button, List, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { CodeChip } from '@/wax/components/code-chip'
import * as DescriptionList from '@/wax/components/description-list'
import { Disclosure } from '@/wax/components/disclosure'
import { Markdown } from '@/wax/components/markdown'
import { Pill } from '@/wax/components/pill'
import type { OperationHintsView } from '@/views/providers/types'
import { policyOptions } from '@/components/policy-toggle'
import { Icon } from '@/wax/components/icon'
import { Tooltip } from '@/wax/components/tooltip'
import type {
  Description,
  DiscoveryContext,
  DiscoveryEntityKind,
  InteractionEvidence,
  OperationFlags,
  SearchHit,
  SearchPage,
} from '../types'
import { coverageLine, worstTone, type CoverageTone } from './coverage'
import * as styles from './discovery.css'

/** A declaration or a source stays open, but takes less than one screen. */
const DECLARATION_LINES = 20

/** One retained `search` or `describe` attempt, as the agent received it then. */
export function DiscoveryEvidence({
  evidence,
}: {
  evidence: Extract<InteractionEvidence, { kind: 'discovery' }>
}) {
  const { context, view, response } = evidence
  return (
    <div className={styles.stack}>
      {context.kind === 'describe' && (
        <dl className={styles.facts}>
          <Fact label="Call path">
            <CodeChip>{context.callPath}</CodeChip>
          </Fact>
          {context.types.length > 0 && (
            <Fact label="Types">
              {context.types.map((name) => (
                <CodeChip key={name}>{name}</CodeChip>
              ))}
            </Fact>
          )}
        </dl>
      )}
      {view.kind === 'search' && <SearchLedger page={view.page} />}
      {view.kind === 'describe' && <DescriptionDocument description={view.description} />}
      {view.kind === 'raw' &&
        (context.kind === 'unknown' ? (
          <Typography.Body>The request context is unavailable.</Typography.Body>
        ) : (
          <Banner variant="info" title="Stored in an earlier shape">
            Reef cannot show this response in the current layout. The stored response follows.
          </Banner>
        ))}
      {/* A search shows only its results; a response Reef cannot read shows as stored. */}
      {view.kind === 'raw' ? (
        <RawResponse response={response} />
      ) : (
        context.kind !== 'search' && (
          <Disclosure title="Raw response">
            <RawResponse response={response} />
          </Disclosure>
        )
      )}
    </div>
  )
}

function RawResponse({ response }: { response: string }) {
  return (
    <>
      <Typography.BodySmall>
        This is what the agent received then. Coral does not repeat the request.
      </Typography.BodySmall>
      <CodeBlock value={response} format="json" />
    </>
  )
}

/** What a search asked for, apart from the intent the agent gave for the call. */
export function SearchParameters({
  context,
}: {
  context: Extract<DiscoveryContext, { kind: 'search' }>
}) {
  return (
    <DescriptionList.List title="Search">
      <DescriptionList.Row label="Query">
        {context.query === null ? (
          'None. The agent listed without a query.'
        ) : (
          <Typography.BodySmallStrong>“{context.query}”</Typography.BodySmallStrong>
        )}
      </DescriptionList.Row>
      <DescriptionList.Row label="Provider">
        {context.provider === null ? 'all providers' : <CodeChip>{context.provider}</CodeChip>}
      </DescriptionList.Row>
      <DescriptionList.Row label="Page">
        {context.cursor === null ? 'first page' : 'a later page'}, up to {context.limit} results
      </DescriptionList.Row>
    </DescriptionList.List>
  )
}

const toneColors = { success: 'green', neutral: 'gray', warning: 'amber', error: 'red' } as const

/** A "+" on a count says more matched. A trimmed page lists no such count, so it says it once. */
function CoverageStrip({ page }: { page: SearchPage }) {
  const tone = worstTone(page.coverage)
  const counted = page.coverage.some(
    (coverage) => coverage.state === 'results_found' && coverage.hasMore,
  )
  return (
    <div
      className={classNames(styles.coverage, {
        [styles.coverageWarning]: tone === 'warning',
        [styles.coverageError]: tone === 'error',
      })}
      role="group"
      aria-label="Search coverage"
    >
      {page.coverage.map((coverage) => {
        const line = coverageLine(coverage)
        return <CoverageRow key={coverage.kind} {...line} />
      })}
      {page.hasMore && !counted && (
        <Typography.BodySmall variant="tertiary">
          More results were available after this page.
        </Typography.BodySmall>
      )}
    </div>
  )
}
function CoverageRow({
  subject,
  label,
  tone,
  detail,
}: {
  subject: string
  label: string
  tone: CoverageTone
  detail: string | null
}) {
  return (
    <>
      <Typography.BodySmallStrong>{subject}</Typography.BodySmallStrong>
      <Pill color={toneColors[tone]} size="small">
        {label}
      </Pill>
      <Typography.BodySmall variant="tertiary">{detail}</Typography.BodySmall>
    </>
  )
}

/** A trimmed page states coverage only for a kind whose results may be missing. */
function SearchLedger({ page }: { page: SearchPage }) {
  const id = useId()
  return (
    <section className={styles.stack} aria-labelledby={id}>
      <Typography.BodySmallStrong as="h4" id={id}>
        Result
      </Typography.BodySmallStrong>
      {(page.coverage.length > 0 || page.hasMore) && <CoverageStrip page={page} />}
      {page.hits.length === 0 ? (
        <Typography.Body>This page has no results.</Typography.Body>
      ) : (
        <List.Container as="ol" aria-label="Results in rank order">
          {page.hits.map((hit) => (
            <List.Item as="li" key={hit.callPath}>
              <Hit hit={hit} />
            </List.Item>
          ))}
        </List.Container>
      )}
    </section>
  )
}

/** One compact result: what it is, where it lives, and why it matched. Today's catalog page
 * holds its signature and description. */
function Hit({ hit }: { hit: SearchHit }) {
  return (
    <>
      <div className={styles.row}>
        <List.Title>{hit.title}</List.Title>
        <KindPill kind={hit.kind} />
        {hit.operation && <Flags operation={hit.operation} approval="icon" />}
      </div>
      <div className={styles.row}>
        <CodeChip>{hit.callPath}</CodeChip>
        <CurrentIconLink target={hit} />
      </div>
      <MatchedIn matched={hit.matched} />
    </>
  )
}

function KindPill({ kind }: { kind: DiscoveryEntityKind }) {
  return kind === 'operation' ? (
    <Pill color="blue" size="small" variant="outline">
      operation
    </Pill>
  ) : (
    <Pill color="purple" size="small" variant="outline">
      saved function
    </Pill>
  )
}

const hintLabels: [keyof OperationHintsView, string][] = [
  ['readOnly', 'read-only'],
  ['destructive', 'destructive'],
  ['idempotent', 'idempotent'],
  ['openWorld', 'open world'],
]
/** A hint shows only when the provider declared it; a declared false reads "not". */
function Flags({
  operation,
  approval = 'pill',
}: {
  operation: OperationFlags
  /** A compact search result shows approval as the policy hand the Providers page uses. */
  approval?: 'pill' | 'icon'
}) {
  return (
    <>
      {operation.approvalRequired &&
        (approval === 'icon' ? (
          <NeedsApproval />
        ) : (
          <Pill color="amber" size="small">
            approval required
          </Pill>
        ))}
      {operation.deprecated && (
        <Pill color="orange" size="small">
          deprecated
        </Pill>
      )}
      {hintLabels.map(([hint, label]) => {
        const declared = operation.hints[hint]
        return declared === undefined ? null : (
          <Pill key={hint} color={declared ? 'gray' : 'graySubtle'} size="small" variant="outline">
            {declared ? label : `not ${label}`}
          </Pill>
        )
      })}
    </>
  )
}

const needsApproval = policyOptions.find((option) => option.value === 'approval_required')!
/** The policy hand, named and explained as the Providers page's policy control does. */
function NeedsApproval() {
  const { label, icon, description } = needsApproval
  return (
    <Tooltip
      content={
        <>
          {label}
          <br />
          {description}
        </>
      }
    >
      <span
        role="img"
        aria-label={`${label}. ${description}`}
        tabIndex={0}
        className={styles.approval}
      >
        <Icon name={icon} size="14" color="inherit" />
      </span>
    </Tooltip>
  )
}

const fieldLabels: Record<string, string> = {
  call_path: 'call path',
  provider_namespace: 'provider namespace',
  provider_display_name: 'provider name',
}
/** Why the hit matched. Matched tags and declaration paths replace their bare field names. */
function MatchedIn({ matched }: { matched: SearchHit['matched'] }) {
  const fields = matched.fields.filter((field) => field !== 'tags' || matched.tags.length === 0)
  if (fields.length + matched.tags.length + matched.paths.length === 0) return null
  return (
    <div className={styles.row}>
      <Typography.BodySmall variant="tertiary">Matched in</Typography.BodySmall>
      {fields.map((field) => (
        <Pill key={field} color="green" size="small" variant="outline">
          {fieldLabels[field] ?? field.replaceAll('_', ' ')}
        </Pill>
      ))}
      {matched.tags.map((tag) => (
        <Pill key={tag} color="green" size="small">
          tag: {tag}
        </Pill>
      ))}
      {matched.paths.length > 0 && (
        <Pill color="green" size="small" variant="outline">
          {matched.paths.length === 1
            ? '1 declaration path'
            : `${matched.paths.length} declaration paths`}
        </Pill>
      )}
    </div>
  )
}

/** The link opens today's catalog, not the one the agent saw. A target that is gone has none. */
/** Today's catalog page for a search result, beside its exact call path. A target gone from
 * the catalog has no link. */
function CurrentIconLink({
  target,
}: {
  target: Pick<SearchHit, 'kind' | 'callPath' | 'currentHref'>
}) {
  if (target.currentHref === null) return null
  const subject = target.kind === 'operation' ? 'operation' : 'function'
  return (
    <Button.IconLink
      name="ExternalLink"
      to={target.currentHref}
      tooltipText={`Open current ${subject} ${target.callPath}`}
      size="22"
    />
  )
}
function CurrentLink({ target }: { target: Pick<SearchHit, 'kind' | 'currentHref'> }) {
  if (target.currentHref === null) return null
  return (
    <Typography.BodySmall>
      <Link to={target.currentHref}>
        {target.kind === 'operation' ? 'Open current operation' : 'Open current function'}
      </Link>
    </Typography.BodySmall>
  )
}

const diagnosticColors = {
  error: 'red',
  warning: 'amber',
  suggestion: 'gray',
  message: 'gray',
} as const

function DescriptionDocument({ description }: { description: Description }) {
  const { operation, savedFunction } = description
  return (
    <>
      <div className={styles.row}>
        <Typography.HeadingSmall as="h3">{description.title}</Typography.HeadingSmall>
        <KindPill kind={description.kind} />
        {operation && <Flags operation={operation} />}
      </div>
      {operation && (
        <dl className={styles.facts}>
          <Fact label="Provider">{operation.providerLabel}</Fact>
          {operation.tags.length > 0 && (
            <Fact label="Tags">
              <span className={styles.row}>
                {operation.tags.map((tag) => (
                  <Pill key={tag} color="graySubtle" size="small">
                    {tag}
                  </Pill>
                ))}
              </span>
            </Fact>
          )}
          {operation.guidance && <Fact label="Guidance">{operation.guidance}</Fact>}
        </dl>
      )}
      <Markdown>{description.description}</Markdown>
      <Typography.HeadingXSmall as="h4">Declaration</Typography.HeadingXSmall>
      <CodeBlock
        value={description.declaration}
        format="typescript"
        framed
        maxLines={DECLARATION_LINES}
      />
      {savedFunction && (
        <>
          <Typography.HeadingXSmall as="h4">Source</Typography.HeadingXSmall>
          <CodeBlock
            value={savedFunction.source}
            format="typescript"
            framed
            maxLines={DECLARATION_LINES}
          />
          {(savedFunction.diagnostics.length > 0 || savedFunction.diagnosticsTruncated) && (
            <>
              <Typography.HeadingXSmall as="h4">Diagnostics</Typography.HeadingXSmall>
              <ul className={styles.diagnostics}>
                {savedFunction.diagnostics.map((diagnostic, index) => (
                  <li key={index} className={styles.row}>
                    <Pill color={diagnosticColors[diagnostic.category]} size="small">
                      {diagnostic.category} TS{diagnostic.code}
                    </Pill>
                    {diagnostic.line !== null && (
                      <Typography.BodySmall>
                        {diagnostic.line}
                        {diagnostic.column !== null && `:${diagnostic.column}`}
                      </Typography.BodySmall>
                    )}
                    <Typography.Body>{diagnostic.message}</Typography.Body>
                  </li>
                ))}
              </ul>
              {savedFunction.diagnosticsTruncated && (
                <Typography.BodySmall variant="tertiary">
                  Coral kept only the first diagnostics.
                </Typography.BodySmall>
              )}
            </>
          )}
        </>
      )}
      <CurrentLink target={description} />
    </>
  )
}

function Fact({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <>
      <Typography.BodySmall as="dt" variant="tertiary">
        {label}
      </Typography.BodySmall>
      <Typography.BodySmall as="dd" className={styles.fact}>
        {children}
      </Typography.BodySmall>
    </>
  )
}
