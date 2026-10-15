import classNames from 'classnames'
import { Link } from 'react-router'
import { Banner, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { CodeChip } from '@/wax/components/code-chip'
import { Disclosure } from '@/wax/components/disclosure'
import { Markdown } from '@/wax/components/markdown'
import { Pill } from '@/wax/components/pill'
import type { OperationHintsView } from '@/views/providers/types'
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
      {context.kind === 'search' && <SearchRequest context={context} />}
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
      {view.kind === 'raw' ? (
        <RawResponse response={response} />
      ) : (
        <Disclosure title="Raw response">
          <RawResponse response={response} />
        </Disclosure>
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

/** What the agent asked for, apart from the intent it gave for the call. */
function SearchRequest({ context }: { context: Extract<DiscoveryContext, { kind: 'search' }> }) {
  return (
    <dl className={styles.facts}>
      <Fact label="Query">
        {context.query === null ? (
          'None. The agent listed without a query.'
        ) : (
          <Typography.BodySmallStrong>“{context.query}”</Typography.BodySmallStrong>
        )}
      </Fact>
      <Fact label="Provider">
        {context.provider === null ? 'all providers' : <CodeChip>{context.provider}</CodeChip>}
      </Fact>
      <Fact label="Page">
        {context.cursor === null ? 'first page' : 'a later page'}, up to {context.limit} results
      </Fact>
    </dl>
  )
}

const toneColors = { success: 'green', neutral: 'gray', warning: 'amber', error: 'red' } as const

function CoverageStrip({ page }: { page: SearchPage }) {
  const tone = worstTone(page.coverage)
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
      {page.hasMore && (
        <Typography.BodySmall variant="tertiary" className={styles.coverageNote}>
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
  return (
    <>
      {(page.coverage.length > 0 || page.hasMore) && <CoverageStrip page={page} />}
      {page.hits.length === 0 ? (
        <Typography.Body>This page has no results.</Typography.Body>
      ) : (
        <ol className={styles.hits} aria-label="Results in rank order">
          {page.hits.map((hit, index) => (
            <li key={hit.callPath} className={styles.hit}>
              <span className={styles.rank} aria-hidden>
                {index + 1}
              </span>
              <Hit hit={hit} />
            </li>
          ))}
        </ol>
      )}
    </>
  )
}

function Hit({ hit }: { hit: SearchHit }) {
  return (
    <div className={styles.hitBody}>
      <div className={styles.row}>
        <Typography.BodyStrong>{hit.title}</Typography.BodyStrong>
        <KindPill kind={hit.kind} />
        {hit.operation && <Flags operation={hit.operation} />}
      </div>
      <div>
        <CodeChip>{hit.callPath}</CodeChip>
      </div>
      <CodeBlock value={hit.signatureLine} format="typescript" framed wrap />
      <Typography.Body className={styles.snippet}>{hit.descriptionSnippet}</Typography.Body>
      {hit.guidanceSnippet !== null && (
        <Typography.BodySmall>Guidance: {hit.guidanceSnippet}</Typography.BodySmall>
      )}
      <MatchedIn matched={hit.matched} />
      {hit.matched.paths.length > 0 && (
        <Disclosure
          variant="inline"
          title={`Matched declaration paths (${hit.matched.paths.length})`}
        >
          <div className={styles.row}>
            {hit.matched.paths.map((path) => (
              <CodeChip key={path}>{path}</CodeChip>
            ))}
          </div>
        </Disclosure>
      )}
      <CurrentLink target={hit} />
    </div>
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
function Flags({ operation }: { operation: OperationFlags }) {
  return (
    <>
      {operation.approvalRequired && (
        <Pill color="amber" size="small">
          approval required
        </Pill>
      )}
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
