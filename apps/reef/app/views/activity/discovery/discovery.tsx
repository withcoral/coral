import classNames from 'classnames'
import { useId } from 'react'
import { Banner, Button, List, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { CodeChip } from '@/wax/components/code-chip'
import * as DescriptionList from '@/wax/components/description-list'
import { Disclosure } from '@/wax/components/disclosure'
import { Markdown } from '@/wax/components/markdown'
import { Pill } from '@/wax/components/pill'
import type { ToolHintsView } from '@/views/providers/types'
import { policyOptions } from '@/components/policy-toggle'
import { Icon } from '@/wax/components/icon'
import { Tooltip } from '@/wax/components/tooltip'
import type {
  Description,
  DiscoveryContext,
  DiscoveryEntityKind,
  InteractionEvidence,
  ToolFlags,
  SearchHit,
  SearchPage,
} from '../types'
import { coverageLine, worstTone, type CoverageTone } from './coverage'
import * as styles from './discovery.css'

/** A declaration or a source stays open, but takes less than one screen. */
const DECLARATION_LINES = 20

/** One retained `search` or `describe` attempt, as the agent received it then. Each result
 * card keeps its retained documentation in a closed Details inside the card. A describe
 * never shows its raw response; a search Reef cannot read shows as stored. */
export function DiscoveryEvidence({
  evidence,
  record,
  describe = false,
}: {
  evidence: Extract<InteractionEvidence, { kind: 'discovery' }>
  /** The selected record's name, kept in a describe's Details. */
  record?: string | null
  /** The attempt was a `describe`, even when its stored request context is unavailable. */
  describe?: boolean
}) {
  const { context, view, response } = evidence
  const raw = view.kind === 'raw' && !describe
  return (
    <div className={styles.stack}>
      {view.kind === 'search' && <SearchLedger page={view.page} />}
      {view.kind === 'describe' && (
        <DescriptionResult description={view.description} record={record} />
      )}
      {view.kind === 'raw' &&
        (context.kind === 'unknown' ? (
          <Typography.Body>The request context is unavailable.</Typography.Body>
        ) : (
          <Banner variant="info" title="Stored in an earlier shape">
            Reef cannot show this response in the current layout.
            {raw && ' The stored response follows.'}
          </Banner>
        ))}
      {raw && <RawResponse response={response} />}
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
    // Under the panel's h2 heading.
    <DescriptionList.List title="Search" heading="h3">
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

/** What a describe asked for, as a search states its query, then `children`: the call's own
 * times, in the same list. The Provider is the described Tool's, from the stored
 * response. The panel's subtitle names the call, so the list has no heading. */
export function DescribeParameters({
  context,
  description,
  intent,
  children,
}: {
  context: Extract<DiscoveryContext, { kind: 'describe' }>
  description: Description | null
  intent: string
  children?: React.ReactNode
}) {
  return (
    <DescriptionList.List>
      <DescriptionList.Row label="Call path">
        <CodeChip>{context.callPath}</CodeChip>
      </DescriptionList.Row>
      {context.types.length > 0 && (
        <DescriptionList.Row label="Types">
          <span className={styles.row}>
            {context.types.map((name) => (
              <CodeChip key={name}>{name}</CodeChip>
            ))}
          </span>
        </DescriptionList.Row>
      )}
      {description?.tool && (
        <DescriptionList.Row label="Provider">{description.tool.providerLabel}</DescriptionList.Row>
      )}
      {intent && <DescriptionList.Row label="Intent">{intent}</DescriptionList.Row>}
      {children}
    </DescriptionList.List>
  )
}

const toneColors = { success: 'green', neutral: 'gray', warning: 'amber', error: 'red' } as const

/** Per-kind coverage. A "+" on a count says more matched than Coral returned. */
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

/** A trimmed page states coverage only for a kind whose results may be missing. Coral
 * stores no total match count, so the heading counts this page and a "+" says more remain. */
function SearchLedger({ page }: { page: SearchPage }) {
  const id = useId()
  return (
    <section className={styles.stack} aria-labelledby={id}>
      <Typography.BodySmallStrong as="h4" id={id}>
        Results ({page.hits.length}
        {page.hasMore && '+'})
      </Typography.BodySmallStrong>
      {page.coverage.length > 0 && <CoverageStrip page={page} />}
      {page.hits.length === 0 ? (
        <Typography.Body>This page has no results.</Typography.Body>
      ) : (
        <List.Container as="ol" aria-label="Results in rank order">
          {page.hits.map((hit) => (
            <List.Item as="li" key={hit.callPath} className={styles.detailed}>
              <Hit hit={hit} />
            </List.Item>
          ))}
        </List.Container>
      )}
    </section>
  )
}

/** One compact result: what it is, where it lives, and why it matched. What else the page
 * retained about it waits in Details at the foot of the same card. */
function Hit({ hit }: { hit: SearchHit }) {
  return (
    <>
      <div className={styles.row}>
        <List.Title>{hit.title}</List.Title>
        <KindPill kind={hit.kind} />
        {hit.tool && <Flags tool={hit.tool} approval="icon" />}
      </div>
      <div className={styles.row}>
        <CodeChip>{hit.callPath}</CodeChip>
        <CurrentIconLink target={hit} />
      </div>
      <MatchedIn matched={hit.matched} />
      <HitDetails hit={hit} />
    </>
  )
}

/** The guidance, description and signature the page retained for a result, as Coral cut them.
 * A result that retained none of them has no Details. */
function HitDetails({ hit }: { hit: SearchHit }) {
  const guidance = hit.guidanceSnippet?.trim() ? hit.guidanceSnippet : null
  const description = hit.descriptionSnippet.trim() ? hit.descriptionSnippet : null
  const signature = hit.signatureLine.trim() ? hit.signatureLine : null
  if (guidance === null && description === null && signature === null) return null
  return (
    <Disclosure title="Details">
      {guidance !== null && (
        <dl className={styles.facts}>
          <Fact label="Guidance">{guidance}</Fact>
        </dl>
      )}
      {/* Cut Markdown: shown as the text Coral kept. */}
      {description !== null && (
        <Typography.Body className={styles.snippet}>{description}</Typography.Body>
      )}
      {signature !== null && (
        <>
          <Typography.HeadingXSmall as="h5">Signature</Typography.HeadingXSmall>
          <CodeBlock value={signature} format="typescript" framed wrap />
        </>
      )}
    </Disclosure>
  )
}

function KindPill({ kind }: { kind: DiscoveryEntityKind }) {
  return kind === 'provider_tool' ? (
    <Pill color="blue" size="small" variant="outline">
      tool
    </Pill>
  ) : (
    <Pill color="purple" size="small" variant="outline">
      saved function
    </Pill>
  )
}

const hintLabels: [keyof ToolHintsView, string][] = [
  ['readOnly', 'read-only'],
  ['destructive', 'destructive'],
  ['idempotent', 'idempotent'],
  ['openWorld', 'open world'],
]
/** A hint shows only when the provider declared it; a declared false reads "not". */
function Flags({
  tool,
  approval = 'pill',
}: {
  tool: ToolFlags
  /** A compact search result shows approval as the policy hand the Providers page uses. */
  approval?: 'pill' | 'icon'
}) {
  return (
    <>
      {tool.approvalRequired &&
        (approval === 'icon' ? (
          <NeedsApproval />
        ) : (
          <Pill color="amber" size="small">
            approval required
          </Pill>
        ))}
      {tool.deprecated && (
        <Pill color="orange" size="small">
          deprecated
        </Pill>
      )}
      {hintLabels.map(([hint, label]) => {
        const declared = tool.hints[hint]
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
  const subject = target.kind === 'provider_tool' ? 'tool' : 'function'
  return (
    <Button.IconLink
      name="ExternalLink"
      to={target.currentHref}
      tooltipText={`Open current ${subject} ${target.callPath}`}
      size="22"
    />
  )
}
const diagnosticColors = {
  error: 'red',
  warning: 'amber',
  suggestion: 'gray',
  message: 'gray',
} as const

/** A describe's target, as compact as a search result: what it is, where it lives, its
 * flags and tags. The documentation stays in Details at the foot of the same card. */
function DescriptionResult({
  description,
  record,
}: {
  description: Description
  record?: string | null
}) {
  const id = useId()
  const { tool, savedFunction } = description
  const problems = (['error', 'warning'] as const)
    .map((category) => ({
      category,
      count: savedFunction?.diagnostics.filter((item) => item.category === category).length ?? 0,
    }))
    .filter(({ count }) => count > 0)
  return (
    <section className={styles.stack} aria-labelledby={id}>
      <Typography.BodySmallStrong as="h4" id={id}>
        Result
      </Typography.BodySmallStrong>
      <List.Container>
        <List.Item className={styles.detailed}>
          <div className={styles.row}>
            <List.Title>{description.title}</List.Title>
            <KindPill kind={description.kind} />
            {tool && <Flags tool={tool} approval="icon" />}
          </div>
          <div className={styles.row}>
            <CodeChip>{description.callPath}</CodeChip>
            <CurrentIconLink target={description} />
          </div>
          {(tool?.tags.length || problems.length > 0) && (
            <div className={styles.row}>
              {tool?.tags.map((tag) => (
                <Pill key={tag} color="graySubtle" size="small">
                  {tag}
                </Pill>
              ))}
              {problems.map(({ category, count }) => (
                <Pill key={category} color={diagnosticColors[category]} size="small">
                  {count} {category}
                  {count === 1 ? '' : 's'}
                  {savedFunction?.diagnosticsTruncated ? '+' : ''}
                </Pill>
              ))}
            </div>
          )}
          <DescribeDetails description={description} record={record} />
        </List.Item>
      </List.Container>
    </section>
  )
}

/** Everything a describe retained beyond its Result, closed until you open it. */
function DescribeDetails({
  description,
  record,
}: {
  description: Description
  record?: string | null
}) {
  const savedFunction = description.savedFunction
  return (
    <Disclosure title="Details">
      {description.tool?.guidance && (
        <dl className={styles.facts}>
          <Fact label="Guidance">{description.tool.guidance}</Fact>
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
      {record && (
        <dl className={styles.facts}>
          <Fact label="Record">
            <Typography.Code className={styles.record}>{record}</Typography.Code>
          </Fact>
        </dl>
      )}
    </Disclosure>
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
