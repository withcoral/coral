import { useEffect, useId, useState } from 'react'

import { Banner, Button, Table } from '@/wax/components'
import { Pill, type PillColor } from '@/wax/components/pill'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import type {
  SearchFieldView,
  SearchFunctionResultView,
  SearchKnownResultViewBase,
  SearchMatchingValuesView,
  SearchProviderView,
  SearchResultView,
  SearchResultsView,
  SearchTableResultView,
  SearchTruncationView,
} from './search-response'
import * as styles from './search-response-results.css'

export interface SearchResponseResultsProps {
  view: SearchResultsView
}

const RESULT_COLUMNS: Table.Column[] = [
  { ariaLabel: 'Result details', label: '', width: 46 },
  { align: 'right', ariaLabel: 'Result rank', label: '#', width: 44 },
  { label: 'type', width: 'content' },
  { label: 'result', width: 'fill' },
  { label: 'found via', width: 'content' },
]

const FIELD_COLUMNS: Table.Column[] = [
  { label: 'name', width: 'content' },
  { label: 'type', width: 'content' },
]

function providerColor(provider: SearchProviderView): PillColor {
  if (provider.tone === 'catalog') return 'blue'
  if (provider.tone === 'observed') return 'purple'
  return 'graySubtle'
}

function ProviderBadge({ provider }: { provider: SearchProviderView }) {
  return (
    <Pill as="span" color={providerColor(provider)}>
      {provider.label}
    </Pill>
  )
}

function ProviderBadges({ providers }: { providers: SearchProviderView[] }) {
  if (providers.length === 0) {
    return <Typography.BodySmall variant="tertiary">—</Typography.BodySmall>
  }

  return (
    <span className={styles.providerBadges}>
      {providers.map((provider, index) => (
        <ProviderBadge key={`${provider.label}-${index}`} provider={provider} />
      ))}
    </span>
  )
}

function Fields({
  fields,
  requiredLabel,
  title,
}: {
  fields: SearchFieldView[]
  requiredLabel?: string
  title: string
}) {
  if (fields.length === 0) return null
  return (
    <section className={styles.section}>
      <Typography.BodySmallStrong as="h3" className={styles.sectionTitle}>
        {title}
      </Typography.BodySmallStrong>
      <Table.Container ariaLabel={title} columns={FIELD_COLUMNS} density="compact">
        <Table.Head />
        <Table.Body>
          {fields.map((field, index) => (
            <Table.Row key={`${field.name}-${index}`}>
              <Table.Cell mono>
                {field.name}
                {field.required && requiredLabel ? (
                  <Tooltip content={requiredLabel} side="top">
                    <span aria-label={requiredLabel} className={styles.requiredStar} tabIndex={0}>
                      *
                    </span>
                  </Tooltip>
                ) : null}
              </Table.Cell>
              <Table.Cell mono>{field.dataType || 'Type unavailable'}</Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Container>
    </section>
  )
}

function MatchingValues({ groups }: { groups: SearchMatchingValuesView[] }) {
  if (groups.length === 0) return null
  return (
    <section className={styles.section}>
      <Typography.BodySmallStrong as="h3" className={styles.sectionTitle}>
        Matching values
      </Typography.BodySmallStrong>
      <dl className={styles.matchingValues}>
        {groups.map((group, index) => (
          <div className={styles.matchingValueRow} key={`${group.field}-${index}`}>
            <Typography.CodeSmallInlineStrong as="dt" className={styles.matchingValueField}>
              {group.field}
            </Typography.CodeSmallInlineStrong>
            <Typography.BodySmall as="dd" className={styles.matchingValueCopy} variant="secondary">
              {group.values.length > 0 ? group.values.join(', ') : 'No values retained'}
            </Typography.BodySmall>
          </div>
        ))}
      </dl>
    </section>
  )
}

function ResultDescription({ result }: { result: SearchKnownResultViewBase }) {
  if (!result.description) return null
  return (
    <Typography.Body as="p" className={styles.bodyCopy} variant="secondary">
      {result.description}
    </Typography.Body>
  )
}

function CommonResultSections({ result }: { result: SearchKnownResultViewBase }) {
  return (
    <>
      <MatchingValues groups={result.matchingValues} />
      {result.omittedMatchingFieldCount > 0 ? (
        <Typography.BodySmall as="p" className={styles.bodyCopy} variant="tertiary">
          {result.omittedMatchingFieldCount} more matching field
          {result.omittedMatchingFieldCount === 1 ? '' : 's'} not shown.
        </Typography.BodySmall>
      ) : null}
      {result.guide ? (
        <section className={styles.section}>
          <Typography.BodySmallStrong as="h3" className={styles.sectionTitle}>
            Guide
          </Typography.BodySmallStrong>
          <Typography.Body as="p" className={styles.bodyCopy} variant="secondary">
            {result.guide}
          </Typography.Body>
        </section>
      ) : null}
    </>
  )
}

function TableResultBody({ result }: { result: SearchTableResultView }) {
  return (
    <>
      <ResultDescription result={result} />
      <Fields fields={result.fields} requiredLabel="Required filter" title="Fields" />
      <CommonResultSections result={result} />
    </>
  )
}

function FunctionResultBody({ result }: { result: SearchFunctionResultView }) {
  return (
    <>
      <ResultDescription result={result} />
      <Fields fields={result.arguments} requiredLabel="Required argument" title="Arguments" />
      <Fields fields={result.returns} title="Returns" />
      <CommonResultSections result={result} />
    </>
  )
}

function KnownResultRows({
  expanded,
  onToggle,
  rank,
  result,
}: {
  expanded: boolean
  onToggle: () => void
  rank: number
  result: SearchFunctionResultView | SearchTableResultView
}) {
  const detailsId = useId()
  const resultLabel = result.kind === 'table' ? 'Table' : 'Function'

  return (
    <>
      <Table.Row>
        <Table.Cell className={styles.disclosureCell}>
          <Button.IconButton
            aria-controls={expanded ? detailsId : undefined}
            aria-expanded={expanded}
            ariaLabel={`${expanded ? 'Hide' : 'Show'} details for ${result.sqlReference}`}
            name={expanded ? 'ChevronDown' : 'ChevronRight'}
            onClick={onToggle}
            size="22"
            variant="bare"
          />
        </Table.Cell>
        <Table.Cell mono>{rank}</Table.Cell>
        <Table.Cell>
          <Pill as="span" color={result.kind === 'table' ? 'green' : 'orange'}>
            {resultLabel}
          </Pill>
        </Table.Cell>
        <Table.Cell mono title={result.sqlReference}>
          {result.sqlReference}
        </Table.Cell>
        <Table.Cell wrap>
          <ProviderBadges providers={result.providers} />
        </Table.Cell>
      </Table.Row>
      {expanded ? (
        <Table.Row className={styles.resultDetailRow}>
          <Table.Cell className={styles.resultDetailCell} fullWidth wrap>
            <div className={styles.resultBody} id={detailsId}>
              {result.kind === 'table' ? (
                <TableResultBody result={result} />
              ) : (
                <FunctionResultBody result={result} />
              )}
            </div>
          </Table.Cell>
        </Table.Row>
      ) : null}
    </>
  )
}

function UnknownResultRow({ rank }: { rank: number }) {
  return (
    <Table.Row>
      <Table.Cell>
        <span aria-hidden className={styles.disclosureSpacer} />
      </Table.Cell>
      <Table.Cell mono>{rank}</Table.Cell>
      <Table.Cell>
        <Pill as="span" color="graySubtle">
          Unknown
        </Pill>
      </Table.Cell>
      <Table.Cell>Unknown result</Table.Cell>
      <Table.Cell>
        <Typography.BodySmall variant="tertiary">—</Typography.BodySmall>
      </Table.Cell>
    </Table.Row>
  )
}

function ResultRows({
  expanded,
  onToggle,
  rank,
  result,
}: {
  expanded: boolean
  onToggle: () => void
  rank: number
  result: SearchResultView
}) {
  return result.kind === 'unknown' ? (
    <UnknownResultRow rank={rank} />
  ) : (
    <KnownResultRows expanded={expanded} onToggle={onToggle} rank={rank} result={result} />
  )
}

function Truncation({ truncation }: { truncation: SearchTruncationView }) {
  return (
    <Banner title="Results truncated">
      <Typography.BodySmall as="p">
        Showing {truncation.returnedCount} {truncation.returnedCount === 1 ? 'result' : 'results'}.
        The result limit was {truncation.maxResults}.
      </Typography.BodySmall>
      {truncation.note ? (
        <Typography.BodySmall as="p" variant="tertiary">
          {truncation.note}
        </Typography.BodySmall>
      ) : null}
    </Banner>
  )
}

function ResultState({ children }: { children: string }) {
  return (
    <div className={styles.resultState}>
      <Typography.Body variant="tertiary">{children}</Typography.Body>
    </div>
  )
}

function AvailableResults({ view }: { view: Extract<SearchResultsView, { state: 'available' }> }) {
  const [expandedResults, setExpandedResults] = useState<Set<number>>(() => new Set())

  useEffect(() => setExpandedResults(new Set()), [view])

  const toggleResult = (index: number) => {
    setExpandedResults((current) => {
      const next = new Set(current)
      if (next.has(index)) next.delete(index)
      else next.add(index)
      return next
    })
  }

  return (
    <div className={styles.root}>
      {view.results.length > 0 ? (
        <Table.Container
          ariaLabel="Search results"
          className={styles.resultTable}
          columns={RESULT_COLUMNS}
          density="compact"
          variant="card"
        >
          <Table.Head />
          <Table.Body>
            {view.results.map((result, index) => (
              <ResultRows
                expanded={expandedResults.has(index)}
                key={index}
                onToggle={() => toggleResult(index)}
                rank={index + 1}
                result={result}
              />
            ))}
          </Table.Body>
        </Table.Container>
      ) : (
        <ResultState>No results found for this search.</ResultState>
      )}
      {view.truncation?.truncated ? <Truncation truncation={view.truncation} /> : null}
    </div>
  )
}

export function SearchResponseResults({ view }: SearchResponseResultsProps) {
  if (view.state === 'unavailable') {
    return (
      <div className={styles.root}>
        <ResultState>Results are unavailable for this search.</ResultState>
      </div>
    )
  }
  if (view.state === 'tooLarge') {
    return (
      <div className={styles.root}>
        <ResultState>This result response was too large to retain.</ResultState>
      </div>
    )
  }

  return <AvailableResults view={view} />
}
