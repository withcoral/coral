import type { ReactNode } from 'react'

import { ErrorBanner } from '@/components/error-banner'
import {
  QueryDetailResults,
  QueryDetailSummary,
  type QueryDetailStatusTone,
} from '@/components/query-detail'
import type { SourceCatalogEntry } from '@/components/sources'
import { formatSQL } from '@/lib/sql-highlight'
import { Table, Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'

import { OnboardingPage } from './onboarding-page'
import * as styles from './onboarding-sample-query-page.css'
import { getOnboardingStepState } from './onboarding-steps'
import { pluralise } from '~/utils/pluralise'

export type SampleQueryLoadState = 'error' | 'idle' | 'loading' | 'success'

export const ONBOARDING_SAMPLE_QUERY = `SELECT schema_name AS source, COUNT(*) AS tables
FROM coral.tables
GROUP BY schema_name
ORDER BY schema_name`

export interface OnboardingSampleQueryRow {
  source: string
  tables: bigint | number | string
}

export interface OnboardingSampleQueryPageProps {
  connectedSources: SourceCatalogEntry[]
  continueDisabled?: boolean
  continueLabel?: string
  errorMessage?: string | null
  loadState?: SampleQueryLoadState
  onContinue?: () => void
  onRetry?: () => void
  rows?: OnboardingSampleQueryRow[]
}

export function OnboardingSampleQueryPage({
  connectedSources,
  continueDisabled = false,
  continueLabel = 'Finish setup',
  errorMessage = null,
  loadState = 'idle',
  onContinue,
  onRetry,
  rows = [],
}: OnboardingSampleQueryPageProps) {
  const step = getOnboardingStepState('query')
  const sourceCount = connectedSources.length
  const canContinue = sourceCount > 0 && loadState === 'success' && !continueDisabled

  return (
    <OnboardingPage
      action={{
        disabled: !canContinue,
        label: continueLabel,
        onClick: onContinue,
      }}
      ariaLabel="Confirm connected sources"
      step={step}
      sideContent={
        <>
          <Typography.BodyLarge>
            Coral exposes your connected {pluralise(sourceCount, 'source')} as tables, and converts
            SQL into what is necessary to retrieve data, whether that's multiple network calls, file
            reads, and more.
          </Typography.BodyLarge>
          <Typography.BodyLarge>
            Next time you ask a question to your agent, instead of having to learn how to call many
            different APIs, and how to understand their response format, your agent will only need
            to know how to write SQL and how to parse tabular data, two things agents are great at.
          </Typography.BodyLarge>
        </>
      }
      sideTitle={
        sourceCount === 0
          ? 'Connect a source to continue'
          : `Your ${pluralise(sourceCount, 'source is', 'sources are')} ready`
      }
    >
      <QueryPanelBody
        connectedSources={connectedSources}
        errorMessage={errorMessage}
        loadState={loadState}
        onRetry={onRetry}
        rows={rows}
      />
    </OnboardingPage>
  )
}

function QueryPanelBody({
  connectedSources,
  errorMessage,
  loadState,
  onRetry,
  rows,
}: {
  connectedSources: SourceCatalogEntry[]
  errorMessage: string | null
  loadState: SampleQueryLoadState
  onRetry?: () => void
  rows: OnboardingSampleQueryRow[]
}) {
  if (connectedSources.length === 0) {
    return <MissingSourceFallback />
  }

  const queryState = getQueryState({
    connectedSourceCount: connectedSources.length,
    errorMessage,
    loadState,
    onRetry,
    rows,
  })

  return (
    <QueryDetailSummary
      sql={formatSQL(ONBOARDING_SAMPLE_QUERY)}
      statusLabel={queryState.statusLabel}
      statusTone={queryState.statusTone}
      title="Query details"
    >
      {queryState.content}
    </QueryDetailSummary>
  )
}

function getQueryState({
  connectedSourceCount,
  errorMessage,
  loadState,
  onRetry,
  rows,
}: {
  connectedSourceCount: number
  errorMessage: string | null
  loadState: SampleQueryLoadState
  onRetry?: () => void
  rows: OnboardingSampleQueryRow[]
}): {
  content: ReactNode
  statusLabel: string
  statusTone: QueryDetailStatusTone
} {
  if (loadState === 'loading') {
    return {
      content: (
        <div className={styles.statePanel}>
          <Icon color="tertiary" name="Loader" size="16" className={styles.stateIcon} />
          <Typography.BodyLargeStrong>Running query</Typography.BodyLargeStrong>
          <Typography.Body variant="tertiary">
            Coral is checking the tables exposed by your connected{' '}
            {connectedSourceCount === 1 ? 'source' : 'sources'}.
          </Typography.Body>
        </div>
      ),
      statusLabel: 'running',
      statusTone: 'running',
    }
  }

  if (loadState === 'error') {
    return {
      content: (
        <ErrorBanner
          title="Couldn't run the catalog query"
          message={errorMessage ?? 'Check Coral and try again.'}
          onRetry={onRetry}
        />
      ),
      statusLabel: 'error',
      statusTone: 'error',
    }
  }

  if (loadState === 'success') {
    return {
      content: <CatalogQueryResults rows={rows} />,
      statusLabel: 'done',
      statusTone: 'ok',
    }
  }

  return {
    content: (
      <div className={styles.statePanel}>
        <Icon color="tertiary" name="Play" size="20" />
        <Typography.BodyLargeStrong>Ready to run</Typography.BodyLargeStrong>
        <Typography.Body variant="tertiary">
          Coral will query its catalog for your connected{' '}
          {connectedSourceCount === 1 ? 'source' : 'sources'}.
        </Typography.Body>
      </div>
    ),
    statusLabel: 'ready',
    statusTone: 'running',
  }
}

function MissingSourceFallback() {
  return (
    <QueryDetailSummary
      sql={formatSQL(ONBOARDING_SAMPLE_QUERY)}
      statusLabel="error"
      statusTone="error"
      title="Query details"
    >
      <ErrorBanner
        title="No connected sources"
        message="Connect at least one source before running the catalog query."
      />
    </QueryDetailSummary>
  )
}

function CatalogQueryResults({ rows }: { rows: OnboardingSampleQueryRow[] }) {
  return (
    <QueryDetailResults>
      {rows.length > 0 ? (
        <Table.Wrapper className={styles.resultTable} style="compact">
          <Table.Root>
            <Table.Head>
              <Table.Row>
                <Table.HeaderCell>Source</Table.HeaderCell>
                <Table.HeaderCell align="right">Tables</Table.HeaderCell>
              </Table.Row>
            </Table.Head>
            <Table.Body>
              {rows.map((row) => (
                <Table.Row key={row.source}>
                  <Table.Cell mono>{row.source}</Table.Cell>
                  <Table.Cell align="right" mono>
                    {row.tables.toString()}
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Root>
        </Table.Wrapper>
      ) : null}
    </QueryDetailResults>
  )
}
