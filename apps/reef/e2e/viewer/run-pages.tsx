import { Banner } from '@/wax/components/banner'
import * as Card from '@/wax/components/card'
import * as Table from '@/wax/components/table'
import { Typography } from '@/wax/components/typography'

import {
  Breadcrumbs,
  Counts,
  Empty,
  EvidenceLink,
  Failure,
  RunContext,
  Section,
  Status,
  date,
} from './shared'
import type { RunSummary } from './types'
import { ScenarioFolders } from './scenario-folders'
import * as styles from './reel.css'

function ScenarioTable({ run, prefix }: { run: RunSummary; prefix: string }) {
  const waiting = run.targets.filter(
    (target) =>
      ['waiting', 'starting'].includes(target.state) ||
      (target.state === 'ready' &&
        !run.results.some(({ result }) => result.target === target.name)),
  )
  const rank = { failed: 0, pending: 1, skipped: 2, passed: 3 }
  const results = run.results.toSorted(
    (a, b) =>
      rank[a.result.status] - rank[b.result.status] ||
      a.result.scenario.localeCompare(b.result.scenario) ||
      a.result.source.localeCompare(b.result.source) ||
      a.result.target.localeCompare(b.result.target) ||
      a.path.localeCompare(b.path),
  )
  return (
    <div className={styles.stack}>
      {run.setupError && (
        <Failure title="Target failure">
          <Typography.Code as="pre" className={styles.code}>
            {run.setupError}
          </Typography.Code>
        </Failure>
      )}
      {waiting.length > 0 && (
        <Banner>
          Waiting for scenario results from {waiting.map((target) => target.name).join(', ')}.
          Refresh to see progress.
        </Banner>
      )}
      {run.warning && <Banner variant="warning">{run.warning}</Banner>}
      {results.length ? (
        <Table.Container
          ariaLabel="Scenario results"
          variant="card"
          columns={[
            { label: 'Scenario', width: 'minmax(260px, 1fr)' },
            { label: 'Result', width: 170 },
            { label: 'Target', width: 180 },
            { label: 'Duration', width: 90, align: 'right' },
          ]}
        >
          <Table.Head />
          <ScenarioFolders entries={results} prefix={prefix} />
        </Table.Container>
      ) : (
        <Empty>
          <div className={styles.intro}>
            <Typography.HeadingSmall as="h3">No scenario results</Typography.HeadingSmall>
            <Typography.Body as="p">
              The run may still be starting, have failed before test execution, or have selected no
              scenarios. This is not a passing result.
            </Typography.Body>
            <Typography.Body as="p">Refresh to check for new evidence.</Typography.Body>
          </div>
        </Empty>
      )}
    </div>
  )
}
function History({ runs }: { runs: RunSummary[] }) {
  if (!runs.length) return <Typography.Body as="p">No runs in this section yet.</Typography.Body>
  return (
    <ol className={styles.history}>
      {runs.map((run) => (
        <li key={run.id}>
          <Card.Container className={styles.historyItem}>
            <div className={styles.intro}>
              <div>
                <EvidenceLink href={`/${encodeURIComponent(run.id)}/index.html`}>
                  {run.startedAt ? date(run.startedAt) : run.id} →
                </EvidenceLink>
              </div>
              <RunContext run={run} />
              {run.warning && <Typography.BodySmall as="p">{run.warning}</Typography.BodySmall>}
            </div>
            <div className={styles.row}>
              {run.setupError && <Status status="failed">Target failure</Status>}
              <Counts results={run.results} />
            </div>
          </Card.Container>
        </li>
      ))}
    </ol>
  )
}
export function LandingPage({ runs }: { runs: RunSummary[] }) {
  const ordered = runs.toSorted(
    (a, b) =>
      (Date.parse(b.startedAt ?? '') || 0) - (Date.parse(a.startedAt ?? '') || 0) ||
      b.id.localeCompare(a.id),
  )
  const scenarios = ordered.filter((run) => !run.probes)
  const probes = ordered.filter((run) => run.probes)
  const latest = scenarios[0]
  return (
    <>
      <header className={styles.intro}>
        <Typography.BodySmall>Review real browser journeys</Typography.BodySmall>
        <Typography.HeadingLarge as="h1">Reel</Typography.HeadingLarge>
        <Typography.Body as="p">
          Choose a scenario to watch what happened, inspect its steps, or read the test.
        </Typography.Body>
      </header>
      <Section
        title="Latest scenario run"
        aside={
          latest && (
            <EvidenceLink href={`/${encodeURIComponent(latest.id)}/index.html`}>
              View test run →
            </EvidenceLink>
          )
        }
      >
        {latest ? (
          <>
            <RunContext run={latest} />
            <Counts results={latest.results} />
            <ScenarioTable run={latest} prefix={`/${encodeURIComponent(latest.id)}/`} />
          </>
        ) : (
          <Empty>
            <div className={styles.intro}>
              <Typography.HeadingSmall as="h3">No scenario runs yet</Typography.HeadingSmall>
              <Typography.Body as="p">
                Record a browser journey to see its results here.
              </Typography.Body>
              <Typography.Code as="code">npm run test:e2e --prefix apps/reef</Typography.Code>
            </div>
          </Empty>
        )}
      </Section>
      <Section
        title="Run history"
        aside={
          <Typography.BodySmall>
            {scenarios.length} scenario run{scenarios.length === 1 ? '' : 's'}
          </Typography.BodySmall>
        }
      >
        <Typography.BodySmall as="p">
          Each entry is one test command, including every selected target. Separate commands and
          commits stay separate.
        </Typography.BodySmall>
        <History runs={scenarios} />
      </Section>
      <Card.Container as="details">
        <Typography.BodyStrong as="summary" className={styles.summary}>
          Harness self-checks ({probes.length})
        </Typography.BodyStrong>
        <div className={`${styles.stack} ${styles.detailsContent}`}>
          <Typography.BodySmall as="p">
            Recorder diagnostics include deliberate assertion failures and timeouts. They are not
            product test failures.
          </Typography.BodySmall>
          <History runs={probes} />
        </div>
      </Card.Container>
    </>
  )
}
export function RunPage({ run }: { run: RunSummary }) {
  return (
    <>
      <Breadcrumbs items={[{ label: 'All runs', href: '/' }, { label: 'Test run' }]} />
      <header className={styles.intro}>
        <Typography.BodySmall>
          {run.probes
            ? 'Harness self-check · includes deliberate failures'
            : 'Recorded scenario results'}
        </Typography.BodySmall>
        <Typography.HeadingLarge as="h1">Test run</Typography.HeadingLarge>
        <RunContext run={run} />
        <Counts results={run.results} />
      </header>
      <ScenarioTable run={run} prefix="" />
      <Section title="Run details">
        <Typography.BodySmall as="p">
          Each row is one test scenario. Outcomes and durations include the whole test and its
          hooks; recordings are child evidence.
        </Typography.BodySmall>
        <div>
          <EvidenceLink href="run.json">Run metadata</EvidenceLink>
        </div>
        <ul className={styles.intro}>
          {run.targets.map((target) => (
            <li key={target.name}>
              <EvidenceLink href={`${encodeURIComponent(target.name)}/target.json`}>
                {target.name} metadata
              </EvidenceLink>
              <Typography.BodySmall> · {target.state}</Typography.BodySmall>
            </li>
          ))}
        </ul>
        <Card.Container as="details">
          <Typography.BodyStrong as="summary" className={styles.summary}>
            Artifact directory
          </Typography.BodyStrong>
          <Typography.Code as="code">{run.id}</Typography.Code>
        </Card.Container>
      </Section>
    </>
  )
}
