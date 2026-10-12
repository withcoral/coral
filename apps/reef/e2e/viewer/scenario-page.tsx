import { Banner } from '@/wax/components/banner'
import * as Card from '@/wax/components/card'
import { Typography } from '@/wax/components/typography'

import {
  Breadcrumbs,
  Empty,
  EvidenceLink,
  Failure,
  Section,
  Source,
  Status,
  date,
  evidencePath,
  seconds,
  sessionLabel,
} from './shared'
import type { ScenarioPageProps } from './types'
import * as styles from './reel.css'

export function ScenarioPage({ result: r, sessions, source }: ScenarioPageProps) {
  const attempts = Math.max(
    1 + r.retries + r.repeats,
    ...sessions.map(({ result }) => result.attempt),
  )
  return (
    <>
      <Breadcrumbs
        items={[
          { label: 'All runs', href: '/' },
          { label: 'Test run', href: '../../index.html' },
          { label: 'Scenario' },
        ]}
      />
      <header className={styles.intro}>
        <Typography.BodySmall>Whole test scenario</Typography.BodySmall>
        <Typography.HeadingLarge as="h1" className={styles.heading}>
          {r.scenario}
        </Typography.HeadingLarge>
        <div className={styles.row}>
          <Status status={r.status} />
          <Typography.BodySmall>{r.target}</Typography.BodySmall>
          <Typography.BodySmall>{seconds(r.durationMs)} total test time</Typography.BodySmall>
          <Typography.BodySmall>{date(r.startedAt)}</Typography.BodySmall>
        </div>
        <Typography.BodySmall as="p">
          Outcome and elapsed time come from the test runner, including hooks and retries—not the
          sum of recordings.
        </Typography.BodySmall>
      </header>
      {r.failure && (
        <Failure>
          <Typography.Code as="pre" className={styles.code}>
            {r.failure}
          </Typography.Code>
        </Failure>
      )}
      {r.note && <Banner>{r.note}</Banner>}
      {r.status === 'pending' && (
        <Banner variant="warning">
          No final test outcome yet. Refresh after the test finishes; interrupted runs remain
          incomplete.
        </Banner>
      )}
      {attempts > 1 && (
        <Banner>
          {attempts} attempts · {r.retries} retries · {r.repeats} repeats. Earlier recordings are
          retained separately.
        </Banner>
      )}
      {Array.from({ length: attempts }, (_, index) => {
        const attempt = index + 1
        const entries = sessions.filter(({ result }) => result.attempt === attempt)
        return (
          <Section
            key={attempt}
            title={attempts > 1 ? `Attempt ${attempt}` : 'Recordings in order'}
            aside={
              <Typography.BodySmall>
                {entries.length} recording{entries.length === 1 ? '' : 's'}
              </Typography.BodySmall>
            }
          >
            {entries.length ? (
              <ol className={styles.history}>
                {entries.map(({ path, result }) => (
                  <li key={path}>
                    <Card.Container
                      as="a"
                      interactive
                      href={`${evidencePath(path)}/index.html`}
                      className={styles.sessionLink}
                    >
                      <Typography.CodeSmallInline>
                        {String(result.sequence).padStart(2, '0')}
                      </Typography.CodeSmallInline>
                      <div className={styles.sessionLabel}>
                        <Typography.BodyStrong>{sessionLabel(result)}</Typography.BodyStrong>
                        <Typography.BodySmall as="div">
                          {result.purpose === 'setup' ? 'Setup' : 'Interaction'} ·{' '}
                          {seconds(result.durationMs)} session time
                        </Typography.BodySmall>
                      </div>
                      <Status status={result.status} />
                    </Card.Container>
                  </li>
                ))}
              </ol>
            ) : (
              <Empty>
                No browser recordings for this attempt. Its outcome still belongs to the scenario.
              </Empty>
            )}
          </Section>
        )
      })}
      <Source source={source} path={r.source} />
      <div>
        <EvidenceLink href="scenario.json">Scenario metadata</EvidenceLink>
      </div>
    </>
  )
}
