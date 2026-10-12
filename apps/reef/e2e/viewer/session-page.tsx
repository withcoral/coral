import { Banner } from '@/wax/components/banner'
import { TextButton } from '@/wax/components/button'
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
  seconds,
  sessionLabel,
} from './shared'
import type { SessionPageProps } from './types'
import * as styles from './reel.css'

export function SessionPage({ result: r, source, hasFailureScreenshot }: SessionPageProps) {
  return (
    <>
      <Breadcrumbs
        items={[
          { label: 'All runs', href: '/' },
          { label: 'Test run', href: '../../../../index.html' },
          { label: r.scenario, href: '../../index.html' },
          { label: `Recording ${r.sequence}` },
        ]}
      />
      <header className={styles.intro}>
        <Typography.BodySmall>
          {r.purpose === 'setup' ? 'Setup recording' : 'Interaction recording'} · attempt{' '}
          {r.attempt} · recording {r.sequence}
        </Typography.BodySmall>
        <Typography.HeadingLarge as="h1" className={styles.heading}>
          {sessionLabel(r)}
        </Typography.HeadingLarge>
        <div className={styles.row}>
          <Status status={r.status} />
          <Typography.BodySmall>{r.target}</Typography.BodySmall>
          <Typography.BodySmall>{seconds(r.durationMs)} session time</Typography.BodySmall>
          <Typography.BodySmall>{date(r.startedAt)}</Typography.BodySmall>
        </div>
        <Typography.BodySmall as="p">
          This is the recording's outcome.{' '}
          <EvidenceLink href="../../index.html">
            See the whole scenario's result and other recordings →
          </EvidenceLink>
        </Typography.BodySmall>
      </header>
      {r.failure && (
        <Failure>
          <Typography.Code as="pre" className={`${styles.code} ${styles.failure}`}>
            {r.failure}
          </Typography.Code>
          {hasFailureScreenshot && (
            <EvidenceLink href="failure.png">Open failure screenshot →</EvidenceLink>
          )}
        </Failure>
      )}
      <nav className={styles.row} aria-label="Evidence">
        <EvidenceLink href="#recording">Recording</EvidenceLink>
        <EvidenceLink href="#steps">Steps ({r.steps.length})</EvidenceLink>
        <EvidenceLink href="#source">Test source</EvidenceLink>
        <EvidenceLink href="#downloads">Downloads</EvidenceLink>
      </nav>
      <Section
        id="recording"
        title="Recording"
        aside={
          <Typography.BodySmall>Browser viewport · not the whole desktop</Typography.BodySmall>
        }
      >
        {r.recording ? (
          <>
            <div hidden data-playback-controls>
              <div className={styles.row} role="group" aria-label="Playback speed">
                <Typography.BodySmall>Playback speed</Typography.BodySmall>
                {[0.1, 0.25, 1].map((rate) => (
                  <TextButton
                    key={rate}
                    variant="secondary"
                    data-playback-rate={rate}
                    aria-pressed={rate === 1}
                  >
                    {rate === 1 ? '1× (normal)' : `${rate}×`}
                  </TextButton>
                ))}
              </div>
            </div>
            <video
              className={styles.media}
              controls
              autoPlay
              muted
              playsInline
              preload="auto"
              aria-label="Scenario recording"
              src={encodeURIComponent(r.recording)}
              poster={r.steps[0] ? encodeURIComponent(r.steps[0].screenshot) : undefined}
            />
          </>
        ) : (
          <Empty>
            No recording available for this session. Check the failure and artifact warnings.
          </Empty>
        )}
      </Section>
      <Section
        id="steps"
        title="Recorded steps"
        aside={<Typography.BodySmall>Select a screenshot to inspect it</Typography.BodySmall>}
      >
        <Typography.BodySmall as="p">
          Screenshots follow successful steps. Times are since session start, not video positions.
        </Typography.BodySmall>
        {r.steps.length ? (
          <ol className={styles.steps} data-testid="step-grid">
            {r.steps.map((step, index) => (
              <li key={index}>
                <Card.Container as="a" interactive href={encodeURIComponent(step.screenshot)}>
                  <img
                    className={styles.screenshot}
                    loading="lazy"
                    src={encodeURIComponent(step.screenshot)}
                    alt={`Screenshot: ${step.label}`}
                  />
                  <div className={styles.caption}>
                    <Typography.CodeSmallInline>
                      {String(index + 1).padStart(2, '0')}
                    </Typography.CodeSmallInline>
                    <div className={styles.intro}>
                      <Typography.BodyStrong>{step.label}</Typography.BodyStrong>
                      <Typography.BodySmall>
                        {seconds(step.elapsedMs)} since session start
                      </Typography.BodySmall>
                    </div>
                  </div>
                </Card.Container>
              </li>
            ))}
          </ol>
        ) : (
          <Empty>No successful steps were recorded.</Empty>
        )}
      </Section>
      <Source source={source} path={r.source} session />
      {r.warnings.length > 0 && (
        <Banner variant="warning" title="Artifact warnings">
          <details>
            <Typography.BodyStrong as="summary" className={styles.summary}>
              Capture and cleanup diagnostics ({r.warnings.length})
            </Typography.BodyStrong>
            <Typography.BodySmall as="p">
              These are separate from the test assertion output.
            </Typography.BodySmall>
            <Typography.Code as="pre" className={styles.code}>
              {r.warnings.join('\n\n')}
            </Typography.Code>
          </details>
        </Banner>
      )}
      <Section id="downloads" title="Downloads & metadata">
        <div className={styles.row}>
          {r.trace && (
            <EvidenceLink href={encodeURIComponent(r.trace)}>
              Download Playwright trace
            </EvidenceLink>
          )}
          {r.recording && (
            <EvidenceLink href={encodeURIComponent(r.recording)} download>
              Download recording
            </EvidenceLink>
          )}
          {source !== undefined && <EvidenceLink href="scenario.ts">Download source</EvidenceLink>}
          <EvidenceLink href="result.json">Session metadata</EvidenceLink>
        </div>
        {r.trace && (
          <Typography.BodySmall as="p">
            Inspect a downloaded trace locally:{' '}
            <Typography.CodeSmallInline as="code">
              npx playwright show-trace path/to/trace.zip
            </Typography.CodeSmallInline>
          </Typography.BodySmall>
        )}
      </Section>
    </>
  )
}
