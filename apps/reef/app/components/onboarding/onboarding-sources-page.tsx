import { Button, Typography } from '@/wax/components'
import { Pill } from '@/wax/components/pill'

import { SourceCatalogSurface } from '@/components/sources'
import type { SourceCatalogEntry, SourceCatalogLoadState } from '@/components/sources'

import * as styles from './onboarding-sources-page.css'

export interface OnboardingSourcesPageProps {
  continueDisabled?: boolean
  continueLabel?: string
  entries: SourceCatalogEntry[]
  errorMessage?: string | null
  loadState?: SourceCatalogLoadState
  onContinue?: () => void
  onRetry?: () => void
  onSearchChange: (search: string) => void
  onSourceSelect: (entry: SourceCatalogEntry) => void
  search: string
  stepLabel?: string
}

export function OnboardingSourcesPage({
  continueDisabled = false,
  continueLabel = 'I have connected enough sources',
  entries,
  errorMessage = null,
  loadState = 'idle',
  onContinue,
  onRetry,
  onSearchChange,
  onSourceSelect,
  search,
  stepLabel = 'Step 1/3',
}: OnboardingSourcesPageProps) {
  const hasConnectedSource = entries.some((entry) => entry.installed)
  const canContinue = hasConnectedSource && !continueDisabled

  return (
    <section className={styles.root} aria-label="Onboarding">
      <div className={styles.content}>
        <header className={styles.header}>
          <div className={styles.headerText}>
            <div className={styles.titleRow}>
              <Typography.HeadingLarge as="h1">Onboarding</Typography.HeadingLarge>
              <Pill className={styles.stepPill} color="graySubtle">
                {stepLabel}
              </Pill>
            </div>
          </div>
        </header>

        <div className={styles.body}>
          <div className={styles.explainer}>
            <div className={styles.explainerText}>
              <Typography.HeadingSmall>Connect sources to Coral</Typography.HeadingSmall>
              <Typography.BodyLarge>
                Sources turn APIs, services, and local datasets into tables that Coral can query.
              </Typography.BodyLarge>
              <Typography.BodyLarge>
                Coral ships with{' '}
                <a
                  className={styles.inlineLink}
                  href="https://withcoral.com/docs/reference/bundled-sources"
                  rel="noopener noreferrer"
                  target="_blank"
                >
                  core sources
                </a>
                , built and maintained by the Coral team. It is also extensible: import{' '}
                <a
                  className={styles.inlineLink}
                  href="https://withcoral.com/docs/reference/community-sources"
                  rel="noopener noreferrer"
                  target="_blank"
                >
                  community source specs
                </a>{' '}
                or{' '}
                <a
                  className={styles.inlineLink}
                  href="https://withcoral.com/docs/guides/write-a-custom-source"
                  rel="noopener noreferrer"
                  target="_blank"
                >
                  write your own spec
                </a>
                .
              </Typography.BodyLarge>
            </div>

            <Button.Container disabled={!canContinue} onClick={onContinue} variant="secondary">
              <Button.Text>{continueLabel}</Button.Text>
            </Button.Container>
          </div>

          <div className={styles.catalogFrame}>
            <SourceCatalogSurface
              entries={entries}
              errorMessage={errorMessage}
              loadState={loadState}
              onPick={onSourceSelect}
              onRetry={onRetry}
              onSearchChange={onSearchChange}
              search={search}
              showTitle={false}
              variant="compact"
            />
          </div>
        </div>
      </div>
    </section>
  )
}
