import { Typography } from '@/wax/components'

import { SourceCatalogSurface } from '@/components/sources'
import type { SourceCatalogEntry, SourceCatalogLoadState } from '@/components/sources'

import { OnboardingLink, OnboardingPage } from './onboarding-page'
import { getOnboardingStepState } from './onboarding-steps'

export interface OnboardingSourcesPageProps {
  continueDisabled?: boolean
  continueLabel?: string
  entries: SourceCatalogEntry[]
  errorMessage?: string | null
  loadState?: SourceCatalogLoadState
  onRetry?: () => void
  onSearchChange: (search: string) => void
  onSourceSelect: (entry: SourceCatalogEntry) => void
  search: string
}

export function OnboardingSourcesPage({
  continueDisabled = false,
  continueLabel = 'I have connected enough sources',
  entries,
  errorMessage = null,
  loadState = 'idle',
  onRetry,
  onSearchChange,
  onSourceSelect,
  search,
}: OnboardingSourcesPageProps) {
  const step = getOnboardingStepState('sources')
  const hasConnectedSource = entries.some((entry) => entry.installed)
  const canContinue = hasConnectedSource && !continueDisabled

  if (!step.nextHref) {
    throw new Error('The onboarding sources step must have a next step')
  }

  return (
    <OnboardingPage
      action={{
        disabled: !canContinue,
        label: continueLabel,
        to: step.nextHref,
      }}
      step={step}
      sideContent={
        <>
          <Typography.BodyLarge>
            Sources turn APIs, services, and local datasets into tables that Coral can query.
          </Typography.BodyLarge>
          <Typography.BodyLarge>
            Coral ships with{' '}
            <OnboardingLink href="https://withcoral.com/docs/reference/bundled-sources">
              core sources
            </OnboardingLink>
            , built and maintained by the Coral team. It is also extensible: import{' '}
            <OnboardingLink href="https://withcoral.com/docs/reference/community-sources">
              community source specs
            </OnboardingLink>{' '}
            or{' '}
            <OnboardingLink href="https://withcoral.com/docs/guides/write-a-custom-source">
              write your own spec
            </OnboardingLink>
            .
          </Typography.BodyLarge>
        </>
      }
      sideTitle="Connect sources to Coral"
    >
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
    </OnboardingPage>
  )
}
