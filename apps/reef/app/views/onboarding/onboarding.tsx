import { Suspense, useEffect, useState } from 'react'
import { Await, useNavigate, useNavigation, useRevalidator } from 'react-router'

import type { SourcesActionData } from '@/routes/sources-action'

import { OnboardingNextStepsPage } from '@/components/onboarding/onboarding-next-steps-page'
import type { McpLaunchConfigState } from '@/components/onboarding/onboarding-next-steps-page'
import { OnboardingSampleQueryPage } from '@/components/onboarding/onboarding-sample-query-page'
import type { SampleQueryLoadState } from '@/components/onboarding/onboarding-sample-query-page'
import { OnboardingSourcesPage } from '@/components/onboarding/onboarding-sources-page'
import type { OnboardingStepState } from '@/components/onboarding/onboarding-steps'
import { coralDesktopApi, desktopErrorMessage } from '@/lib/coral-desktop'
import type { OnboardingSampleQueryResult } from '@/lib/onboarding-query'
import type { CatalogEntry } from '@/lib/sources'
import { routePath } from '@/routing/routemap'
import { SourceDetailDialog } from '@/views/sources/source-detail'
import { SourceInstallDialog } from '@/views/sources/source-install'

export function OnboardingView({
  actionData,
  loaderData,
}: {
  actionData: SourcesActionData
  loaderData: {
    entries: CatalogEntry[]
    loadError: string | null
    sampleQuery: OnboardingSampleQueryResult | Promise<OnboardingSampleQueryResult> | null
    step: OnboardingStepState
    workspaceId: string
  }
}) {
  const navigate = useNavigate()
  const { step } = loaderData

  if (step.step === 'next-steps') {
    return (
      <OnboardingNextStepsStep
        onContinue={() =>
          navigate(routePath('workspaceSources', { workspaceId: loaderData.workspaceId }))
        }
        step={step}
      />
    )
  }

  if (step.step === 'query') {
    const { nextHref } = step
    if (!nextHref) {
      throw new Error('The onboarding query step must have a next step')
    }

    const sampleQueryProps = {
      entries: loaderData.entries,
      loadError: loaderData.loadError,
      onComplete: () => navigate(nextHref),
      step,
    }

    if (!loaderData.sampleQuery) {
      return <SampleQueryStep {...sampleQueryProps} queryResult={null} />
    }

    return (
      <Suspense fallback={<SampleQueryStep {...sampleQueryProps} pending queryResult={null} />}>
        <Await
          errorElement={
            <SampleQueryStep
              {...sampleQueryProps}
              queryResult={{
                message: "The sample query couldn't be completed. Try again.",
                status: 'error',
              }}
            />
          }
          resolve={loaderData.sampleQuery}
        >
          {(queryResult) => <SampleQueryStep {...sampleQueryProps} queryResult={queryResult} />}
        </Await>
      </Suspense>
    )
  }

  return (
    <SourcesStep
      actionData={actionData}
      entries={loaderData.entries}
      loadError={loaderData.loadError}
      step={step}
      workspaceId={loaderData.workspaceId}
    />
  )
}

function OnboardingNextStepsStep({
  onContinue,
  step,
}: {
  onContinue: () => void
  step: OnboardingStepState
}) {
  const [mcpLaunchConfig, setMcpLaunchConfig] = useState<McpLaunchConfigState>({
    status: 'loading',
  })

  useEffect(() => {
    const desktop = coralDesktopApi()
    if (!desktop) {
      setMcpLaunchConfig({ status: 'unavailable' })
      return
    }

    let cancelled = false
    desktop
      .getMcpLaunchConfig()
      .then((config) => {
        if (!cancelled) setMcpLaunchConfig({ config, status: 'success' })
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setMcpLaunchConfig({ message: desktopErrorMessage(error), status: 'error' })
        }
      })

    return () => {
      cancelled = true
    }
  }, [])

  return (
    <OnboardingNextStepsPage
      mcpLaunchConfig={mcpLaunchConfig}
      onContinue={onContinue}
      step={step}
    />
  )
}

export function SourcesStep({
  actionData,
  entries,
  loadError,
  step,
  workspaceId,
}: {
  actionData: SourcesActionData
  entries: CatalogEntry[]
  loadError: string | null
  step: OnboardingStepState
  workspaceId: string
}) {
  const [search, setSearch] = useState('')
  const [selectedEntry, setSelectedEntry] = useState<CatalogEntry | null>(null)
  const [dismissedActionData, setDismissedActionData] = useState<SourcesActionData>(undefined)
  const navigation = useNavigation()
  const revalidator = useRevalidator()
  const pendingIntent = navigation.formData?.get('_intent')
  const pendingName = navigation.formData?.get('name')
  const dialogActionData = actionData === dismissedActionData ? undefined : actionData
  const actionError = dialogActionData?.status === 'error' ? dialogActionData : null

  const closeSelectedEntry = () => {
    if (actionData?.status === 'error') setDismissedActionData(actionData)
    setSelectedEntry(null)
  }

  useEffect(() => {
    if (actionData?.status !== 'success') return
    setSelectedEntry((selected) => (selected?.name === actionData.name ? null : selected))
  }, [actionData])

  return (
    <>
      <OnboardingSourcesPage
        entries={entries}
        errorMessage={loadError}
        loadState={loadError ? 'error' : revalidator.state === 'loading' ? 'loading' : 'idle'}
        onRetry={() => revalidator.revalidate()}
        onSearchChange={setSearch}
        onSourceSelect={setSelectedEntry}
        search={search}
        step={step}
      />
      {selectedEntry?.installed ? (
        <SourceDetailDialog
          actionData={dialogActionData}
          entry={selectedEntry}
          loadError={null}
          open
          onOpenChange={(open) => {
            if (!open) closeSelectedEntry()
          }}
        />
      ) : (
        <SourceInstallDialog
          actionError={
            actionError &&
            actionError.intent === 'install' &&
            actionError.name === selectedEntry?.name
              ? actionError.message
              : null
          }
          entry={selectedEntry}
          open={selectedEntry !== null}
          onOAuthInstallComplete={closeSelectedEntry}
          onOpenChange={(open) => {
            if (!open) closeSelectedEntry()
          }}
          submitting={pendingIntent === 'install' && pendingName === selectedEntry?.name}
          workspaceId={workspaceId}
        />
      )}
    </>
  )
}

function SampleQueryStep({
  entries,
  loadError,
  onComplete,
  pending = false,
  queryResult,
  step,
}: {
  entries: CatalogEntry[]
  loadError: string | null
  onComplete: () => void
  pending?: boolean
  queryResult: OnboardingSampleQueryResult | null
  step: OnboardingStepState
}) {
  const connectedSources = entries.filter((entry) => entry.installed)
  const revalidator = useRevalidator()
  const queryLoadState: SampleQueryLoadState =
    pending || revalidator.state === 'loading' ? 'loading' : (queryResult?.status ?? 'idle')

  return (
    <OnboardingSampleQueryPage
      connectedSources={connectedSources}
      errorMessage={
        loadError ?? (queryResult?.status === 'error' ? queryResult.message : undefined)
      }
      errorTitle={loadError ? "Couldn't load sources" : undefined}
      loadState={loadError ? 'error' : queryLoadState}
      onContinue={onComplete}
      onRetry={() => revalidator.revalidate()}
      rows={queryResult?.status === 'success' ? queryResult.rows : []}
      step={step}
    />
  )
}
