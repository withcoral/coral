import { Suspense, useEffect, useState } from 'react'
import { Await, useNavigate, useNavigation, useRevalidator, useSubmit } from 'react-router'

import type { SourcesActionData } from '@/routes/sources-action'

import type { McpClientsConnectionState } from '@/components/mcp-clients-list'
import { OnboardingNextStepsPage } from '@/components/onboarding/onboarding-next-steps-page'
import { OnboardingSampleQueryPage } from '@/components/onboarding/onboarding-sample-query-page'
import type { SampleQueryLoadState } from '@/components/onboarding/onboarding-sample-query-page'
import { OnboardingSourcesPage } from '@/components/onboarding/onboarding-sources-page'
import type { OnboardingStepState } from '@/components/onboarding/onboarding-steps'
import type { CompleteGuiOnboardingError } from '@/lib/gui-onboarding'
import type { OnboardingSampleQueryResult } from '@/lib/onboarding-query'
import type { CatalogEntry } from '@/lib/sources'
import { SourceDetailDialog } from '@/views/sources/source-detail'
import { SourceInstallDialog } from '@/views/sources/source-install'
import { ToastContainer } from '@/wax/components/toast'

export function OnboardingView({
  actionData,
  loaderData,
  mcpClients,
}: {
  actionData: CompleteGuiOnboardingError | SourcesActionData
  loaderData: {
    entries: CatalogEntry[]
    loadError: string | null
    runtime: 'desktop' | 'web'
    sampleQuery: OnboardingSampleQueryResult | Promise<OnboardingSampleQueryResult> | null
    step: OnboardingStepState
    workspaceId: string
    workspaces: ReadonlyArray<{ name: string }>
  }
  mcpClients: McpClientsConnectionState
}) {
  const navigate = useNavigate()
  const navigation = useNavigation()
  const submit = useSubmit()
  const { step } = loaderData
  const completing =
    navigation.state !== 'idle' && navigation.formData?.get('intent') === 'complete-onboarding'
  const completionError =
    !completing && actionData?.intent === 'complete-onboarding' ? actionData.message : null
  const sourcesActionData = actionData?.intent === 'complete-onboarding' ? undefined : actionData

  switch (step.step) {
    case 'sources':
      return (
        <SourcesStep
          actionData={sourcesActionData}
          entries={loaderData.entries}
          loadError={loaderData.loadError}
          step={step}
          workspaceId={loaderData.workspaceId}
        />
      )
    case 'query': {
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
    case 'next-steps':
      return (
        <OnboardingNextStepsStep
          completionError={completionError}
          completing={completing}
          mcpClients={mcpClients}
          onContinue={() =>
            submit({ intent: 'complete-onboarding' }, { method: 'post', replace: true })
          }
          runtime={loaderData.runtime}
          step={step}
          workspaces={loaderData.workspaces}
        />
      )
    default: {
      const exhaustive: never = step.step
      return exhaustive
    }
  }
}

function OnboardingNextStepsStep({
  completionError,
  completing,
  mcpClients,
  onContinue,
  runtime,
  step,
  workspaces,
}: {
  completionError: string | null
  completing: boolean
  mcpClients: McpClientsConnectionState
  onContinue: () => void
  runtime: 'desktop' | 'web'
  step: OnboardingStepState
  workspaces: ReadonlyArray<{ name: string }>
}) {
  return (
    <>
      {runtime === 'desktop' ? (
        <OnboardingNextStepsPage
          completionError={completionError}
          completing={completing}
          mcpClients={mcpClients}
          onContinue={onContinue}
          runtime="desktop"
          step={step}
          workspaces={workspaces}
        />
      ) : (
        <OnboardingNextStepsPage
          completionError={completionError}
          completing={completing}
          onContinue={onContinue}
          runtime="web"
          step={step}
        />
      )}
      {/*
        Onboarding renders outside the app shell, which is where the rest of the app
        mounts its toast host. Without one here, connecting a client would silently
        drop both its confirmation and its failures.
      */}
      <ToastContainer />
    </>
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
