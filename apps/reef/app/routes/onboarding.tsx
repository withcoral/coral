import { create } from '@bufbuild/protobuf'
import { useCallback, useEffect, useRef, useState } from 'react'
import { useNavigate, useNavigation, useRevalidator, useSearchParams } from 'react-router'

import type { Route } from './+types/onboarding'
import type { SourcesActionData } from './sources-action'

import { OnboardingSampleQueryPage } from '@/components/onboarding/onboarding-sample-query-page'
import type { SampleQueryLoadState } from '@/components/onboarding/onboarding-sample-query-page'
import { OnboardingSourcesPage } from '@/components/onboarding/onboarding-sources-page'
import { ExecuteSqlRequestSchema } from '@/generated/coral/v1/query_pb'
import { WorkspaceSchema } from '@/generated/coral/v1/resources_pb'
import { getQueryClient } from '@/lib/coral-clients'
import {
  decodeOnboardingSampleQueryRows,
  type OnboardingSampleQueryRow,
} from '@/lib/onboarding-query'
import type { CatalogEntry } from '@/lib/sources'
import { errorMessage } from '@/lib/utils'
import { firstWorkspaceForRequest } from '@/lib/workspaces.server'
import { routePath } from '@/routing/routemap'
import { SourceDetailDialog } from '@/views/sources/source-detail'
import { SourceInstallDialog } from '@/views/sources/source-install'

import { runSourcesAction } from './sources-action'
import { loadSourcesRouteData } from './sources-loader'

export async function loader({ request }: Route.LoaderArgs) {
  const workspace = await firstWorkspaceForRequest(request)
  return {
    ...(await loadSourcesRouteData(request, workspace)),
    workspaceId: workspace.name,
  }
}

export async function action({ request }: Route.ActionArgs): Promise<SourcesActionData> {
  return runSourcesAction(request, await firstWorkspaceForRequest(request))
}

export default function OnboardingRoute({ actionData, loaderData }: Route.ComponentProps) {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const step = searchParams.get('step') === 'query' ? 'query' : 'sources'

  if (step === 'query') {
    return (
      <SampleQueryStep
        entries={loaderData.entries}
        loadError={loaderData.loadError}
        onComplete={() =>
          navigate(routePath('workspaceSources', { workspaceId: loaderData.workspaceId }))
        }
        workspaceId={loaderData.workspaceId}
      />
    )
  }

  return (
    <SourcesStep
      actionData={actionData}
      entries={loaderData.entries}
      loadError={loaderData.loadError}
    />
  )
}

export function SourcesStep({
  actionData,
  entries,
  loadError,
}: {
  actionData: SourcesActionData
  entries: CatalogEntry[]
  loadError: string | null
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
          onOpenChange={(open) => {
            if (!open) closeSelectedEntry()
          }}
          submitting={pendingIntent === 'install' && pendingName === selectedEntry?.name}
        />
      )}
    </>
  )
}

function SampleQueryStep({
  entries,
  loadError,
  onComplete,
  workspaceId,
}: {
  entries: CatalogEntry[]
  loadError: string | null
  onComplete: () => void
  workspaceId: string
}) {
  const connectedSources = entries.filter((entry) => entry.installed)
  const [loadState, setLoadState] = useState<SampleQueryLoadState>('idle')
  const [rows, setRows] = useState<OnboardingSampleQueryRow[]>([])
  const [queryError, setQueryError] = useState<string | null>(null)
  const runSequence = useRef(0)
  const revalidator = useRevalidator()

  const runSampleQuery = useCallback(async (sql: string) => {
    const sequence = ++runSequence.current
    setLoadState('loading')
    setQueryError(null)

    try {
      const queryClient = await getQueryClient()
      const response = await queryClient.executeSql(
        create(ExecuteSqlRequestSchema, {
          sql,
          workspace: create(WorkspaceSchema, { name: workspaceId }),
        }),
      )
      const nextRows = decodeOnboardingSampleQueryRows(response.arrowIpcStream)
      if (sequence !== runSequence.current) return
      setRows(nextRows)
      setLoadState('success')
    } catch (error) {
      if (sequence !== runSequence.current) return
      setRows([])
      setQueryError(errorMessage(error))
      setLoadState('error')
    }
  }, [workspaceId])

  return (
    <OnboardingSampleQueryPage
      connectedSources={connectedSources}
      errorMessage={loadError ?? queryError}
      errorTitle={loadError ? "Couldn't load sources" : undefined}
      loadState={loadError ? 'error' : loadState}
      onContinue={onComplete}
      onRetry={loadError ? () => revalidator.revalidate() : undefined}
      onRunSampleQuery={runSampleQuery}
      rows={rows}
    />
  )
}
