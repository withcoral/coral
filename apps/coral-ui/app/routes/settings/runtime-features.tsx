import { create } from '@bufbuild/protobuf'
import { Code, ConnectError } from '@connectrpc/connect'
import { useFetcher, useFetchers } from 'react-router'

import type { Route } from './+types/runtime-features'

import { requestAuthContext } from '@/auth/server-context'
import {
  RuntimeFeatureRow,
  RuntimeFeaturesList,
  type RuntimeFeatureListItem,
} from '@/components/runtime-features-list'
import {
  ListFeaturesRequestSchema,
  SetFeatureRequestSchema,
  type FeatureStatus,
} from '@/generated/coral/v1/features_pb'
import { isCoralDesktopBuild } from '@/lib/coral-desktop'
import { featureClientForRequest } from '@/lib/coral-request.server'
import { errorMessage } from '@/lib/utils'
import { Banner, Typography } from '@/wax/components'

import { SettingsPage } from '@/views/settings/settings-page'
import * as styles from '@/views/settings/settings.css'

// Coral cannot restart itself, and how you restart it depends on what is running
// it. The Desktop shell owns its own sidecar; everything else is supervised by
// whatever started the server.
const RESTART_MESSAGE = isCoralDesktopBuild()
  ? 'Coral is running with different settings. Quit and reopen Coral to apply your changes.'
  : 'Coral is running with different settings. Restart the Coral server to apply your changes.'

// Features belong to the machine running Coral, not to any workspace, so a
// shared deployment lets everyone read the state and nobody change it. The
// server is the only place that knows which of the two this is, and it says so
// by refusing the write.
const HOST_MANAGED_MESSAGE =
  'Runtime features are configured on the host that runs this Coral server. You can see what is on here, but changing it has to happen on the host.'

export interface RuntimeFeaturesRouteData {
  features: RuntimeFeatureListItem[]
  loadError: string | null
  /** Whether Coral is running with a feature state its config no longer matches. */
  restartPending: boolean
}

export type RuntimeFeaturesActionData =
  | { key: string; message: string; status: 'error' }
  | { key: string; status: 'host-managed' }
  | { key: string; status: 'success' }

export async function action({
  context,
  request,
}: Route.ActionArgs): Promise<RuntimeFeaturesActionData> {
  const formData = await request.formData()
  const keyValue = formData.get('key')
  const key = typeof keyValue === 'string' ? keyValue : ''
  if (!key) return { key, message: 'Missing feature key', status: 'error' }

  try {
    await featureClientForRequest(request, context.get(requestAuthContext).accessToken).setFeature(
      create(SetFeatureRequestSchema, { enabled: formData.get('enabled') === 'true', key }),
      { signal: request.signal },
    )
    return { key, status: 'success' }
  } catch (error) {
    // A refused write is not a failure to report row by row: it says this
    // whole page is a read-only view of somebody else's machine.
    if (error instanceof ConnectError && error.code === Code.PermissionDenied) {
      return { key, status: 'host-managed' }
    }
    return { key, message: errorMessage(error), status: 'error' }
  }
}

export async function loader({
  context,
  request,
}: Route.LoaderArgs): Promise<RuntimeFeaturesRouteData> {
  try {
    const response = await featureClientForRequest(
      request,
      context.get(requestAuthContext).accessToken,
    ).listFeatures(create(ListFeaturesRequestSchema, {}), { signal: request.signal })
    return {
      features: response.features.map(toRuntimeFeature),
      loadError: null,
      // Coral resolves features once, at startup, so a feature whose configured
      // state has moved away from the state this server booted with is the exact
      // set that needs a restart. Nothing else has to guess.
      restartPending: response.features.some((feature) => feature.enabled !== feature.active),
    }
  } catch (error) {
    return { features: [], loadError: errorMessage(error), restartPending: false }
  }
}

export default function RuntimeFeaturesRoute({ loaderData }: Route.ComponentProps) {
  const { features, loadError, restartPending } = loaderData
  const hostManaged = useHostManaged()

  return (
    <SettingsPage
      header={
        <div className={styles.headerText}>
          <Typography.HeadingLarge as="h1">Features</Typography.HeadingLarge>
          <Typography.Body variant="secondary">
            {hostManaged
              ? 'Experimental Coral capabilities this server is running with.'
              : 'Turn experimental Coral capabilities on or off for this machine. Changes are saved right away and apply the next time Coral starts.'}
          </Typography.Body>
        </div>
      }
    >
      {hostManaged && <Banner>{HOST_MANAGED_MESSAGE}</Banner>}

      <RuntimeFeaturesList
        error={loadError ?? undefined}
        features={features}
        renderRow={(feature) => (
          <RuntimeFeatureToggleRow feature={feature} readOnly={hostManaged} />
        )}
      />

      {/* Below the list, so appearing after a toggle grows into empty page space
          instead of pushing the switch the reader just used. */}
      {restartPending && <Banner>{RESTART_MESSAGE}</Banner>}
    </SettingsPage>
  )
}

// Every row writes through its own fetcher. React Router aborts the request in
// flight when the same fetcher submits again, so one fetcher for the whole page
// would drop the first toggle when a reader moves two switches in a row.
function RuntimeFeatureToggleRow({
  feature,
  readOnly,
}: {
  feature: RuntimeFeatureListItem
  readOnly: boolean
}) {
  const fetcher = useFetcher<RuntimeFeaturesActionData>({ key: fetcherKey(feature.key) })
  // The switch has to move on click, but the value it settles on comes from the
  // server. Read the in-flight submission so the row does not snap back while
  // the action and its revalidation are still running. React Router clears the
  // form data once the fetcher is idle, which returns a failed write to the
  // value config still holds.
  const submitted = fetcher.formData
  const enabled = submitted ? submitted.get('enabled') === 'true' : feature.enabled

  return (
    <RuntimeFeatureRow
      error={fetcher.data?.status === 'error' ? fetcher.data.message : undefined}
      feature={{ ...feature, enabled }}
      onToggle={(next) =>
        fetcher.submit({ enabled: String(next), key: feature.key }, { method: 'post' })
      }
      pending={fetcher.state !== 'idle'}
      readOnly={readOnly}
    />
  )
}

// One refused write settles the whole page: the server refuses on who is
// asking, not on which feature was asked for. Every row writes through its own
// fetcher, so the answer is read back out of whichever row was toggled.
function useHostManaged(): boolean {
  return useFetchers().some(
    (fetcher) =>
      fetcher.key.startsWith(FETCHER_KEY_PREFIX) &&
      (fetcher.data as RuntimeFeaturesActionData | undefined)?.status === 'host-managed',
  )
}

// Fetcher keys are global to the app, so the feature key alone is not enough to
// keep this page's writes apart from anyone else's.
const FETCHER_KEY_PREFIX = 'runtime-feature:'

function fetcherKey(featureKey: string): string {
  return `${FETCHER_KEY_PREFIX}${featureKey}`
}

export function toRuntimeFeature(feature: FeatureStatus): RuntimeFeatureListItem {
  return {
    description: feature.description,
    enabled: feature.enabled,
    key: feature.key,
    label: featureLabel(feature.key),
  }
}

// Feature keys are the stable config contract; the page shows a readable form of
// the same key rather than a second name the CLI would not recognize.
function featureLabel(key: string): string {
  const words = key.replaceAll('_', ' ')
  return words.charAt(0).toUpperCase() + words.slice(1)
}
