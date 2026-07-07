import { useRevalidator } from 'react-router'

import { fetchSchemaFromCoral } from '@/lib/schema-explorer'
import {
  SchemaExplorer,
  SchemaExplorerError,
  SchemaExplorerSkeleton,
} from '@/views/schema-explorer'

import type { Route } from './+types/schema'

// Desktop is SPA-only (no server), and the catalog is fetched over gRPC-web via
// the browser transport + desktop bridge, so this must be a clientLoader.
export async function clientLoader(_args: Route.ClientLoaderArgs) {
  return { schema: await fetchSchemaFromCoral() }
}

// No server loader, so hydrate the initial load from the client.
clientLoader.hydrate = true as const

export function HydrateFallback() {
  return <SchemaExplorerSkeleton />
}

export default function SchemaRoute({ loaderData }: Route.ComponentProps) {
  return <SchemaExplorer schema={loaderData.schema} />
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  const { revalidate } = useRevalidator()
  const message = error instanceof Error ? error.message : String(error)
  return <SchemaExplorerError message={message} onRetry={() => void revalidate()} />
}
