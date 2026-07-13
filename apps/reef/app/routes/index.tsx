import type { Route } from './+types/index'

import { SourcesIndex } from '@/views/sources/sources-index'

export { action } from './sources-action'
export { loader } from './sources-loader'

export default function AppIndex({ loaderData }: Route.ComponentProps) {
  return <SourcesIndex entries={loaderData.entries} loadError={loaderData.loadError} />
}
