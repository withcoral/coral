import type { Route } from './+types/trace-detail'

import { TraceDetail } from '@/views/traces/trace-detail'

export { loader } from './trace-detail-loader'

export default function TraceDetailRoute({ loaderData, params }: Route.ComponentProps) {
  return (
    <TraceDetail
      detail={loaderData.detail}
      loadError={loaderData.loadError}
      traceId={params.traceId}
    />
  )
}
