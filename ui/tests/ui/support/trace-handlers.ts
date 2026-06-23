import { http } from 'msw'

import {
  GetTraceResponseSchema,
  ListTracesResponseSchema,
} from '../../../src/generated/coral/v1/traces_pb'
import { grpcWebError, grpcWebResponse } from './grpc-web'
import {
  emptyTraceListResponse,
  selectedTraceDetailResponse,
  traceListResponse,
} from './trace-fixtures'

const listTracesUrl = '*/coral.v1.TraceService/ListTraces'
const getTraceUrl = '*/coral.v1.TraceService/GetTrace'

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export const traceHandlers = {
  empty: [
    http.post(listTracesUrl, () =>
      grpcWebResponse(ListTracesResponseSchema, emptyTraceListResponse),
    ),
  ],
  unavailable: [http.post(listTracesUrl, () => grpcWebError(12, 'Trace storage is not enabled'))],
  tenTraceDetailFlow: [
    http.post(listTracesUrl, () => grpcWebResponse(ListTracesResponseSchema, traceListResponse)),
    http.post(getTraceUrl, () =>
      grpcWebResponse(GetTraceResponseSchema, selectedTraceDetailResponse),
    ),
  ],
  delayedTraceDetailFlow: [
    http.post(listTracesUrl, () => grpcWebResponse(ListTracesResponseSchema, traceListResponse)),
    http.post(getTraceUrl, async () => {
      await delay(650)
      return grpcWebResponse(GetTraceResponseSchema, selectedTraceDetailResponse)
    }),
  ],
  delayedTraceList: [
    http.post(listTracesUrl, async () => {
      await delay(650)
      return grpcWebResponse(ListTracesResponseSchema, traceListResponse)
    }),
    http.post(getTraceUrl, () =>
      grpcWebResponse(GetTraceResponseSchema, selectedTraceDetailResponse),
    ),
  ],
  tracesThenUnavailable: (() => {
    let listCalls = 0
    return [
      http.post(listTracesUrl, () => {
        listCalls += 1
        if (listCalls === 1) return grpcWebResponse(ListTracesResponseSchema, traceListResponse)
        return grpcWebError(12, 'Trace storage is not enabled')
      }),
    ]
  })(),
}
