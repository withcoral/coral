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
}
