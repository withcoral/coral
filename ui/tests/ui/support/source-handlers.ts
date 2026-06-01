import { http } from 'msw'

import {
  CreateBundledSourceResponseSchema,
  DeleteSourceResponseSchema,
  DiscoverSourcesResponseSchema,
  GetSourceInfoResponseSchema,
  GetSourceResponseSchema,
  ListSourcesResponseSchema,
} from '../../../src/generated/coral/v1/sources_pb'
import { grpcWebResponse } from './grpc-web'
import {
  createLinearResponse,
  deleteSourceResponse,
  discoverAfterLinearInstallResponse,
  discoverAfterLinearRemovedResponse,
  discoverInitialResponse,
  getInfoCloudwatchLogsResponse,
  getInfoGithubResponse,
  getInfoLinearResponse,
  getInstalledCloudwatchLogsResponse,
  getInstalledGithubResponse,
  getInstalledLinearResponse,
  listAfterLinearInstallResponse,
  listAfterLinearRemovedResponse,
  listInitialResponse,
} from './source-fixtures'

const discoverUrl = '*/coral.v1.SourceService/DiscoverSources'
const listUrl = '*/coral.v1.SourceService/ListSources'
const getUrl = '*/coral.v1.SourceService/GetSource'
const getInfoUrl = '*/coral.v1.SourceService/GetSourceInfo'
const createBundledUrl = '*/coral.v1.SourceService/CreateBundledSource'
const deleteUrl = '*/coral.v1.SourceService/DeleteSource'

// Lifecycle handlers track installed state across the full user flow: start
// with `github` installed, install `linear` (paste), edit github's variable,
// remove linear. List and Discover responses advance one step at a time as
// the UI calls Create/Delete.
export function sourceLifecycleHandlers() {
  let listResponse = listInitialResponse
  let discoverResponse = discoverInitialResponse

  return [
    http.post(discoverUrl, () => grpcWebResponse(DiscoverSourcesResponseSchema, discoverResponse)),
    http.post(listUrl, () => grpcWebResponse(ListSourcesResponseSchema, listResponse)),
    http.post(getInfoUrl, async ({ request }) => {
      const body = new TextDecoder().decode(await request.arrayBuffer())
      const response = body.includes('cloudwatch_logs')
        ? getInfoCloudwatchLogsResponse
        : body.includes('github')
          ? getInfoGithubResponse
          : getInfoLinearResponse
      return grpcWebResponse(GetSourceInfoResponseSchema, response)
    }),
    http.post(getUrl, async ({ request }) => {
      const body = new TextDecoder().decode(await request.arrayBuffer())
      const response = body.includes('linear')
        ? getInstalledLinearResponse
        : body.includes('cloudwatch_logs')
          ? getInstalledCloudwatchLogsResponse
          : getInstalledGithubResponse
      return grpcWebResponse(GetSourceResponseSchema, response)
    }),
    http.post(createBundledUrl, () => {
      listResponse = listAfterLinearInstallResponse
      discoverResponse = discoverAfterLinearInstallResponse
      return grpcWebResponse(CreateBundledSourceResponseSchema, createLinearResponse)
    }),
    http.post(deleteUrl, () => {
      listResponse = listAfterLinearRemovedResponse
      discoverResponse = discoverAfterLinearRemovedResponse
      return grpcWebResponse(DeleteSourceResponseSchema, deleteSourceResponse)
    }),
  ]
}
