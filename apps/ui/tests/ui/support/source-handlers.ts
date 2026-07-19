import { http } from 'msw'

import {
  CreateBundledSourceRequestSchema,
  CreateBundledSourceResponseSchema,
  CreateBundledSourceWithOAuthRequestSchema,
  CreateBundledSourceWithOAuthResponseSchema,
  DeleteSourceRequestSchema,
  DeleteSourceResponseSchema,
  DiscoverSourcesResponseSchema,
  GetSourceInfoRequestSchema,
  GetSourceInfoResponseSchema,
  GetSourceRequestSchema,
  GetSourceResponseSchema,
  ListSourcesResponseSchema,
} from '../../../src/generated/coral/v1/sources_pb'
import { grpcWebError, grpcWebRequest, grpcWebResponse, grpcWebStreamResponse } from './grpc-web'
import {
  createGithubOauthResponses,
  createLinearResponse,
  deleteSourceResponse,
  discoverGithubOauthResponse,
  discoverAfterLinearInstallResponse,
  discoverAfterLinearRemovedResponse,
  discoverInitialResponse,
  getInfoCloudwatchLogsResponse,
  getInfoGithubResponse,
  getInfoLinearResponse,
  getInstalledBundledGithubResponse,
  getInstalledCloudwatchLogsResponse,
  getInstalledGithubResponse,
  getInstalledLinearResponse,
  listAfterGithubOauthResponse,
  listAfterLinearInstallResponse,
  listAfterLinearRemovedResponse,
  listEmptyResponse,
  listInitialResponse,
} from './source-fixtures'

const discoverUrl = '*/coral.v1.SourceService/DiscoverSources'
const listUrl = '*/coral.v1.SourceService/ListSources'
const getUrl = '*/coral.v1.SourceService/GetSource'
const getInfoUrl = '*/coral.v1.SourceService/GetSourceInfo'
const createBundledUrl = '*/coral.v1.SourceService/CreateBundledSource'
const createBundledWithOAuthUrl = '*/coral.v1.SourceService/CreateBundledSourceWithOAuth'
const deleteUrl = '*/coral.v1.SourceService/DeleteSource'

function sourceInfoResponse(name: string) {
  if (name === 'cloudwatch_logs') return getInfoCloudwatchLogsResponse
  if (name === 'github') return getInfoGithubResponse
  if (name === 'linear') return getInfoLinearResponse
  return null
}

function installedSourceResponse(name: string) {
  if (name === 'linear') return getInstalledLinearResponse
  if (name === 'cloudwatch_logs') return getInstalledCloudwatchLogsResponse
  if (name === 'github') return getInstalledGithubResponse
  return null
}

function expectLinearSecret(value: string, expected: string, action: string) {
  if (value !== expected) {
    throw new Error(`expected ${action} to send LINEAR_API_TOKEN=${expected}, got ${value}`)
  }
}

// Lifecycle handlers track installed state across the full user flow: start
// with `github` installed, install `linear` (paste), edit github's variable,
// remove linear. List and Discover responses advance one step at a time as
// the UI calls Create/Delete.
export function sourceLifecycleHandlers() {
  let listResponse = listInitialResponse
  let discoverResponse = discoverInitialResponse
  let createCalls = 0

  return [
    http.post(discoverUrl, () => grpcWebResponse(DiscoverSourcesResponseSchema, discoverResponse)),
    http.post(listUrl, () => grpcWebResponse(ListSourcesResponseSchema, listResponse)),
    http.post(getInfoUrl, async ({ request }) => {
      const message = await grpcWebRequest(GetSourceInfoRequestSchema, request)
      const response = sourceInfoResponse(message.name)
      if (!response) return grpcWebError(5, `source info ${message.name} not found`)
      return grpcWebResponse(GetSourceInfoResponseSchema, response)
    }),
    http.post(getUrl, async ({ request }) => {
      const message = await grpcWebRequest(GetSourceRequestSchema, request)
      const response = installedSourceResponse(message.name)
      if (!response) return grpcWebError(5, `source ${message.name} not found`)
      return grpcWebResponse(GetSourceResponseSchema, response)
    }),
    http.post(createBundledUrl, async ({ request }) => {
      const message = await grpcWebRequest(CreateBundledSourceRequestSchema, request)
      if (message.name !== 'linear') {
        throw new Error(`expected CreateBundledSource for linear, got ${message.name}`)
      }

      createCalls += 1
      const token = message.secrets.find((secret) => secret.key === 'LINEAR_API_TOKEN')?.value
      if (createCalls === 1) {
        expectLinearSecret(token ?? '', 'lin_test_token', 'install')
      } else if (createCalls === 2) {
        expectLinearSecret(token ?? '', 'lin_test_token_v2', 'edit')
      } else {
        throw new Error(`unexpected CreateBundledSource call ${createCalls}`)
      }

      listResponse = listAfterLinearInstallResponse
      discoverResponse = discoverAfterLinearInstallResponse
      return grpcWebResponse(CreateBundledSourceResponseSchema, createLinearResponse)
    }),
    http.post(deleteUrl, async ({ request }) => {
      const message = await grpcWebRequest(DeleteSourceRequestSchema, request)
      if (message.name !== 'linear') {
        throw new Error(`expected DeleteSource for linear, got ${message.name}`)
      }
      listResponse = listAfterLinearRemovedResponse
      discoverResponse = discoverAfterLinearRemovedResponse
      return grpcWebResponse(DeleteSourceResponseSchema, deleteSourceResponse)
    }),
  ]
}

export function sourceOAuthInstallHandlers() {
  let listResponse = listEmptyResponse
  let discoverResponse = discoverGithubOauthResponse

  return [
    http.post(discoverUrl, () => grpcWebResponse(DiscoverSourcesResponseSchema, discoverResponse)),
    http.post(listUrl, () => grpcWebResponse(ListSourcesResponseSchema, listResponse)),
    http.post(getInfoUrl, async ({ request }) => {
      const message = await grpcWebRequest(GetSourceInfoRequestSchema, request)
      if (message.name !== 'github') return grpcWebError(5, `source info ${message.name} not found`)
      return grpcWebResponse(GetSourceInfoResponseSchema, getInfoGithubResponse)
    }),
    http.post(getUrl, async ({ request }) => {
      const message = await grpcWebRequest(GetSourceRequestSchema, request)
      if (message.name !== 'github') return grpcWebError(5, `source ${message.name} not found`)
      return grpcWebResponse(GetSourceResponseSchema, getInstalledBundledGithubResponse)
    }),
    http.post(createBundledWithOAuthUrl, async ({ request }) => {
      const message = await grpcWebRequest(CreateBundledSourceWithOAuthRequestSchema, request)
      if (message.name !== 'github') {
        throw new Error(`expected CreateBundledSourceWithOAuth for github, got ${message.name}`)
      }
      const retrieval = message.oauthCredentialRetrievals[0]
      if (
        message.oauthCredentialRetrievals.length !== 1 ||
        retrieval?.inputKey !== 'GITHUB_TOKEN' ||
        retrieval.methodIndex !== 0
      ) {
        throw new Error('expected GitHub device-code OAuth retrieval for GITHUB_TOKEN')
      }

      listResponse = listAfterGithubOauthResponse
      discoverResponse = discoverInitialResponse
      return grpcWebStreamResponse(
        CreateBundledSourceWithOAuthResponseSchema,
        createGithubOauthResponses,
        { delayMs: 1500 },
      )
    }),
  ]
}
