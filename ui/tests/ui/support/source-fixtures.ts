import { create } from '@bufbuild/protobuf'

import {
  CreateBundledSourceResponseSchema,
  DeleteSourceResponseSchema,
  DiscoverSourcesResponseSchema,
  GetSourceInfoResponseSchema,
  GetSourceResponseSchema,
  ListSourcesResponseSchema,
  SourceCredentialStorage,
  SourceInfoSchema,
  SourceInputSpecSchema,
  SourceOrigin,
  SourceSchema,
  SourceSecretInputSchema,
  SourceSecretSchema,
  SourceVariableInputSchema,
  SourceVariableSchema,
  type Source,
  type SourceInfo,
} from '../../../src/generated/coral/v1/sources_pb'

function makeSourceInfo(name: string, description: string, installed: boolean): SourceInfo {
  return create(SourceInfoSchema, {
    name,
    description,
    version: '1.0.0',
    installed,
    origin: SourceOrigin.BUNDLED,
    credentialStorage: SourceCredentialStorage.FILE,
    inputs: [
      create(SourceInputSpecSchema, {
        key: `${name.toUpperCase()}_API_TOKEN`,
        required: true,
        hint: `API token for ${name}.`,
        input: {
          case: 'secret',
          value: create(SourceSecretInputSchema, {}),
        },
      }),
    ],
  })
}

function makeBundledSourceWithVariable(
  name: string,
  description: string,
  variableKey: string,
  variableDefault: string,
  installed: boolean,
): SourceInfo {
  return create(SourceInfoSchema, {
    name,
    description,
    version: '1.1.6',
    installed,
    origin: SourceOrigin.BUNDLED,
    credentialStorage: SourceCredentialStorage.FILE,
    inputs: [
      create(SourceInputSpecSchema, {
        key: variableKey,
        required: false,
        hint: `Override the default ${variableKey}.`,
        input: {
          case: 'variable',
          value: create(SourceVariableInputSchema, { defaultValue: variableDefault }),
        },
      }),
      create(SourceInputSpecSchema, {
        key: `${name.toUpperCase()}_TOKEN`,
        required: true,
        hint: `Personal access token for ${name}.`,
        input: {
          case: 'secret',
          value: create(SourceSecretInputSchema, {}),
        },
      }),
    ],
  })
}

const githubInfo = makeBundledSourceWithVariable(
  'github',
  'Query repositories, issues, and pull requests from GitHub.',
  'GITHUB_API_BASE',
  'https://api.github.com',
  true,
)

const linearInfo = makeSourceInfo(
  'linear',
  'Query issues, projects, cycles, teams, and users from Linear.',
  false,
)

const slackInfo = makeSourceInfo(
  'slack',
  'Query channels, messages, thread replies, and users from your Slack workspace.',
  false,
)

const sentryInfo = makeSourceInfo(
  'sentry',
  'Query issues, events, projects, releases, deployments, teams, and members from Sentry.',
  false,
)

export const bundledCatalog: SourceInfo[] = [githubInfo, linearInfo, slackInfo, sentryInfo]

const installedGithub: Source = create(SourceSchema, {
  name: 'github',
  version: '1.1.6',
  origin: SourceOrigin.BUNDLED,
  credentialStorage: SourceCredentialStorage.FILE,
  variables: [
    create(SourceVariableSchema, { key: 'GITHUB_API_BASE', value: 'https://api.github.com' }),
  ],
  secrets: [create(SourceSecretSchema, { key: 'GITHUB_TOKEN', value: '' })],
})

const installedLinear: Source = create(SourceSchema, {
  name: 'linear',
  version: '1.0.0',
  origin: SourceOrigin.BUNDLED,
  credentialStorage: SourceCredentialStorage.FILE,
  variables: [],
  secrets: [create(SourceSecretSchema, { key: 'LINEAR_API_TOKEN', value: '' })],
})

export const initialInstalledSources: Source[] = [installedGithub]

export const discoverInitialResponse = create(DiscoverSourcesResponseSchema, {
  sources: bundledCatalog,
})

export const discoverAfterLinearInstallResponse = create(DiscoverSourcesResponseSchema, {
  sources: bundledCatalog.map((info) =>
    info.name === 'linear' ? { ...info, installed: true } : info,
  ),
})

export const discoverAfterLinearRemovedResponse = discoverInitialResponse

export const listInitialResponse = create(ListSourcesResponseSchema, {
  sources: initialInstalledSources,
})

export const listAfterLinearInstallResponse = create(ListSourcesResponseSchema, {
  sources: [...initialInstalledSources, installedLinear],
})

export const listAfterLinearRemovedResponse = listInitialResponse

export const getInfoLinearResponse = create(GetSourceInfoResponseSchema, { sourceInfo: linearInfo })
export const getInfoGithubResponse = create(GetSourceInfoResponseSchema, { sourceInfo: githubInfo })

export const getInstalledGithubResponse = create(GetSourceResponseSchema, {
  source: installedGithub,
})
export const getInstalledLinearResponse = create(GetSourceResponseSchema, {
  source: installedLinear,
})

export const createLinearResponse = create(CreateBundledSourceResponseSchema, {
  source: installedLinear,
})

export const deleteSourceResponse = create(DeleteSourceResponseSchema, {})
