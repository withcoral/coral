import { create } from '@bufbuild/protobuf'

import {
  CreateBundledSourceResponseSchema,
  CreateBundledSourceWithOAuthResponseSchema,
  DeleteSourceResponseSchema,
  DiscoverSourcesResponseSchema,
  GetSourceInfoResponseSchema,
  GetSourceResponseSchema,
  ListSourcesResponseSchema,
  OAuthCredentialClientIdSchema,
  OAuthCredentialClientSchema,
  OAuthCredentialAuthorizationSchema,
  OAuthCredentialCompletedSchema,
  OAuthCredentialEndpointsSchema,
  OAuthCredentialMethodSchema,
  OauthCredentialFlowType,
  SourceConfigCredentialMethodSchema,
  SourceCredentialMethodSchema,
  SourceCredentialSchema,
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

const githubInfo = create(SourceInfoSchema, {
  name: 'github',
  description: 'Query repositories, issues, and pull requests from GitHub.',
  version: '1.1.6',
  installed: true,
  // Modelled as the effective metadata returned by GetSourceInfo for an
  // installed imported source.
  origin: SourceOrigin.IMPORTED,
  credentialStorage: SourceCredentialStorage.FILE,
  inputs: [
    create(SourceInputSpecSchema, {
      key: 'GITHUB_API_BASE',
      required: false,
      hint: 'Override the default GITHUB_API_BASE.',
      input: {
        case: 'variable',
        value: create(SourceVariableInputSchema, { defaultValue: 'https://api.github.com' }),
      },
    }),
    create(SourceInputSpecSchema, {
      key: 'GITHUB_TOKEN',
      required: true,
      hint: 'Personal access token for github.',
      input: {
        case: 'secret',
        value: create(SourceSecretInputSchema, {
          credential: create(SourceCredentialSchema, {
            methods: [
              create(SourceCredentialMethodSchema, {
                label: 'Connect with GitHub device code',
                description: 'Sign in to GitHub with a device code.',
                method: {
                  case: 'oauth',
                  value: create(OAuthCredentialMethodSchema, {
                    flow: OauthCredentialFlowType.DEVICE_CODE,
                    endpoints: create(OAuthCredentialEndpointsSchema, {
                      deviceAuthorizationUrl: 'https://github.com/login/device/code',
                      tokenUrl: 'https://github.com/login/oauth/access_token',
                    }),
                    client: create(OAuthCredentialClientSchema, {
                      id: create(OAuthCredentialClientIdSchema, {
                        defaultValue: 'test-client-id',
                        input: 'GITHUB_OAUTH_CLIENT_ID',
                      }),
                    }),
                  }),
                },
              }),
              create(SourceCredentialMethodSchema, {
                label: 'Paste token',
                method: {
                  case: 'sourceConfig',
                  value: create(SourceConfigCredentialMethodSchema, {}),
                },
              }),
            ],
          }),
        }),
      },
    }),
  ],
})

const githubBundledInstalledInfo = create(SourceInfoSchema, {
  ...githubInfo,
  origin: SourceOrigin.BUNDLED,
})

export const githubOauthCatalogInfo = create(SourceInfoSchema, {
  ...githubInfo,
  installed: false,
  // The OAuth install flow exercises GitHub as a bundled catalog source.
  origin: SourceOrigin.BUNDLED,
})

const cloudwatchLogsInfo = create(SourceInfoSchema, {
  name: 'cloudwatch_logs',
  description: 'Query Amazon CloudWatch Logs groups, streams, and events.',
  version: '0.1.0',
  installed: true,
  origin: SourceOrigin.BUNDLED,
  credentialStorage: SourceCredentialStorage.FILE,
  inputs: [
    create(SourceInputSpecSchema, {
      key: 'AWS_REGION',
      required: false,
      hint: 'AWS region for CloudWatch Logs API requests, for example `us-east-1`.',
      input: {
        case: 'variable',
        value: create(SourceVariableInputSchema, { defaultValue: 'us-east-1' }),
      },
    }),
    create(SourceInputSpecSchema, {
      key: 'AWS_ENDPOINT_SUFFIX',
      required: false,
      hint: 'AWS endpoint DNS suffix. Keep `amazonaws.com` for standard AWS regions.',
      input: {
        case: 'variable',
        value: create(SourceVariableInputSchema, { defaultValue: 'amazonaws.com' }),
      },
    }),
    create(SourceInputSpecSchema, {
      key: 'AWS_ACCESS_KEY_ID',
      required: true,
      hint: 'AWS access key ID with CloudWatch Logs read permissions.',
      input: {
        case: 'secret',
        value: create(SourceSecretInputSchema, {}),
      },
    }),
    create(SourceInputSpecSchema, {
      key: 'AWS_SECRET_ACCESS_KEY',
      required: true,
      hint: 'AWS secret access key.',
      input: {
        case: 'secret',
        value: create(SourceSecretInputSchema, {}),
      },
    }),
  ],
})

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

export const bundledCatalog: SourceInfo[] = [
  cloudwatchLogsInfo,
  githubBundledInstalledInfo,
  linearInfo,
  slackInfo,
  sentryInfo,
]

const installedGithub: Source = create(SourceSchema, {
  name: 'github',
  version: '1.1.6',
  origin: SourceOrigin.IMPORTED,
  credentialStorage: SourceCredentialStorage.FILE,
  variables: [
    create(SourceVariableSchema, { key: 'GITHUB_API_BASE', value: 'https://api.github.com' }),
  ],
  secrets: [create(SourceSecretSchema, { key: 'GITHUB_TOKEN', value: '' })],
})

const installedBundledGithub: Source = create(SourceSchema, {
  ...installedGithub,
  origin: SourceOrigin.BUNDLED,
})

const installedCloudwatchLogs: Source = create(SourceSchema, {
  name: 'cloudwatch_logs',
  version: '0.1.0',
  origin: SourceOrigin.BUNDLED,
  credentialStorage: SourceCredentialStorage.FILE,
  variables: [
    create(SourceVariableSchema, { key: 'AWS_REGION', value: 'us-east-1' }),
    create(SourceVariableSchema, { key: 'AWS_ENDPOINT_SUFFIX', value: 'amazonaws.com' }),
  ],
  secrets: [
    create(SourceSecretSchema, { key: 'AWS_ACCESS_KEY_ID', value: '' }),
    create(SourceSecretSchema, { key: 'AWS_SECRET_ACCESS_KEY', value: '' }),
  ],
})

const installedLinear: Source = create(SourceSchema, {
  name: 'linear',
  version: '1.0.0',
  origin: SourceOrigin.BUNDLED,
  credentialStorage: SourceCredentialStorage.FILE,
  variables: [],
  secrets: [create(SourceSecretSchema, { key: 'LINEAR_API_TOKEN', value: '' })],
})

export const initialInstalledSources: Source[] = [installedCloudwatchLogs, installedGithub]

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

export const listEmptyResponse = create(ListSourcesResponseSchema, {
  sources: [],
})

export const listAfterGithubOauthResponse = create(ListSourcesResponseSchema, {
  sources: [installedBundledGithub],
})

export const discoverGithubOauthResponse = create(DiscoverSourcesResponseSchema, {
  sources: [githubOauthCatalogInfo],
})

export const listAfterLinearInstallResponse = create(ListSourcesResponseSchema, {
  sources: [...initialInstalledSources, installedLinear],
})

export const listAfterLinearRemovedResponse = listInitialResponse

export const getInfoLinearResponse = create(GetSourceInfoResponseSchema, { sourceInfo: linearInfo })
export const getInfoGithubResponse = create(GetSourceInfoResponseSchema, { sourceInfo: githubInfo })
export const getInfoCloudwatchLogsResponse = create(GetSourceInfoResponseSchema, {
  sourceInfo: cloudwatchLogsInfo,
})

export const getInstalledGithubResponse = create(GetSourceResponseSchema, {
  source: installedGithub,
})
export const getInstalledBundledGithubResponse = create(GetSourceResponseSchema, {
  source: installedBundledGithub,
})
export const getInstalledCloudwatchLogsResponse = create(GetSourceResponseSchema, {
  source: installedCloudwatchLogs,
})
export const getInstalledLinearResponse = create(GetSourceResponseSchema, {
  source: installedLinear,
})

export const createLinearResponse = create(CreateBundledSourceResponseSchema, {
  source: installedLinear,
})

export const createGithubOauthResponses = [
  create(CreateBundledSourceWithOAuthResponseSchema, {
    event: {
      case: 'oauthAuthorization',
      value: create(OAuthCredentialAuthorizationSchema, {
        inputKey: 'GITHUB_TOKEN',
        authorizationUrl: 'https://github.com/login/device?user_code=ABCD-1234',
        expiresInSeconds: 900n,
        userCode: 'ABCD-1234',
        verificationUri: 'https://github.com/login/device',
        verificationUriComplete: 'https://github.com/login/device?user_code=ABCD-1234',
      }),
    },
  }),
  create(CreateBundledSourceWithOAuthResponseSchema, {
    event: {
      case: 'oauthCompleted',
      value: create(OAuthCredentialCompletedSchema, {
        inputKey: 'GITHUB_TOKEN',
        metadata: [],
      }),
    },
  }),
  create(CreateBundledSourceWithOAuthResponseSchema, {
    event: {
      case: 'source',
      value: installedBundledGithub,
    },
  }),
]

export const deleteSourceResponse = create(DeleteSourceResponseSchema, {})
