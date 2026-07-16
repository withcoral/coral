import { createContext, useContext, useEffect, useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'

import { createRoutesStub } from 'react-router'
import { expect, fn, within } from 'storybook/test'

import { oauthInstallEventToNdjson } from '@/lib/source-oauth-install-stream'
import type { OAuthInstallStreamEvent } from '@/lib/source-oauth-install-stream'
import type { CatalogEntry } from '@/lib/sources'
import { SourceInstallDialog } from '@/views/sources/source-install'

import { SourceCardList } from './source-card-list'

const entries: CatalogEntry[] = [
  {
    description: 'Sync issues, pull requests, and code from your repositories.',
    installed: true,
    name: 'github',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Query messages and metadata from Gmail.',
    installed: false,
    name: 'gmail',
    origin: 'bundled',
    version: '1.0.0',
  },
  {
    description: 'Imported source',
    installed: false,
    name: 'custom_warehouse',
    origin: 'imported',
    version: '0.1.0',
  },
]

const githubOAuthEntry: CatalogEntry = {
  description: 'Sync issues, pull requests, and code from your repositories.',
  inputSpecs: [
    {
      hint: '',
      input: {
        case: 'secret',
        value: {
          credential: {
            methods: [
              {
                description: 'Authorize Coral with GitHub in your browser.',
                hint: '',
                label: 'OAuth',
                method: { case: 'oauth', value: {} },
              },
            ],
          },
        },
      },
      key: 'GITHUB_TOKEN',
      required: true,
    },
  ],
  installed: false,
  name: 'github',
  origin: 'bundled',
  version: '1.0.0',
}

const meta = {
  args: {
    entries,
    onPick: fn(),
  },
  component: SourceCardList,
  parameters: {
    layout: 'padded',
  },
  render: (args) => (
    <div style={{ maxWidth: 960 }}>
      <SourceCardList {...args} />
    </div>
  ),
  tags: ['autodocs'],
  title: 'Components/Sources/SourceCardList',
} satisfies Meta<typeof SourceCardList>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const Empty: Story = {
  args: {
    entries: [],
  },
}

type OAuthStoryState =
  | 'awaiting-authorization'
  | 'exchanging-token'
  | 'finishing-install'
  | 'configured'
  | 'error'

type OAuthInstallFlowStory = StoryObj<{ oauthState: OAuthStoryState }>

const oauthStateLabels: Record<OAuthStoryState, string> = {
  'awaiting-authorization': 'Waiting for Github token authorization in your browser…',
  'exchanging-token': 'Github token authorization received. Exchanging token…',
  'finishing-install': 'Github token authorized. Finishing install…',
  configured: 'Github configured.',
  error: 'GitHub denied the authorization request.',
}

const OAuthStoryStateContext = createContext<OAuthStoryState>('awaiting-authorization')

const OAuthInstallFlowRoutesStub = createRoutesStub([
  {
    Component: OAuthInstallFlowRoute,
    path: '*',
  },
])

export const OAuthInstallFlow: OAuthInstallFlowStory = {
  args: {
    oauthState: 'awaiting-authorization',
  },
  argTypes: {
    oauthState: {
      control: 'select',
      description: 'OAuth install state shown in the source dialog.',
      name: 'OAuth state',
      options: [
        'awaiting-authorization',
        'exchanging-token',
        'finishing-install',
        'configured',
        'error',
      ] satisfies OAuthStoryState[],
    },
  },
  name: 'OAuth install flow',
  parameters: {
    docs: {
      description: {
        story:
          'Shows the GitHub source card and starts its real install flow with simulated OAuth stream events.',
      },
    },
  },
  play: async ({ args, canvasElement }) => {
    const page = within(canvasElement.ownerDocument.body)

    const status = await page.findByText(oauthStateLabels[args.oauthState])
    await expect(status).toBeInTheDocument()
  },
  render: ({ oauthState }) => <OAuthInstallFlowPreview key={oauthState} oauthState={oauthState} />,
}

function OAuthInstallFlowPreview({ oauthState }: { oauthState: OAuthStoryState }) {
  return (
    <OAuthStoryStateContext.Provider value={oauthState}>
      <OAuthInstallFlowRoutesStub initialEntries={['/sources']} />
    </OAuthStoryStateContext.Provider>
  )
}

function OAuthInstallFlowRoute() {
  const oauthState = useContext(OAuthStoryStateContext)
  return <OAuthInstallFlowSurface oauthState={oauthState} />
}

function OAuthInstallFlowSurface({ oauthState }: { oauthState: OAuthStoryState }) {
  const [selectedEntry, setSelectedEntry] = useState<CatalogEntry | null>(githubOAuthEntry)

  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      const addSourceButton = [...document.querySelectorAll<HTMLButtonElement>('button')].find(
        (button) => button.textContent?.trim() === 'Add source',
      )
      addSourceButton?.click()
    })

    return () => window.cancelAnimationFrame(frame)
  }, [])

  return (
    <>
      <div style={{ maxWidth: 480 }}>
        <SourceCardList entries={[githubOAuthEntry]} onPick={setSelectedEntry} />
      </div>
      <SourceInstallDialog
        entry={selectedEntry}
        fetchOAuthInstall={oauthResponse(oauthState)}
        onOpenChange={(open) => {
          if (!open) setSelectedEntry(null)
        }}
        open={selectedEntry !== null}
        openAuthorization={() => undefined}
      />
    </>
  )
}

function oauthResponse(state: OAuthStoryState): typeof fetch {
  return async (_input, init) => {
    const encoder = new TextEncoder()
    const events: OAuthInstallStreamEvent[] = [
      {
        authorizationUrl: 'https://github.com/login/device',
        expiresInSeconds: '900',
        inputKey: 'GITHUB_TOKEN',
        type: 'oauthAuthorization',
        userCode: 'ABCD-EFGH',
        verificationUri: 'https://github.com/login/device',
        verificationUriComplete: 'https://github.com/login/device?user_code=ABCD-EFGH',
      },
    ]

    if (state === 'error') {
      events.push({ message: oauthStateLabels.error, type: 'error' })
    } else if (state !== 'awaiting-authorization') {
      events.push({ inputKey: 'GITHUB_TOKEN', type: 'oauthCallbackReceived' })
      if (state !== 'exchanging-token') {
        events.push({ inputKey: 'GITHUB_TOKEN', metadata: [], type: 'oauthCompleted' })
        if (state === 'configured') {
          events.push({ name: 'github', type: 'source', version: '1.0.0' })
        }
      }
    }

    const terminal = state === 'configured' || state === 'error'

    return new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          for (const event of events) {
            controller.enqueue(encoder.encode(oauthInstallEventToNdjson(event)))
          }
          if (terminal) {
            controller.close()
          } else {
            init?.signal?.addEventListener('abort', () => controller.close(), { once: true })
          }
        },
      }),
    )
  }
}
