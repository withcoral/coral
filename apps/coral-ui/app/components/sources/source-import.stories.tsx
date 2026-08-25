import type { Meta, StoryObj } from '@storybook/react-vite'
import { createContext, useContext, type ComponentProps } from 'react'

import { createRoutesStub } from 'react-router'
import { expect, fn, waitFor, within } from 'storybook/test'

import { dismissAllToasts } from '@/wax/components/toast'
import { ToastContainer } from '@/wax/components/toast/toast-container'

import { oauthInstallEventToNdjson } from '@/lib/source-oauth-install-stream'
import type { SourceDescribeData } from '@/lib/source-describe'
import { SourceImportDialog } from '@/views/sources/source-import'

type SourceImportDialogProps = ComponentProps<typeof SourceImportDialog>

const DESCRIBE_PATH = '/workspaces/default/sources/describe'
const OAUTH_IMPORT_PATH = '/workspaces/default/sources/oauth-import'

const MANIFEST = `name: weather_api
dsl_version: 4
surface:
  type: mcp
`

// Shaped like a real DescribeSourceManifest response: one variable, and one
// secret whose OAuth method needs a client ID from the user. The stub answers
// every manifest with this, so a paste or a picked file both reach step 2.
const DESCRIBED: SourceDescribeData = {
  entry: {
    description: 'Weather observations and forecasts',
    installed: false,
    inputSpecs: [
      {
        hint: '',
        input: { case: 'variable', value: { defaultValue: 'https://weather.example/v1' } },
        key: 'WEATHER_BASE_URL',
        required: true,
      },
      {
        hint: 'Connect with the weather provider.',
        input: {
          case: 'secret',
          value: {
            credential: {
              methods: [
                {
                  description: 'Sign in through the provider.',
                  hint: '',
                  label: 'Connect with OAuth',
                  method: {
                    case: 'oauth',
                    value: { client: { id: { defaultValue: '', input: 'WEATHER_CLIENT_ID' } } },
                  },
                },
                {
                  description: '',
                  hint: '',
                  label: 'Paste token',
                  method: { case: 'sourceConfig', value: {} },
                },
              ],
            },
          },
        },
        key: 'WEATHER_TOKEN',
        required: true,
      },
    ],
    name: 'weather_api',
    origin: 'imported',
    version: '1.0.0',
  },
  status: 'success',
}

const REJECTED: SourceDescribeData = {
  message:
    "invalid input: OpenAPI descriptor './openapi.yaml' is relative, but imported DSL v4 manifests must use absolute file descriptors.",
  status: 'error',
}

const SourceImportStoryContext = createContext<SourceImportDialogProps | null>(null)

function importRoutesStub(describe: SourceDescribeData) {
  return createRoutesStub([
    { action: () => describe, path: '/workspaces/:workspaceId/sources/describe' },
    { Component: SourceImportStoryRoute, path: '/workspaces/:workspaceId/sources/import' },
  ])
}

const pendingOAuthResponse: typeof fetch = async (_input, init) => {
  const event = oauthInstallEventToNdjson({
    authorizationUrl: 'https://weather.example/device',
    expiresInSeconds: '900',
    inputKey: 'WEATHER_TOKEN',
    type: 'oauthAuthorization',
    userCode: 'WXYZ-1234',
    verificationUri: 'https://weather.example/device',
    verificationUriComplete: 'https://weather.example/device?user_code=WXYZ-1234',
  })
  const encoder = new TextEncoder()

  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(encoder.encode(event))
        init?.signal?.addEventListener('abort', () => controller.close(), { once: true })
      },
    }),
  )
}

const meta = {
  // Toasts outlive a story, so clear them before each one runs.
  beforeEach: () => {
    dismissAllToasts()
  },
  args: {
    describePath: DESCRIBE_PATH,
    oauthImportPath: OAUTH_IMPORT_PATH,
    onOpenChange: fn(),
    open: true,
  },
  component: SourceImportDialog,
  parameters: {
    layout: 'fullscreen',
  },
  render: (args) => <SourceImportDialogStory describe={DESCRIBED} {...args} />,
  tags: ['autodocs'],
  title: 'Components/Sources/SourceImportDialog',
} satisfies Meta<typeof SourceImportDialog>

export default meta
type Story = StoryObj<typeof meta>

// No play function: drop a file, pick one, or paste a manifest to walk the whole
// import yourself.
export const Default: Story = {}

export const OAuthLoading: Story = {
  args: {
    fetchOAuthImport: pendingOAuthResponse,
    openAuthorization: fn(),
  },
  name: 'OAuth loading',
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    // The first step takes a paste as the manifest itself, so there is no field
    // to fill and no button to press before Coral reads it.
    await userEvent.paste(MANIFEST)
    await waitFor(() => expect(page.getByRole('button', { name: 'Import source' })).toBeVisible())

    // Every credential method also renders an inert copy that sizes the tab area,
    // so the client field is only unambiguous inside the selected panel.
    const panel = within(page.getByRole('tabpanel'))
    await userEvent.type(panel.getByLabelText('Weather client id'), 'storybook-client')
    await userEvent.click(page.getByRole('button', { name: 'Import source' }))

    await waitFor(() =>
      expect(page.getByRole('dialog', { name: 'Authorize Weather token' })).toBeVisible(),
    )
    await expect(page.getByText('WXYZ-1234')).toBeVisible()
  },
}

export const ManifestError: Story = {
  name: 'Manifest error',
  render: (args) => <SourceImportDialogStory describe={REJECTED} {...args} />,
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.paste(MANIFEST)

    // The toast carries the parse error, and the dialog stays on the first step so
    // the next paste or file replaces the rejected manifest.
    await waitFor(() => expect(page.getByText('Coral could not read that manifest')).toBeVisible())
    await expect(page.getByText(/must use absolute file descriptors/)).toBeVisible()
    await expect(page.getByText('Drop a manifest file here')).toBeVisible()
  },
}

function SourceImportDialogStory({
  describe,
  ...args
}: SourceImportDialogProps & { describe: SourceDescribeData }) {
  const RoutesStub = importRoutesStub(describe)
  return (
    <SourceImportStoryContext.Provider value={args}>
      <RoutesStub initialEntries={['/workspaces/default/sources/import']} />
      <ToastContainer />
    </SourceImportStoryContext.Provider>
  )
}

function SourceImportStoryRoute() {
  const args = useContext(SourceImportStoryContext)
  if (!args) return null
  return <SourceImportDialog {...args} />
}
