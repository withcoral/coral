import type { Meta, StoryObj } from '@storybook/react-vite'
import { createContext, useContext, type ComponentProps } from 'react'

import { createRoutesStub } from 'react-router'
import { expect, fn, waitFor, within } from 'storybook/test'

import { dismissAllToasts } from '@/wax/components/toast'
import { ToastContainer } from '@/wax/components/toast/toast-container'

import type { SourceDescribeData } from '@/lib/source-describe'
import { oauthInstallEventToNdjson } from '@/lib/source-oauth-install-stream'
import { SourceAddDialog } from '@/views/sources/source-add'

type SourceAddDialogProps = ComponentProps<typeof SourceAddDialog>

const DESCRIBE_PATH = '/workspaces/default/sources/describe'
const DISCOVERY_PATH = '/workspaces/default/sources/discover'
const OAUTH_IMPORT_PATH = '/workspaces/default/sources/oauth-import'

const DISCOVERY = {
  auth: { kind: 'bearer' as const, label: 'Bearer token' },
  description: 'Weather observations and forecasts',
  format: 'openapi-yaml' as const,
  name: 'weather_api',
  serverUrl: 'https://weather.example/v1',
  status: 'success' as const,
  title: 'Weather API',
  url: 'https://weather.example/openapi.yaml',
}

const MANIFEST = `name: weather_api
dsl_version: 4
surface:
  type: mcp
`

// Shaped like a real materialize failure: a Connect status prefix, unbreakable
// Rust type paths, and the server's newline-separated `Hint:` line.
const UNBREAKABLE_IMPORT_ERROR = `[unavailable] unavailable: failed to materialize source 'axiom': failed precondition: MCP HTTP server for source \`axiom\` returned a message error Transport [rmcp::transport::worker::WorkerTransport<rmcp::transport::streamable_http_client::StreamableHttpClientWorker>] error: Auth required, when send initialize request
Hint: Install or update the source with the required OAuth or bearer credentials, then retry the query.`

// Shaped like a real DescribeSourceManifest response: one variable, and one
// secret whose OAuth method needs a client ID from the user. The stub answers
// every manifest with this, so a paste or a picked file both reach the last step.
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
  message: UNBREAKABLE_IMPORT_ERROR,
  status: 'error',
}

const SourceAddStoryContext = createContext<SourceAddDialogProps | null>(null)

function addRoutesStub(describe: SourceDescribeData) {
  return createRoutesStub([
    { action: () => describe, path: '/workspaces/:workspaceId/sources/describe' },
    { loader: () => DISCOVERY, path: '/workspaces/:workspaceId/sources/discover' },
    { Component: SourceAddStoryRoute, path: '/workspaces/:workspaceId/sources/install' },
  ])
}

function pendingOAuthResponse(inputKey: string, userCode: string): typeof fetch {
  return async (_input, init) => {
    const event = oauthInstallEventToNdjson({
      authorizationUrl: 'https://weather.example/device',
      expiresInSeconds: '900',
      inputKey,
      type: 'oauthAuthorization',
      userCode,
      verificationUri: 'https://weather.example/device',
      verificationUriComplete: `https://weather.example/device?user_code=${userCode}`,
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
}

const meta = {
  // Toasts outlive a story, so clear them before each one runs.
  beforeEach: () => {
    dismissAllToasts()
  },
  args: {
    describePath: DESCRIBE_PATH,
    discoveryPath: DISCOVERY_PATH,
    oauthImportPath: OAUTH_IMPORT_PATH,
    onOpenChange: fn(),
    open: true,
  },
  component: SourceAddDialog,
  parameters: {
    layout: 'fullscreen',
  },
  render: (args) => <SourceAddDialogStory describe={DESCRIBED} {...args} />,
  tags: ['autodocs'],
  title: 'Components/Sources/SourceAddDialog',
} satisfies Meta<typeof SourceAddDialog>

export default meta
type Story = StoryObj<typeof meta>

export const FirstStep: Story = {
  name: 'First step',
  play: async ({ canvasElement }) => {
    const page = within(canvasElement.ownerDocument.body)

    await expect(page.getByRole('dialog')).toBeVisible()
    await expect(page.getByLabelText('Source URL')).toBeVisible()
    await expect(
      page.getByText('Enter an OpenAPI document or streamable HTTP MCP endpoint.'),
    ).toBeVisible()
    await expect(page.getByText('Drop a manifest file here')).toBeVisible()
    await expect(page.getByRole('button', { name: 'Choose a file' })).toBeVisible()
    // Neither way in is chosen yet, so the step count is not knowable here.
    await expect(page.queryByText(/^Step /)).toBeNull()
  },
}

export const UrlDiscardConfirmation: Story = {
  name: 'URL: discard confirmation',
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.type(page.getByLabelText('Source URL'), DISCOVERY.url)
    await userEvent.click(page.getByRole('button', { name: 'Cancel' }))

    await waitFor(() =>
      expect(page.getByRole('dialog', { name: 'Discard source draft?' })).toBeVisible(),
    )
    await expect(page.getByRole('button', { name: 'Keep editing' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Discard' })).toBeVisible()
  },
}

export const UrlDetails: Story = {
  name: 'URL: details',
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.type(page.getByLabelText('Source URL'), DISCOVERY.url)
    await userEvent.click(page.getByRole('button', { name: 'Next' }))

    await waitFor(() => expect(page.getByLabelText('Name')).toHaveValue('weather_api'))
    await waitFor(() => expect(page.getByText('Step 2/3')).toBeVisible())
    await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(2))
    await userEvent.click(page.getByRole('radio', { name: 'MCP server' }))
    await expect(page.getByRole('radio', { name: 'MCP server' })).toBeChecked()
  },
}

export const UrlCredentials: Story = {
  name: 'URL: credentials',
  play: async (context) => {
    const { canvasElement, userEvent } = context
    const page = within(canvasElement.ownerDocument.body)

    await stepToCredentials(context)

    const activeToken = page
      .getAllByPlaceholderText('Paste token')
      .find((element) => !element.hasAttribute('disabled'))
    if (!activeToken) throw new Error('Active token input not found')
    await waitFor(() => expect(activeToken).toBeVisible())
    await waitFor(() => expect(page.getByText('Step 3/3')).toBeVisible())
    await userEvent.click(page.getByRole('radio', { name: 'None' }))
    await expect(page.getByText('This endpoint doesn’t require credentials.')).toBeVisible()
    await expect(activeToken).not.toBeVisible()
  },
}

export const UrlOAuthLoading: Story = {
  args: {
    fetchOAuthImport: pendingOAuthResponse('API_TOKEN', 'ABCD-EFGH'),
    openAuthorization: fn(),
  },
  name: 'URL: OAuth loading',
  play: async (context) => {
    const { canvasElement, userEvent } = context
    const page = within(canvasElement.ownerDocument.body)

    await stepToCredentials(context)
    await userEvent.click(page.getByRole('radio', { name: 'OAuth device flow' }))
    await userEvent.type(
      page.getByLabelText('Device authorization URL'),
      'https://weather.example/device/code',
    )
    await userEvent.type(page.getByLabelText('Token URL'), 'https://weather.example/oauth/token')
    await userEvent.type(page.getByLabelText('Client ID'), 'storybook-client')
    await userEvent.click(page.getByRole('button', { name: 'Add source' }))

    await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(4))
    await waitFor(() =>
      expect(page.getByRole('dialog', { name: 'Authorize Api token' })).toBeVisible(),
    )
    await expect(page.getByText('ABCD-EFGH')).toBeVisible()
  },
}

// No play function: drop a file or pick one to walk the whole import yourself.
export const Manifest: Story = {}

export const ManifestOAuthLoading: Story = {
  args: {
    fetchOAuthImport: pendingOAuthResponse('WEATHER_TOKEN', 'WXYZ-1234'),
    openAuthorization: fn(),
  },
  name: 'Manifest: OAuth loading',
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.upload(manifestInput(page), manifestFile())
    await waitFor(() => expect(page.getByRole('button', { name: 'Add source' })).toBeVisible())
    await expect(page.getByText('Step 2/2')).toBeVisible()

    // Every credential method also renders an inert copy that sizes the tab area,
    // so the client field is only unambiguous inside the selected panel.
    const panel = within(page.getByRole('tabpanel'))
    await userEvent.type(panel.getByLabelText('Weather client id'), 'storybook-client')
    await userEvent.click(page.getByRole('button', { name: 'Add source' }))

    await waitFor(() =>
      expect(page.getByRole('dialog', { name: 'Authorize Weather token' })).toBeVisible(),
    )
    await expect(page.getByText('WXYZ-1234')).toBeVisible()
  },
}

export const ManifestDiscardConfirmation: Story = {
  name: 'Manifest: discard confirmation',
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.upload(manifestInput(page), manifestFile())
    await waitFor(() => expect(page.getByText('Step 2/2')).toBeVisible())

    // Nothing typed yet, so there is nothing to confirm.
    const panel = within(page.getByRole('tabpanel'))
    await userEvent.type(panel.getByLabelText('Weather client id'), 'storybook-client')
    await userEvent.click(page.getByRole('button', { name: 'Cancel' }))

    await waitFor(() =>
      expect(page.getByRole('dialog', { name: 'Discard source draft?' })).toBeVisible(),
    )
  },
}

export const ManifestError: Story = {
  name: 'Manifest: error',
  render: (args) => <SourceAddDialogStory describe={REJECTED} {...args} />,
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.upload(manifestInput(page), manifestFile())

    // The toast carries the parse error, and the dialog stays on the first step so
    // the next file replaces the rejected manifest.
    await waitFor(() => expect(page.getByText('Coral could not read that manifest')).toBeVisible())
    await expect(page.getByText('Drop a manifest file here')).toBeVisible()

    // Server errors carry unbreakable Rust type paths, and the toast is a fixed
    // width, so the message has to break inside it rather than spill past it.
    const message = page.getByText(/failed to materialize source 'axiom'/)
    const toast = message.closest('[class*="container"]')
    if (!(toast instanceof HTMLElement)) throw new Error('Toast not found')
    await expect(message.getBoundingClientRect().right).toBeLessThanOrEqual(
      toast.getBoundingClientRect().right,
    )
  },
}

type PlayContext = Parameters<NonNullable<Story['play']>>[0]

/** Walk the URL branch to its last step, which both credential stories start from. */
async function stepToCredentials({ canvasElement, userEvent }: PlayContext) {
  const page = within(canvasElement.ownerDocument.body)

  await userEvent.type(page.getByLabelText('Source URL'), DISCOVERY.url)
  await userEvent.click(page.getByRole('button', { name: 'Next' }))
  await waitFor(() => expect(page.getByLabelText('Name')).toHaveValue('weather_api'))
  await userEvent.click(page.getByRole('button', { name: 'Next' }))
  await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(3))
}

/** The picker is the only way in that a play function can drive. */
function manifestInput(page: ReturnType<typeof within>): HTMLElement {
  return page.getByTestId('manifest-file')
}

function manifestFile(): File {
  return new File([MANIFEST], 'weather_api.yaml', { type: 'application/yaml' })
}

function activeDialogCount(document: Document) {
  return activeDialogs(document).length
}

function activeDialogs(document: Document) {
  return document.querySelectorAll('[role="dialog"]:not([data-ending-style])')
}

function SourceAddDialogStory({
  describe,
  ...args
}: SourceAddDialogProps & { describe: SourceDescribeData }) {
  const RoutesStub = addRoutesStub(describe)
  return (
    <SourceAddStoryContext.Provider value={args}>
      <RoutesStub initialEntries={['/workspaces/default/sources/install']} />
      <ToastContainer />
    </SourceAddStoryContext.Provider>
  )
}

function SourceAddStoryRoute() {
  const args = useContext(SourceAddStoryContext)
  if (!args) return null
  return <SourceAddDialog {...args} />
}
