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
type UserEventLike = {
  paste: (text: string) => Promise<void>
}

const DESCRIBE_PATH = '/workspaces/default/sources/describe'
const OAUTH_IMPORT_PATH = '/workspaces/default/sources/oauth-import'

const MANIFEST = `name: weather_api
dsl_version: 4
surface:
  type: mcp
`

// Shaped like a real DescribeSourceManifest response: one variable, and one
// secret whose OAuth method needs a client ID from the user.
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

// Dynamic Client Registration sources declare no client inputs, so the OAuth
// method renders only the sign-in prompt.
const DESCRIBED_WITHOUT_CLIENT_FIELDS: SourceDescribeData = {
  entry: {
    description: 'ClickHouse Cloud remote MCP server.',
    installed: false,
    inputSpecs: [
      {
        hint: '',
        input: {
          case: 'secret',
          value: {
            credential: {
              methods: [
                {
                  description: '',
                  hint: '',
                  label: 'Connect with ClickHouse Cloud',
                  method: { case: 'oauth', value: { client: {} } },
                },
              ],
            },
          },
        },
        key: 'CLICKHOUSE_ACCESS_TOKEN',
        required: true,
      },
    ],
    name: 'clickhouse_cloud',
    origin: 'imported',
    version: '0.3.0',
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

export const Basics: Story = {
  name: 'Choose how',
  play: async ({ canvasElement }) => {
    const page = within(canvasElement.ownerDocument.body)

    await expect(page.getByRole('dialog')).toBeVisible()
    await expect(page.getByText('Step 1/2')).toBeVisible()
    await expect(page.getByText('Drop a manifest file here')).toBeVisible()
    await expect(page.getByRole('button', { name: 'Choose a file' })).toBeVisible()
    // Coral reads the manifest and shows what it found, so there is nothing to
    // edit here and no field to edit it in.
    await expect(page.queryByLabelText('Manifest')).toBeNull()
  },
}

export const PasteStep: Story = {
  name: 'Paste a manifest',
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    // Pasting is the whole intent: the manifest goes straight to the server and
    // the dialog shows the name, version, and inputs it read back.
    await userEvent.paste(MANIFEST)

    await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(2))
    await waitFor(() => expect(page.getByText('Step 2/2')).toBeVisible())
    await expect(page.getByText('weather api')).toBeVisible()
    await expect(page.getByText('1.0.0')).toBeVisible()
    await expect(page.getByRole('button', { name: 'Import source' })).toBeVisible()
  },
}

export const UploadAFile: Story = {
  name: 'Upload a file',
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)
    const file = new File([MANIFEST], 'weather.yaml', { type: 'application/yaml' })

    // A picked file skips straight to the described source, so a 4.6 MB generated
    // manifest never has to render.
    await userEvent.upload(page.getByTestId('manifest-file'), file)

    await waitFor(() => expect(page.getByText('Step 2/2')).toBeVisible())
    await expect(page.getByText('weather api')).toBeVisible()
    await expect(page.getByRole('button', { name: 'Import source' })).toBeVisible()
  },
}

export const Credentials: Story = {
  name: 'Credentials',
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await pasteManifest(userEvent)

    // A described manifest stacks a second dialog rather than growing the first.
    await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(2))
    await waitFor(() => expect(page.getByText('Step 2/2')).toBeVisible())
    await expect(page.getByText('weather api')).toBeVisible()
    await expect(page.getByRole('tab', { name: 'Connect with OAuth' })).toBeVisible()
    await expect(page.getByRole('tab', { name: 'Paste token' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Import source' })).toBeVisible()
  },
}

export const DynamicClientRegistration: Story = {
  name: 'No client fields to fill',
  render: (args) => (
    <SourceImportDialogStory describe={DESCRIBED_WITHOUT_CLIENT_FIELDS} {...args} />
  ),
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await pasteManifest(userEvent)

    // The prompt has to name this dialog's own button, not the install dialog's.
    await waitFor(() =>
      expect(
        page.getByText('Click Import source to open your browser and complete sign-in.'),
      ).toBeVisible(),
    )
  },
}

export const RejectedManifest: Story = {
  name: 'Rejected manifest',
  render: (args) => <SourceImportDialogStory describe={REJECTED} {...args} />,
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await pasteManifest(userEvent)

    // The toast carries the parse error, and the dialog stays on the first step so
    // the next paste or file replaces the rejected manifest.
    await waitFor(() => expect(page.getByText('Coral could not read that manifest')).toBeVisible())
    await expect(page.getByText(/must use absolute file descriptors/)).toBeVisible()
    await expect(activeDialogCount(canvasElement.ownerDocument)).toBe(1)
    await expect(page.getByText('Drop a manifest file here')).toBeVisible()
  },
}

export const OAuthLoading: Story = {
  args: {
    fetchOAuthImport: pendingOAuthResponse,
    openAuthorization: fn(),
  },
  name: 'OAuth loading',
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await pasteManifest(userEvent)
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

function activeDialogCount(document: Document) {
  return document.querySelectorAll('[role="dialog"]:not([data-ending-style])').length
}

// The first step takes a paste as the manifest itself, so there is no field to
// fill and no button to press before Coral reads it.
async function pasteManifest(userEvent: UserEventLike) {
  await userEvent.paste(MANIFEST)
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
