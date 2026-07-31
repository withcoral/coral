import type { Meta, StoryObj } from '@storybook/react-vite'
import { createContext, useContext, type ComponentProps } from 'react'

import { createRoutesStub } from 'react-router'
import { expect, fn, waitFor, within } from 'storybook/test'

import { oauthInstallEventToNdjson } from '@/lib/source-oauth-install-stream'
import { SourceCreateDialog } from '@/views/sources/source-create'

type SourceCreateDialogProps = ComponentProps<typeof SourceCreateDialog>

const DISCOVERY_PATH = '/workspaces/default/sources/discover'
const DISCOVERY = {
  auth: { headerNames: [], kind: 'bearer' as const, kinds: ['bearer' as const] },
  description: 'Weather observations and forecasts',
  format: 'openapi-yaml' as const,
  name: 'weather_api',
  serverUrl: 'https://weather.example/v1',
  status: 'success' as const,
  url: 'https://weather.example/openapi.yaml',
}

const SourceCreateStoryContext = createContext<SourceCreateDialogProps | null>(null)
const SourceCreateRoutesStub = createRoutesStub([
  {
    loader: () => DISCOVERY,
    path: '/workspaces/:workspaceId/sources/discover',
  },
  {
    Component: SourceCreateStoryRoute,
    path: '/workspaces/:workspaceId/sources/install',
  },
])

const pendingOAuthResponse: typeof fetch = async (_input, init) => {
  const event = oauthInstallEventToNdjson({
    authorizationUrl: 'https://github.com/login/device',
    expiresInSeconds: '900',
    inputKey: 'API_TOKEN',
    type: 'oauthAuthorization',
    userCode: 'ABCD-EFGH',
    verificationUri: 'https://github.com/login/device',
    verificationUriComplete: 'https://github.com/login/device?user_code=ABCD-EFGH',
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
  args: {
    actionData: undefined,
    discoveryPath: DISCOVERY_PATH,
    onOpenChange: fn(),
    open: true,
  },
  component: SourceCreateDialog,
  parameters: {
    layout: 'fullscreen',
  },
  render: (args) => <SourceCreateDialogStory {...args} />,
  tags: ['autodocs'],
  title: 'Components/Sources/SourceCreateDialog',
} satisfies Meta<typeof SourceCreateDialog>

export default meta
type Story = StoryObj<typeof meta>

export const Basics: Story = {
  name: 'URL',
  play: async ({ canvasElement }) => {
    const page = within(canvasElement.ownerDocument.body)

    await expect(page.getByRole('dialog')).toBeVisible()
    await expect(page.getByText('Step 1/3')).toBeVisible()
    await expect(page.getByLabelText('Source URL')).toBeVisible()
    await expect(
      page.getByText('Enter an OpenAPI document or streamable HTTP MCP endpoint.'),
    ).toBeVisible()
  },
}

export const DiscardConfirmation: Story = {
  name: 'Discard confirmation',
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

export const Connection: Story = {
  name: 'Details',
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

export const Credentials: Story = {
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.type(page.getByLabelText('Source URL'), DISCOVERY.url)
    await userEvent.click(page.getByRole('button', { name: 'Next' }))
    await waitFor(() => expect(page.getByLabelText('Name')).toHaveValue('weather_api'))
    await userEvent.click(page.getByRole('button', { name: 'Next' }))

    const activeToken = page
      .getAllByPlaceholderText('Paste token')
      .find((element) => !element.hasAttribute('disabled'))
    if (!activeToken) throw new Error('Active token input not found')
    await waitFor(() => expect(activeToken).toBeVisible())
    await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(3))
    await waitFor(() => expect(page.getByText('Step 3/3')).toBeVisible())
    await userEvent.click(page.getByRole('radio', { name: 'None' }))
    await expect(page.getByText('This endpoint doesn’t require credentials.')).toBeVisible()
    await expect(activeToken).not.toBeVisible()
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

    await userEvent.type(page.getByLabelText('Source URL'), DISCOVERY.url)
    await userEvent.click(page.getByRole('button', { name: 'Next' }))
    await waitFor(() => expect(page.getByLabelText('Name')).toHaveValue('weather_api'))
    await userEvent.click(page.getByRole('button', { name: 'Next' }))
    await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(3))
    await userEvent.click(page.getByRole('radio', { name: 'OAuth device flow' }))
    await userEvent.type(
      page.getByLabelText('Device authorization URL'),
      'https://github.com/login/device/code',
    )
    await userEvent.type(
      page.getByLabelText('Token URL'),
      'https://github.com/login/oauth/access_token',
    )
    await userEvent.type(page.getByLabelText('Client ID'), 'storybook-client')
    await userEvent.click(page.getByRole('button', { name: 'Create source' }))

    await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(4))
    await waitFor(() =>
      expect(page.getByRole('dialog', { name: 'Authorize Api token' })).toBeVisible(),
    )
    await expect(page.getByText('ABCD-EFGH')).toBeVisible()
  },
}

export const ImportError: Story = {
  args: {
    actionData: {
      intent: 'import',
      message: 'The OpenAPI descriptor could not be loaded.',
      name: 'weather_api',
      status: 'error',
    },
  },
  name: 'Import error',
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.type(page.getByLabelText('Source URL'), DISCOVERY.url)
    await userEvent.click(page.getByRole('button', { name: 'Next' }))
    await waitFor(() => expect(page.getByLabelText('Name')).toHaveValue('weather_api'))
    await userEvent.click(page.getByRole('button', { name: 'Next' }))

    await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(3))
    await waitFor(() => {
      const dialogs = activeDialogs(canvasElement.ownerDocument)
      const credentialsDialog = dialogs.item(dialogs.length - 1)
      if (!(credentialsDialog instanceof HTMLElement))
        throw new Error('Credentials dialog not found')
      return expect(
        within(credentialsDialog).getByText('The OpenAPI descriptor could not be loaded.'),
      ).toBeVisible()
    })
  },
}

function activeDialogCount(document: Document) {
  return activeDialogs(document).length
}

function activeDialogs(document: Document) {
  return document.querySelectorAll('[role="dialog"]:not([data-ending-style])')
}

function SourceCreateDialogStory(args: SourceCreateDialogProps) {
  return (
    <SourceCreateStoryContext.Provider value={args}>
      <SourceCreateRoutesStub initialEntries={['/workspaces/default/sources/install']} />
    </SourceCreateStoryContext.Provider>
  )
}

function SourceCreateStoryRoute() {
  const args = useContext(SourceCreateStoryContext)
  if (!args) return null
  return <SourceCreateDialog {...args} />
}
