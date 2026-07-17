import type { Meta, StoryObj } from '@storybook/react-vite'
import { createContext, useContext, type ComponentProps } from 'react'

import { createRoutesStub } from 'react-router'
import { expect, fn, waitFor, within } from 'storybook/test'

import { SourceCreateDialog } from '@/views/sources/source-create'

type SourceCreateDialogProps = ComponentProps<typeof SourceCreateDialog>

const DISCOVERY_PATH = '/workspaces/default/sources/discover'
const DISCOVERY = {
  auth: { kind: 'bearer' as const, label: 'Bearer token' },
  description: 'Weather observations and forecasts',
  format: 'openapi-yaml' as const,
  name: 'weather_api',
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
    await expect(page.getByText('Step 1 of 3 — URL')).toBeVisible()
    await expect(page.getByLabelText('Source URL')).toBeVisible()
  },
}

export const Connection: Story = {
  name: 'Details',
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.type(page.getByLabelText('Source URL'), DISCOVERY.url)
    await userEvent.click(page.getByRole('button', { name: 'Next' }))

    await waitFor(() => expect(page.getByLabelText('Name')).toHaveValue('weather_api'))
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

    await waitFor(() => {
      const activeToken = page
        .getAllByPlaceholderText('Paste token')
        .find((element) => !element.hasAttribute('disabled'))
      if (!activeToken) throw new Error('Active token input not found')
      return expect(activeToken).toBeVisible()
    })
    await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(3))
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
