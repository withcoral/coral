import type { Meta, StoryObj } from '@storybook/react-vite'
import { createContext, useContext, type ComponentProps } from 'react'

import { createRoutesStub } from 'react-router'
import { expect, fn, waitFor, within } from 'storybook/test'

import { SourceCreateDialog } from '@/views/sources/source-create'

type SourceCreateDialogProps = ComponentProps<typeof SourceCreateDialog>

const SourceCreateStoryContext = createContext<SourceCreateDialogProps | null>(null)
const SourceCreateRoutesStub = createRoutesStub([
  {
    Component: SourceCreateStoryRoute,
    path: '*',
  },
])

const meta = {
  args: {
    actionData: undefined,
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
  play: async ({ canvasElement }) => {
    const page = within(canvasElement.ownerDocument.body)

    await expect(page.getByRole('dialog')).toBeVisible()
    await expect(page.getByText('Step 1 of 3 — Basics')).toBeVisible()
    await expect(page.getByLabelText('Name')).toBeVisible()
    await expect(page.getByLabelText('Description (optional)')).toBeVisible()
  },
}

export const Connection: Story = {
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.type(page.getByLabelText('Name'), 'weather_api')
    await userEvent.click(page.getByRole('button', { name: 'Next' }))

    await waitFor(() => expect(page.getByLabelText('OpenAPI descriptor URL')).toBeVisible())
    await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(2))
    await userEvent.click(page.getByRole('button', { name: 'Custom header' }))
    await expect(page.getByLabelText('Header name')).toBeVisible()
  },
}

export const Credentials: Story = {
  play: async ({ canvasElement, userEvent }) => {
    const page = within(canvasElement.ownerDocument.body)

    await userEvent.type(page.getByLabelText('Name'), 'weather_api')
    await userEvent.click(page.getByRole('button', { name: 'Next' }))
    await userEvent.type(
      page.getByLabelText('OpenAPI descriptor URL'),
      'https://weather.example/openapi.yaml',
    )
    await userEvent.click(page.getByRole('button', { name: 'Next' }))

    await waitFor(() => expect(page.getByLabelText('Bearer token')).toBeVisible())
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

    await userEvent.type(page.getByLabelText('Name'), 'weather_api')
    await userEvent.click(page.getByRole('button', { name: 'Next' }))
    await userEvent.type(
      page.getByLabelText('OpenAPI descriptor URL'),
      'https://weather.example/openapi.yaml',
    )
    await userEvent.click(page.getByRole('button', { name: 'Next' }))

    await waitFor(() => expect(activeDialogCount(canvasElement.ownerDocument)).toBe(3))
    const dialogs = activeDialogs(canvasElement.ownerDocument)
    const credentialsDialog = dialogs.item(dialogs.length - 1)
    if (!(credentialsDialog instanceof HTMLElement)) throw new Error('Credentials dialog not found')
    await expect(
      within(credentialsDialog).getByText('The OpenAPI descriptor could not be loaded.'),
    ).toBeVisible()
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
