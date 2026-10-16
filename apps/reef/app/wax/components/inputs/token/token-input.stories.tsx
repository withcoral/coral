import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'
import { expect, screen, userEvent as user, waitFor, within } from 'storybook/test'

import { TokenInput, type TokenInputProps } from './token-input'

function Controlled(props: Partial<TokenInputProps>) {
  const [value, setValue] = useState<string[]>(props.value ?? [])
  return (
    <div style={{ width: 360 }}>
      <TokenInput placeholder="Add a scope" {...props} value={value} onChange={setValue} />
      <p style={{ color: 'white', marginTop: 16 }}>Value: {JSON.stringify(value)}</p>
    </div>
  )
}

/** Counts submissions, so a story can tell adding a token from submitting the form. */
function InForm() {
  const [submits, setSubmits] = useState(0)
  return (
    <form
      onSubmit={(event) => {
        event.preventDefault()
        setSubmits((count) => count + 1)
      }}
    >
      <Controlled />
      <button type="submit">Submit</button>
      <p style={{ color: 'white' }}>Submitted {submits}</p>
    </form>
  )
}

const meta = {
  component: TokenInput,
  title: 'Wax/Inputs/TokenInput',
} satisfies Meta<typeof TokenInput>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: { onChange: () => {}, value: [] },
  render: () => <Controlled />,
}

export const WithTokens: Story = {
  args: { onChange: () => {}, value: [] },
  render: () => <Controlled value={['read', 'write', 'offline_access']} />,
}

export const WithSuggestions: Story = {
  args: { onChange: () => {}, value: [] },
  render: () => (
    <Controlled value={['read']} suggestions={['read', 'write', 'admin', 'offline_access']} />
  ),
}

export const Invalid: Story = {
  args: { onChange: () => {}, value: [] },
  render: () => <Controlled invalid value={['bad scope']} />,
}

export const TypeAndPaste: Story = {
  args: { onChange: () => {}, value: [] },
  render: () => <Controlled />,
  play: async ({ canvas, userEvent }) => {
    const input = canvas.getByRole('combobox')
    await userEvent.type(input, 'read write,')
    await expect(canvas.getByText('Value: ["read","write"]')).toBeVisible()
    await expect(input).toHaveValue('')
    // A token already present is not added twice.
    await userEvent.type(input, 'read ')
    await expect(canvas.getByText('Value: ["read","write"]')).toBeVisible()
    // A pasted list splits; its last token waits until the field is left.
    await userEvent.paste('admin, offline_access')
    await expect(canvas.getByText('Value: ["read","write","admin"]')).toBeVisible()
    await expect(input).toHaveValue('offline_access')
    await userEvent.tab()
    await expect(canvas.getByText('Value: ["read","write","admin","offline_access"]')).toBeVisible()
    await userEvent.click(canvas.getByRole('button', { name: 'Remove write' }))
    await expect(canvas.getByText('Value: ["read","admin","offline_access"]')).toBeVisible()
  },
}

export const EnterAddsWithoutSubmitting: Story = {
  args: { onChange: () => {}, value: [] },
  render: () => <InForm />,
  play: async ({ canvas, userEvent }) => {
    await userEvent.type(canvas.getByRole('combobox'), 'read{Enter}')
    await expect(canvas.getByText('Value: ["read"]')).toBeVisible()
    await expect(canvas.getByText('Submitted 0')).toBeVisible()
    // With nothing typed, Enter submits the form as usual.
    await userEvent.keyboard('{Enter}')
    await expect(canvas.getByText('Submitted 1')).toBeVisible()
  },
}

export const ChooseSuggestion: Story = {
  args: { onChange: () => {}, value: [] },
  render: () => <Controlled value={['read']} suggestions={['read', 'write', 'admin']} />,
  play: async ({ canvas, canvasElement, userEvent }) => {
    await userEvent.type(canvas.getByRole('combobox'), 'wr')
    // The list renders in a portal outside the story's canvas.
    const popup = within(canvasElement.ownerDocument.body)
    await expect(popup.queryByRole('option', { name: 'read' })).toBeNull()
    await userEvent.click(await popup.findByRole('option', { name: 'write' }))
    // The text typed to find the suggestion does not become a token of its own.
    await expect(canvas.getByText('Value: ["read","write"]')).toBeVisible()
    await expect(canvas.getByRole('combobox')).toHaveValue('')
  },
}

// An open popup hides the rest of the page from assistive technology, so a
// list with nothing left to offer stays closed.
export const NothingLeftToSuggest: Story = {
  args: { onChange: () => {}, value: [] },
  render: () => (
    <div>
      <Controlled value={['read']} suggestions={['read', 'write']} />
      <button type="button">Next control</button>
    </div>
  ),
  play: async ({ canvas, userEvent }) => {
    const input = canvas.getByRole('combobox')
    await userEvent.type(input, 'wr')
    await expect(input).toHaveAttribute('aria-expanded', 'true')
    await userEvent.type(input, 'ite ')
    await expect(canvas.getByText('Value: ["read","write"]')).toBeVisible()
    await expect(input).toHaveAttribute('aria-expanded', 'false')
    await expect(canvas.getByRole('button', { name: 'Next control' })).toBeVisible()
    // Text that matches no suggestion keeps it closed too.
    await userEvent.type(input, 'admin')
    await expect(input).toHaveAttribute('aria-expanded', 'false')
  },
}

// Catches: text that matched a suggestion and then stopped matching being wiped
// as the list hides. It takes typing at a person's pace for the list to close.
export const TypeAtHumanSpeed: Story = {
  args: { onChange: () => {}, value: [] },
  render: () => <Controlled suggestions={['read', 'write']} />,
  play: async ({ canvas }) => {
    const slow = user.setup({ delay: 50 })
    // "c" matches nothing; "r" matches both suggestions, and "rx" neither.
    await slow.type(canvas.getByRole('combobox'), 'rx{Enter}custom{Enter}')
    await expect(canvas.getByText('Value: ["rx","custom"]')).toBeVisible()
  },
}

// Catches: a token too long for its chip, with no shorter label, that you
// cannot read in full.
export const LongTokenTooltip: Story = {
  args: { onChange: () => {}, value: [] },
  render: () => (
    <Controlled value={['read', 'https://graph.microsoft.com/TeamSettings.ReadWrite.All']} />
  ),
  play: async ({ canvas, userEvent }) => {
    await userEvent.hover(
      canvas.getByText('https://graph.microsoft.com/TeamSettings.ReadWrite.All'),
    )
    await waitFor(() =>
      expect(
        screen.getAllByText('https://graph.microsoft.com/TeamSettings.ReadWrite.All'),
      ).toHaveLength(2),
    )
    // A token that fits its chip shows no tooltip.
    await userEvent.unhover(
      canvas.getByText('https://graph.microsoft.com/TeamSettings.ReadWrite.All'),
    )
    await userEvent.hover(canvas.getByText('read'))
    await new Promise((resolve) => setTimeout(resolve, 600))
    await expect(screen.getAllByText('read')).toHaveLength(1)
  },
}
