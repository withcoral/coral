import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { expect, fireEvent, waitFor } from 'storybook/test'

import { TextArea } from './text-area'

const meta = {
  args: {
    placeholder: 'Describe this source…',
  },
  component: TextArea,
  decorators: [
    (Story) => (
      <div style={{ width: 360 }}>
        <Story />
      </div>
    ),
  ],
  title: 'Wax/Inputs/TextArea',
} satisfies Meta<typeof TextArea>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  play: async ({ canvas, userEvent }) => {
    const textArea = canvas.getByRole('textbox')

    await expect(textArea).toHaveAttribute('tabindex', '0')
    await userEvent.type(textArea, 'Editable text')
    await expect(textArea).toHaveValue('Editable text')
  },
}

export const Disabled: Story = {
  args: {
    disabled: true,
    value: 'This description cannot be edited.',
  },
  play: async ({ canvas }) => {
    const textArea = canvas.getByRole('textbox')

    await expect(textArea).toBeDisabled()
    await expect(textArea).toHaveAttribute('data-disabled')
  },
}

export const Controlled: Story = {
  render: () => {
    const [value, setValue] = useState('A longer description that can span multiple lines.')
    return <TextArea onChange={setValue} value={value} />
  },
}

export const Overflow: Story = {
  args: {
    rows: 4,
    value: Array.from(
      { length: 12 },
      (_, index) => `Line ${index + 1}: Source descriptions can contain detailed setup guidance.`,
    ).join('\n'),
  },
  play: async ({ canvas }) => {
    const textArea = canvas.getByRole('textbox')

    await waitFor(() => expect(textArea.scrollHeight).toBeGreaterThan(textArea.clientHeight))
    await expect(getComputedStyle(textArea).overflowX).toBe('hidden')
    await expect(textArea).toHaveAttribute('data-overflow-y-end')
    await expect(getComputedStyle(textArea).maskImage).toContain('linear-gradient')

    textArea.scrollTop = textArea.scrollHeight
    await fireEvent.scroll(textArea)
    await waitFor(() => expect(textArea).toHaveAttribute('data-overflow-y-start'))
  },
}
