import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { expect, waitFor } from 'storybook/test'

import { Radio } from '@/wax/components'

const meta = {
  parameters: {
    layout: 'centered',
  },
  title: 'Wax/Radio',
} satisfies Meta

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  render: () => {
    const [value, setValue] = useState('openapi')
    return (
      <Radio.Group aria-label="Source type" value={value} onValueChange={setValue}>
        <Radio.Item value="openapi">REST API (OpenAPI)</Radio.Item>
        <Radio.Item value="mcp">MCP server</Radio.Item>
      </Radio.Group>
    )
  },
  play: async ({ canvas, userEvent }) => {
    await expect(canvas.getByRole('radiogroup', { name: 'Source type' })).toBeInTheDocument()

    const openapi = canvas.getByRole('radio', { name: 'REST API (OpenAPI)' })
    const mcp = canvas.getByRole('radio', { name: 'MCP server' })
    await expect(openapi).toBeChecked()
    await expect(openapi.getBoundingClientRect().width).toBe(18)

    openapi.focus()
    await expect(getComputedStyle(openapi.closest('label')!).outlineStyle).toBe('solid')
    await userEvent.keyboard('{ArrowRight}')
    await expect(mcp).toBeChecked()
  },
}

export const DisabledItem: Story = {
  render: () => (
    <Radio.Group aria-label="Authentication" defaultValue="none">
      <Radio.Item value="none">None</Radio.Item>
      <Radio.Item value="bearer">Bearer token</Radio.Item>
      <Radio.Item disabled value="header">
        Custom header
      </Radio.Item>
    </Radio.Group>
  ),
  play: async ({ canvas, userEvent }) => {
    const none = canvas.getByRole('radio', { name: 'None' })
    const bearer = canvas.getByRole('radio', { name: 'Bearer token' })
    const header = canvas.getByRole('radio', { name: 'Custom header' })

    await expect(none).toBeChecked()
    await userEvent.click(header)
    await expect(none).toBeChecked()
    await expect(header).not.toBeChecked()

    await userEvent.click(bearer)
    await expect(bearer).toBeChecked()
  },
}

const authenticationOptions = [
  'None',
  'API key',
  'Bearer token',
  'Basic auth',
  'OAuth 2.0',
  'Custom header',
  'AWS SigV4',
]

export const ManyOptions: Story = {
  render: () => (
    <div style={{ width: 320 }}>
      <ManyOptionsExample />
    </div>
  ),
  play: async ({ canvas, userEvent }) => {
    const group = canvas.getByRole('radiogroup', { name: 'Authentication type' })
    const viewport = group.closest<HTMLElement>('[data-id$="-viewport"]')

    await expect(viewport).toBeDefined()
    await expect(viewport?.scrollWidth).toBeGreaterThan(viewport?.clientWidth ?? 0)
    await expect(getComputedStyle(viewport!).overflowY).toBe('hidden')
    await expect(getComputedStyle(viewport!, '::after').backgroundImage).toContain(
      'linear-gradient',
    )

    const none = canvas.getByRole('radio', { name: 'None' })
    const sigV4 = canvas.getByRole('radio', { name: 'AWS SigV4' })
    none.focus()
    await userEvent.keyboard(
      '{ArrowRight}{ArrowRight}{ArrowRight}{ArrowRight}{ArrowRight}{ArrowRight}',
    )
    await expect(sigV4).toBeChecked()
    await waitFor(() => expect(viewport?.scrollLeft).toBeGreaterThan(0))
  },
}

function ManyOptionsExample() {
  const [value, setValue] = useState(authenticationOptions[0])
  return (
    <Radio.Group aria-label="Authentication type" onValueChange={setValue} value={value}>
      {authenticationOptions.map((option) => (
        <Radio.Item key={option} value={option}>
          {option}
        </Radio.Item>
      ))}
    </Radio.Group>
  )
}
