import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { expect, userEvent } from 'storybook/test'

import { SegmentedControl, SegmentedToggles } from './segmented-control'

const options = [
  { value: 'url', label: 'OpenAPI URL' },
  { value: 'file', label: 'OpenAPI file' },
  { value: 'mcp', label: 'MCP server' },
] as const

function Demo(props: { disabled?: boolean; initial?: (typeof options)[number]['value'] }) {
  const [value, setValue] = useState<(typeof options)[number]['value']>(props.initial ?? 'url')
  return (
    <SegmentedControl
      label="Provider source"
      options={options}
      value={value}
      onChange={setValue}
      name="source"
      disabled={props.disabled}
    />
  )
}

const meta = {
  component: Demo,
  title: 'Components/SegmentedControl',
} satisfies Meta<typeof Demo>
export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
export const SecondSelected: Story = { args: { initial: 'file' } }
export const Disabled: Story = { args: { disabled: true } }

const accessOptions = [
  { value: 'allow', label: 'Always allow', icon: 'CircleCheck' },
  { value: 'approve', label: 'Needs approval', icon: 'Hand' },
  { value: 'disable', label: 'Disabled', icon: 'Ban' },
] as const

function IconDemo({ size }: { size?: 'default' | 'small' }) {
  const [value, setValue] = useState<(typeof accessOptions)[number]['value']>('approve')
  return (
    <SegmentedControl
      label="Access"
      options={accessOptions}
      value={value}
      onChange={setValue}
      size={size}
    />
  )
}

// Icon segments name themselves with their label and show it as a tooltip.
export const Icons: StoryObj<typeof IconDemo> = { render: () => <IconDemo /> }
export const SmallIcons: StoryObj<typeof IconDemo> = { render: () => <IconDemo size="small" /> }

function TotalsDemo() {
  const [pressed, setPressed] = useState<(typeof accessOptions)[number]['value'] | null>(null)
  const counts = { allow: 22, approve: 9, disable: 0 }
  return (
    <div style={{ display: 'grid', gap: 8, justifyItems: 'start' }}>
      <SegmentedToggles
        label="Tools by access"
        size="small"
        pressed={pressed}
        onToggle={(value) => setPressed((current) => (current === value ? null : value))}
        options={accessOptions.map(({ value, label }) => ({
          value,
          text: String(counts[value]),
          label: `${counts[value]} ${label}`,
          tooltip: `Show only ${label}.`,
        }))}
      />
      <IconDemo size="small" />
    </div>
  )
}

// Totals line up over the icon segments they count; a pressed total is raised.
export const TogglesOverIcons: StoryObj<typeof TotalsDemo> = {
  render: () => <TotalsDemo />,
  // Catches: a segment that does not report being pressed, or stays pressed.
  play: async ({ canvas }) => {
    const toggle = canvas.getByRole('button', { name: '9 Needs approval' })
    await expect(toggle).toHaveAttribute('aria-pressed', 'false')
    await userEvent.click(toggle)
    await expect(toggle).toHaveAttribute('aria-pressed', 'true')
    await userEvent.click(toggle)
    await expect(toggle).toHaveAttribute('aria-pressed', 'false')
  },
}
