import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'

import { SegmentedControl } from './segmented-control'

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
