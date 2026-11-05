import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState } from 'react'
import { expect, userEvent } from 'storybook/test'

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
  { value: 'allow', label: 'Allow', icon: 'CircleCheck' },
  { value: 'approve', label: 'Ask', icon: 'Hand' },
  { value: 'disable', label: 'Block', icon: 'Ban' },
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
export const SmallIcons: StoryObj<typeof IconDemo> = {
  render: () => <IconDemo size="small" />,
  // Catches: a style for labelled segments that pushes an icon off its segment's centre.
  play: async ({ canvas }) => {
    for (const radio of canvas.getAllByRole('radio')) {
      const segment = radio.getBoundingClientRect()
      const icon = radio.querySelector('svg')!.getBoundingClientRect()
      const offset = icon.left + icon.width / 2 - (segment.left + segment.width / 2)
      await expect(Math.abs(offset)).toBeLessThan(0.5)
    }
  },
}

function FilterDemo() {
  const [value, setValue] = useState<'all' | (typeof accessOptions)[number]['value']>('all')
  const counts = { allow: 26, approve: 12, disable: 0 }
  return (
    <SegmentedControl
      label="Filter by access"
      size="small"
      value={value}
      onChange={setValue}
      options={[
        { value: 'all', label: 'All', count: 38 },
        ...accessOptions.map((option) => ({
          ...option,
          withLabel: true,
          count: counts[option.value],
          disabled: counts[option.value] === 0,
        })),
      ]}
    />
  )
}

// A filter names each choice with its icon and words, and counts what it shows.
// A choice with nothing to show is disabled.
export const LabelledIconsWithCounts: StoryObj<typeof FilterDemo> = {
  render: () => <FilterDemo />,
  // Catches: a count missing from a segment's name, or a dead end left choosable.
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('radio', { name: 'Ask 12' })).toBeEnabled()
    await expect(canvas.getByRole('radio', { name: 'Block 0' })).toBeDisabled()
    await userEvent.click(canvas.getByRole('radio', { name: 'Allow 26' }))
    await expect(canvas.getByRole('radio', { name: 'Allow 26' })).toBeChecked()
  },
}
