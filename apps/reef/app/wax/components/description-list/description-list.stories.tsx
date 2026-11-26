import type { Meta, StoryObj } from '@storybook/react-vite'

import { CodeBlock } from '@/wax/components/code-block'
import { Picker } from '@/wax/components/picker'

import * as DescriptionList from './index'

const meta = {
  title: 'Components/DescriptionList',
  render: () => (
    <DescriptionList.List title="Context" heading="h3">
      <DescriptionList.Row label="Task intent">Send overdue invoice reminders</DescriptionList.Row>
      <DescriptionList.Row label="Requested by">Collections agent · Finance</DescriptionList.Row>
      <DescriptionList.Row label="Recipients">
        <CodeBlock value='["owner@example.com","billing@example.com"]' format="json" />
      </DescriptionList.Row>
    </DescriptionList.List>
  ),
} satisfies Meta
export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
export const Narrow: Story = { parameters: { chromatic: { viewports: [390] } } }
export const Untitled: Story = {
  render: () => (
    <DescriptionList.List>
      <DescriptionList.Row label="Workspace">workspaces/finance</DescriptionList.Row>
    </DescriptionList.List>
  ),
}
export const Compact: Story = {
  parameters: { chromatic: { viewports: [390, 1280] } },
  render: () => (
    <DescriptionList.List title="Settings" heading="h3" density="compact">
      <DescriptionList.Row label="Size">
        <Picker
          ariaLabel="Size: Medium"
          options={[
            { value: 'small', label: 'Small' },
            { value: 'medium', label: 'Medium' },
          ]}
          value="medium"
          onChange={() => {}}
        />
      </DescriptionList.Row>
      <DescriptionList.Row label="Cloud">AWS</DescriptionList.Row>
      <DescriptionList.Row label="Region">eu-west-1</DescriptionList.Row>
    </DescriptionList.List>
  ),
}
/** Amounts that add up: the total row closes the sum. */
export const Total: Story = {
  render: () => (
    <DescriptionList.List title="Invoice" heading="h3">
      <DescriptionList.Row label="Subscription">€1,200.00</DescriptionList.Row>
      <DescriptionList.Row label="Usage">€84.50</DescriptionList.Row>
      <DescriptionList.Row label="Credit">−€100.00</DescriptionList.Row>
      <DescriptionList.Row label="Total" total>
        €1,184.50
      </DescriptionList.Row>
    </DescriptionList.List>
  ),
}
