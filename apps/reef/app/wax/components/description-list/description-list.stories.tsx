import type { Meta, StoryObj } from '@storybook/react-vite'

import { CodeBlock } from '@/wax/components/code-block'

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
