import type { Meta, StoryObj } from '@storybook/react-vite'

import { ToolName } from './tool-name'

const meta = {
  component: ToolName,
  title: 'Components/ToolName',
  args: { children: 'saveComment', provider: { name: 'Linear' } },
  decorators: [
    (Story) => (
      <div style={{ width: 220 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof ToolName>
export default meta
type Story = StoryObj<typeof meta>

export const WithProvider: Story = {}
// Under its provider's heading, the name stands alone.
export const NameOnly: Story = { args: { provider: undefined } }
// In a line of metadata.
export const Small: Story = { args: { size: 'small' } }
// A name wider than its cell ends in an ellipsis; the full name is on hover.
export const Truncated: Story = {
  args: {
    children: 'coral.providers.clickstack.clickstackDeleteDashboard',
    title: 'coral.providers.clickstack.clickstackDeleteDashboard',
  },
}
