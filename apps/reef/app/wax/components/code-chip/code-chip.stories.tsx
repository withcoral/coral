import type { Meta, StoryObj } from '@storybook/react-vite'

import { CodeChip } from './code-chip'

const meta = {
  component: CodeChip,
  title: 'Components/CodeChip',
  args: { children: 'coral.providers.github.issues.create' },
} satisfies Meta<typeof CodeChip>
export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
export const Long: Story = {
  args: { children: 'coral.providers.a_very_long_program_name.and.a.deeply.nested.tool.path' },
}
