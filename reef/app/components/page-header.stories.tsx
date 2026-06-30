import type { Meta, StoryObj } from '@storybook/react-vite'

import * as Button from '@/wax/components/button'
import { theme } from '@/wax/theme/theme.css'

import { PageHeader } from './page-header'

const meta = {
  component: PageHeader,
  decorators: [
    (Story) => (
      <div style={{ backgroundColor: theme.surface.mainContent, minHeight: 180 }}>
        <Story />
      </div>
    ),
  ],
  title: 'Components/PageHeader',
} satisfies Meta<typeof PageHeader>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    subtitle: '3 schemas / 42 tables',
    title: 'Schema explorer',
  },
}

export const WithActions: Story = {
  args: {
    children: (
      <Button.Container size="32" variant="secondary">
        <Button.Text>Refresh</Button.Text>
      </Button.Container>
    ),
    subtitle: '5 sources connected',
    title: 'Sources',
  },
}
