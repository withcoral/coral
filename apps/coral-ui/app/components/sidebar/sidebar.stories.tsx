import type { Meta, StoryObj } from '@storybook/react-vite'

import { createRoutesStub } from 'react-router'

import { Typography } from '@/wax/components/typography'
import { theme } from '@/wax/theme/theme.css'

import { Sidebar } from './sidebar'

const meta = {
  component: Sidebar,
  decorators: [
    (Story, context) => {
      const RoutesStub = createRoutesStub([
        {
          children: [
            { index: true, Component: () => null },
            { Component: () => null, path: 'settings' },
          ],
          Component: () => <Story />,
          path: '/',
        },
      ])

      return <RoutesStub initialEntries={[context.parameters.initialEntry ?? '/']} />
    },
  ],
  parameters: {
    layout: 'fullscreen',
  },
  render: (args) => (
    <div
      style={{
        backgroundColor: theme.surface.mainContent,
        display: 'flex',
        minHeight: '100dvh',
      }}
    >
      <Sidebar {...args} />
      <main style={{ flex: 1, padding: 24 }}>
        <Typography.HeadingMedium>Content area</Typography.HeadingMedium>
      </main>
    </div>
  ),
  tags: ['autodocs'],
  title: 'Components/Sidebar',
} satisfies Meta<typeof Sidebar>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    initialIsMinimized: false,
    workspaces: [{ name: 'default' }, { name: 'analytics' }],
  },
}

export const Settings: Story = {
  args: {
    initialIsMinimized: false,
    workspaces: [{ name: 'default' }, { name: 'analytics' }],
  },
  parameters: {
    initialEntry: '/settings',
  },
}
