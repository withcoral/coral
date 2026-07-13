import type { Meta, StoryObj } from '@storybook/react-vite'

import { createMemoryRouter, RouterProvider } from 'react-router'

import { Typography } from '@/wax/components/typography'
import { theme } from '@/wax/theme/theme.css'

import { Sidebar } from './sidebar'

const meta = {
  component: Sidebar,
  decorators: [
    (Story, context) => {
      const router = createMemoryRouter(
        [
          {
            children: [{ index: true, element: <div /> }],
            element: <Story />,
            path: '/',
          },
        ],
        {
          initialEntries: [context.parameters.initialEntry ?? '/'],
        },
      )

      return <RouterProvider router={router} />
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

export const Minimized: Story = {
  args: {
    initialIsMinimized: true,
    workspaces: [{ name: 'default' }, { name: 'analytics' }],
  },
}
