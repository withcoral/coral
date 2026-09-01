import type { Meta, StoryObj } from '@storybook/react-vite'

import { createRoutesStub } from 'react-router'
import { fn } from 'storybook/test'

import { routePattern } from '@/routing/routemap'

import { CreateFirstWorkspace } from './create-first-workspace'

const meta = {
  component: CreateFirstWorkspace,
  decorators: [
    (Story) => {
      const RoutesStub = createRoutesStub([
        { Component: () => <Story />, path: '/' },
        {
          action: fn(() => null).mockName('createWorkspaceAction'),
          path: routePattern('workspaces'),
        },
      ])

      return <RoutesStub initialEntries={['/']} />
    },
  ],
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  title: 'Components/Workspaces/CreateFirstWorkspace',
} satisfies Meta<typeof CreateFirstWorkspace>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const Pending: Story = {
  args: {
    pending: true,
  },
}

export const WithError: Story = {
  args: {
    error: 'Workspace name may only contain letters, numbers, and hyphens',
  },
}
