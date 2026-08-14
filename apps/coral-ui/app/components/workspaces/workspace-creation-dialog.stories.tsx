import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'
import { createRoutesStub } from 'react-router'
import { fn } from 'storybook/test'

import { Button } from '@/wax/components'
import { routePattern } from '@/routing/routemap'

import {
  WorkspaceCreationDialog,
  type WorkspaceCreationDialogProps,
} from './workspace-creation-dialog'

const meta = {
  component: WorkspaceCreationDialog,
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
    layout: 'centered',
  },
  render: (args) => <DialogStory {...args} />,
  tags: ['autodocs'],
  title: 'Components/Workspaces/WorkspaceCreationDialog',
} satisfies Meta<typeof WorkspaceCreationDialog>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    fetcherKey: 'workspace-creation-dialog-story',
    onOpenChange: fn(),
    open: true,
  },
}

function DialogStory({ onOpenChange, open: initialOpen, ...props }: WorkspaceCreationDialogProps) {
  const [open, setOpen] = useState(initialOpen)

  const handleOpenChange = (nextOpen: boolean) => {
    setOpen(nextOpen)
    onOpenChange(nextOpen)
  }

  return (
    <>
      <Button.TextButton onClick={() => handleOpenChange(true)}>Open dialog</Button.TextButton>
      <WorkspaceCreationDialog {...props} onOpenChange={handleOpenChange} open={open} />
    </>
  )
}
