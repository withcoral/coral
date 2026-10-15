import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'
import { expect, waitFor } from 'storybook/test'
import { Button } from '@/wax/components'
import { darkTheme } from '@/wax/theme/theme-dark.css'

import { SidebarButton } from './sidebar-button'

const meta = {
  component: SidebarButton,
  decorators: [
    (Story) => (
      <div className={darkTheme} style={{ backgroundColor: '#1C1C1C', padding: 24 }}>
        <Story />
      </div>
    ),
  ],
  parameters: {
    backgrounds: { default: 'dark' },
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Wax/SidebarButton',
} satisfies Meta<typeof SidebarButton>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    children: 'Investigations',
    icon: 'Waypoints',
  },
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, width: 200 }}>
      <SidebarButton icon="CirclePlus" variant="accent">
        New investigation
      </SidebarButton>
      <div style={{ height: 8 }} />
      <SidebarButton icon="Waypoints" isActive>
        Investigations
      </SidebarButton>
      <SidebarButton icon="Folder">Incidents</SidebarButton>
      <SidebarButton icon="Chart">Analytics</SidebarButton>
      <SidebarButton icon="Settings">Settings</SidebarButton>
      <div style={{ height: 8 }} />
      <span style={{ color: '#999', fontSize: 12 }}>Disabled</span>
      <SidebarButton disabled icon="Lock">
        Admin
      </SidebarButton>
    </div>
  ),
}

export const AccentVariant: Story = {
  args: {
    children: 'New investigation',
    icon: 'CirclePlus',
    variant: 'accent',
  },
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, width: 200 }}>
      <SidebarButton icon="CirclePlus" variant="accent">
        New investigation
      </SidebarButton>
      <SidebarButton icon="CirclePlus" isActive variant="accent">
        New investigation (active)
      </SidebarButton>
      <SidebarButton disabled icon="CirclePlus" variant="accent">
        New investigation (disabled)
      </SidebarButton>
    </div>
  ),
}

export const DefaultVariant: Story = {
  args: {
    children: 'Investigations',
    icon: 'Waypoints',
  },
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, width: 200 }}>
      <SidebarButton icon="Waypoints">Investigations</SidebarButton>
      <SidebarButton icon="Waypoints" isActive>
        Investigations (active)
      </SidebarButton>
      <SidebarButton disabled icon="Waypoints">
        Investigations (disabled)
      </SidebarButton>
    </div>
  ),
}

// Quiet at rest; the current entry reads as strongly as any other.
export const MutedVariant: Story = {
  args: {
    children: 'Archived',
    icon: 'Archive',
    variant: 'muted',
  },
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, width: 200 }}>
      <SidebarButton icon="Archive" variant="muted">
        Archived
      </SidebarButton>
      <SidebarButton icon="Archive" isActive variant="muted">
        Archived (active)
      </SidebarButton>
    </div>
  ),
}

function PendingActions() {
  const [count, setCount] = useState(0)
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, width: 200 }}>
      {/* An empty badge is no badge, so clearing it to '' pops it out too. */}
      <SidebarButton icon="Activity" badge={count ? String(count) : ''}>
        Activity
      </SidebarButton>
      <Button.TextButton variant="secondary" onClick={() => setCount(count + 1)}>
        Add action
      </Button.TextButton>
      <Button.TextButton variant="secondary" onClick={() => setCount(0)}>
        Clear actions
      </Button.TextButton>
    </div>
  )
}

export const BadgeArrivesAndLeaves: Story = {
  args: { children: 'Activity', icon: 'Activity' },
  play: async ({ canvas, userEvent }) => {
    const activity = canvas.getByRole('button', { name: 'Activity' })
    const height = activity.getBoundingClientRect().height
    await userEvent.click(canvas.getByRole('button', { name: 'Add action' }))
    const badge = await canvas.findByText('1')
    // An arriving badge pops in, and never moves the rows below it.
    await expect(badge.getAnimations().length).toBeGreaterThan(0)
    await expect(activity.getBoundingClientRect().height).toBe(height)
    await userEvent.click(canvas.getByRole('button', { name: 'Clear actions' }))
    // A leaving badge keeps its count while it pops out, then goes.
    await expect(activity.textContent).toContain('1')
    await waitFor(() => expect(activity.textContent).toBe('Activity'))
  },
  render: () => <PendingActions />,
}
