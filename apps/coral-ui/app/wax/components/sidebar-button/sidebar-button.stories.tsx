import type { Meta, StoryObj } from '@storybook/react-vite'

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
