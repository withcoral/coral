import type { Meta, StoryObj } from '@storybook/react-vite'

import { List } from '@/wax/components'
import { IconButton } from '@/wax/components/button'

import { KeyboardShortcut } from './keyboard-shortcut'

const meta = {
  component: KeyboardShortcut,
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Wax/KeyboardShortcut',
} satisfies Meta<typeof KeyboardShortcut>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    handler: () => ({}),
    shortcut: '',
  },
  render: () => (
    <div style={{ padding: '20px' }}>
      <h3 style={{ marginBottom: '16px' }}>Try these keyboard shortcuts:</h3>
      <List.Container style={{ listStyle: 'none', padding: 0 }}>
        <List.Item style={{ marginBottom: '12px' }}>
          <strong>Shift+D:</strong> Modifier + key combination
        </List.Item>
        <List.Item style={{ marginBottom: '12px' }}>
          <strong>Y E E T:</strong> Key sequence (press keys in order)
        </List.Item>
        <List.Item style={{ marginBottom: '12px' }}>
          <strong>Ctrl+D / Cmd+D:</strong> Cross-platform modifier
        </List.Item>
      </List.Container>

      <KeyboardShortcut
        handler={() => {
          alert("The 'Shift' and 'd' keys were pressed at the same time")
        }}
        shortcut="Shift+d"
      />
      <KeyboardShortcut
        handler={() => {
          alert("The keys 'y', 'e', 'e', and 't' were pressed in order")
        }}
        shortcut="y e e t"
      />
      <KeyboardShortcut
        handler={(event) => {
          event.preventDefault()
          alert("Either 'Control+d' or 'Meta+d' were pressed")
        }}
        shortcut="$mod+d"
      />
    </div>
  ),
}

/** When children and tooltipContent are provided, wraps the child with a tooltip showing the shortcut hint */
export const WithTooltip: Story = {
  args: {
    handler: () => ({}),
    shortcut: '',
  },
  render: () => (
    <div style={{ display: 'flex', gap: '16px', padding: '40px' }}>
      <KeyboardShortcut
        handler={(event) => {
          event.preventDefault()
          alert('Toggle sidebar')
        }}
        shortcut="$mod+b"
        tooltipContent="Collapse sidebar"
        tooltipSide="bottom"
      >
        <IconButton ariaLabel="Collapse sidebar" name="PanelLeft" size="32" variant="bare" />
      </KeyboardShortcut>

      <KeyboardShortcut
        handler={(event) => {
          event.preventDefault()
          alert('Search')
        }}
        shortcut="$mod+k"
        tooltipContent="Search"
        tooltipSide="bottom"
      >
        <IconButton ariaLabel="Search" name="Search" size="32" variant="bare" />
      </KeyboardShortcut>

      <KeyboardShortcut
        handler={(event) => {
          event.preventDefault()
          alert('New chat')
        }}
        shortcut="$mod+n"
        tooltipContent="New chat"
        tooltipSide="bottom"
      >
        <IconButton ariaLabel="New chat" name="Plus" size="32" variant="bare" />
      </KeyboardShortcut>
    </div>
  ),
}
