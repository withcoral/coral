import type { Meta, StoryObj } from '@storybook/react-vite'

import { Tooltip } from '@/wax/components/tooltip'

import { KeyboardHint } from './keyboard-hint'

const meta: Meta<typeof KeyboardHint> = {
  component: KeyboardHint,
  title: 'Wax/KeyboardHint',
}

export default meta
type Story = StoryObj<typeof KeyboardHint>

export const Default: Story = {
  args: {
    shortcut: 'mod+k',
  },
}

export const AllModifiers: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="mod" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>mod (Cmd on Mac, Ctrl on Windows)</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="alt" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>alt (Option on Mac)</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="shift" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>shift</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="ctrl" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>ctrl</span>
      </div>
    </div>
  ),
}

export const SpecialKeys: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="enter" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>enter</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="backspace" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>backspace</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="tab" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>tab</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="esc" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>esc</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="space" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>space</span>
      </div>
    </div>
  ),
}

export const ArrowKeys: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="up" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>up</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="down" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>down</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="left" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>left</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="right" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>right</span>
      </div>
    </div>
  ),
}

export const CommonShortcuts: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="mod+k" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>Search</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="mod+s" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>Save</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="mod+shift+p" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>Command Palette</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="mod+b" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>Toggle sidebar</span>
      </div>
      <div style={{ alignItems: 'center', display: 'flex', gap: '8px' }}>
        <KeyboardHint shortcut="mod+alt+n" />
        <span style={{ fontSize: '12px', opacity: 0.6 }}>New window</span>
      </div>
    </div>
  ),
}

export const ArrayInput: Story = {
  args: {
    shortcut: ['mod', 'shift', 'k'],
  },
}

export const InTooltip: Story = {
  render: () => (
    <div style={{ padding: '40px' }}>
      <Tooltip
        content={
          <>
            Toggle sidebar <KeyboardHint shortcut="mod+b" />
          </>
        }
      >
        <button style={{ padding: '8px 16px' }}>Hover me</button>
      </Tooltip>
    </div>
  ),
}
