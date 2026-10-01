import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'

import { Switch } from './switch'

const meta = {
  component: Switch,
  title: 'Wax/Inputs/Switch',
} satisfies Meta<typeof Switch>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    'aria-label': 'Toggle setting',
  },
}

export const Disabled: Story = {
  args: {
    'aria-label': 'Toggle setting',
    disabled: true,
  },
}

export const Controlled: Story = {
  render: () => {
    const [checked, setChecked] = useState(false)
    return (
      <div style={{ alignItems: 'center', display: 'flex', gap: 8 }}>
        <Switch aria-label="Toggle setting" checked={checked} onCheckedChange={setChecked} />
        <span>{checked ? 'On' : 'Off'}</span>
      </div>
    )
  },
}
