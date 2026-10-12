import type { Meta, StoryObj } from '@storybook/react-vite'

import { expect } from 'storybook/test'

import { Avatar } from './avatar'

const meta = {
  component: Avatar,
  parameters: {
    backgrounds: { default: 'dark' },
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Wax/Avatar',
} satisfies Meta<typeof Avatar>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    name: 'John Doe',
  },
  render: () => (
    <div style={{ alignItems: 'center', display: 'flex', flexWrap: 'wrap', gap: 16 }}>
      <Avatar name="John Doe" src="https://i.pravatar.cc/150?u=john" />
      <Avatar name="Jane Smith" />
      <Avatar name="Alice" />
      <Avatar name="Bob" />
      <Avatar name="Charlie" />
      <Avatar name="Diana" />
      <Avatar name="Eve" />
      <Avatar name="Frank" />
      <Avatar name="Grace" />
      <Avatar name="Henry" />
      <Avatar name="Ivy" />
      <Avatar name="Jack" />
      <Avatar name="Kate" />
      <Avatar name="Leo" />
      <Avatar name="Mia" />
    </div>
  ),
}
export const Square: Story = {
  args: { name: 'Operations team', shape: 'square', size: '28' },
  render: () => (
    <div style={{ alignItems: 'center', display: 'flex', gap: 16 }}>
      <Avatar name="Operations team" shape="square" size="28" />
      <Avatar name="Research" shape="square" size="28" />
      <Avatar name="Growth" shape="square" size="28" />
    </div>
  ),
}

/**
 * Beside a visible name, the fallback initial is decorative. A screen reader
 * reads the name once, not the initial and then the name.
 */
export const BesideName: Story = {
  args: { name: 'Ada Lovelace' },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole('button', { name: 'Ada Lovelace' })).toBeVisible()
  },
  render: () => (
    <button style={{ alignItems: 'center', display: 'flex', gap: 8 }} type="button">
      <Avatar name="Ada Lovelace" />
      Ada Lovelace
    </button>
  ),
}
