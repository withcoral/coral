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
  // Catches: a place given a person's two initials.
  play: async ({ canvas }) => {
    for (const letter of ['O', 'R', 'G']) await expect(canvas.getByText(letter)).toBeVisible()
  },
}

// Two letters tell one person from another better than one does.
export const TwoLetters: Story = {
  args: { name: 'James Summerfield' },
  render: () => (
    <div style={{ alignItems: 'center', display: 'flex', gap: 16 }}>
      <Avatar name="James Summerfield" />
      <Avatar name="James Summerfield" size="28" />
      <Avatar name="Ada Byron Lovelace" size="28" />
      <Avatar name="alex@example.com" size="28" />
      <Avatar name="Jane Doe (Acme)" size="28" />
      <Avatar name="@alex" size="28" />
    </div>
  ),
  // Catches: initials from a middle name or from punctuation, or one word losing its second letter.
  play: async ({ canvas }) => {
    await expect(canvas.getAllByText('JS')).toHaveLength(2)
    for (const initials of ['AL', 'al', 'JD', '@a'])
      await expect(canvas.getByText(initials)).toBeVisible()
  },
}

/**
 * Beside a visible name, the fallback initials are decorative. A screen reader
 * reads the name once, not the initials and then the name.
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
