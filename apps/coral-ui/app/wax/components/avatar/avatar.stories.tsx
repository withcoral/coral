import type { Meta, StoryObj } from '@storybook/react-vite'

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
