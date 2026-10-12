import type { Meta, StoryObj } from '@storybook/react-vite'

import { Skeleton } from './skeleton'

const meta = {
  argTypes: {
    borderRadius: {
      control: 'text',
    },
    height: {
      control: 'text',
    },
    width: {
      control: 'text',
    },
  },
  component: Skeleton,
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Wax/Skeleton',
} satisfies Meta<typeof Skeleton>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    height: '20px',
    width: '200px',
  },
}

export const Circle: Story = {
  args: {
    borderRadius: '50%',
    height: '40px',
    width: '40px',
  },
}

export const Rounded: Story = {
  args: {
    borderRadius: '8px',
    height: '60px',
    width: '300px',
  },
}

export const TextBlock: Story = {
  render: () => (
    <div style={{ width: '400px' }}>
      <Skeleton height="20px" style={{ marginBottom: '8px' }} width="100%" />
      <Skeleton height="20px" style={{ marginBottom: '8px' }} width="90%" />
      <Skeleton height="20px" width="75%" />
    </div>
  ),
}

export const CardLayout: Story = {
  render: () => (
    <div style={{ padding: '16px', width: '300px' }}>
      <div style={{ display: 'flex', gap: '12px', marginBottom: '16px' }}>
        <Skeleton borderRadius="50%" height="48px" width="48px" />
        <div style={{ flex: 1 }}>
          <Skeleton height="16px" style={{ marginBottom: '8px' }} width="70%" />
          <Skeleton height="14px" width="50%" />
        </div>
      </div>
      <Skeleton height="12px" style={{ marginBottom: '6px' }} width="100%" />
      <Skeleton height="12px" style={{ marginBottom: '6px' }} width="95%" />
      <Skeleton height="12px" width="85%" />
    </div>
  ),
}

export const ListItems: Story = {
  render: () => (
    <div style={{ width: '400px' }}>
      {[1, 2, 3, 4].map((i) => (
        <div key={i} style={{ display: 'flex', gap: '12px', marginBottom: '12px', padding: '8px' }}>
          <Skeleton borderRadius="4px" height="24px" width="24px" />
          <div style={{ flex: 1 }}>
            <Skeleton height="16px" width="60%" />
          </div>
        </div>
      ))}
    </div>
  ),
}

export const AllSizes: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
      <Skeleton height="12px" width="150px" />
      <Skeleton height="16px" width="200px" />
      <Skeleton height="20px" width="250px" />
      <Skeleton height="24px" width="300px" />
      <Skeleton height="32px" width="350px" />
    </div>
  ),
}
