import type { Meta, StoryObj } from '@storybook/react-vite'

import { Button } from '@/wax/components'

import { EmptyPage } from './empty-page'

const meta = {
  component: EmptyPage,
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Components/EmptyPage',
} satisfies Meta<typeof EmptyPage>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    action: <Button.TextButton variant="secondary">Reset filters</Button.TextButton>,
    description: 'Try adjusting your search or filters.',
    iconName: 'Search',
    title: 'No matching results',
  },
}

export const WithoutAction: Story = {
  args: {
    description: 'All is well and there are no alerts.',
    iconName: 'Bell',
    title: 'No alerts yet',
  },
}

export const LongDescription: Story = {
  args: {
    description:
      'This is a longer description that explains in more detail what the user can do to populate this table with data.',
    iconName: 'Search',
    title: 'No data available',
  },
}
