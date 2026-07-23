import type { Meta, StoryObj } from '@storybook/react-vite'

import { Button } from '@/wax/components'

import { Banner } from './banner'

const meta = {
  component: Banner,
  tags: ['autodocs'],
  title: 'Wax/Banner',
} satisfies Meta<typeof Banner>

export default meta
type Story = StoryObj<typeof meta>

export const Variants: Story = {
  args: {
    children: 'Banner',
  },
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12, maxWidth: 720 }}>
      <Banner title="Sync in progress">New data will appear here when it is available.</Banner>
      <Banner title="Could not load sources" variant="error">
        Try again after the local Coral runtime is ready.
      </Banner>
      <Banner title="Workspace created" variant="success">
        The new workspace is ready to use.
      </Banner>
      <Banner
        action={
          <Button.Container size="22" variant="secondary">
            <Button.Text>Review</Button.Text>
          </Button.Container>
        }
        title="Setup incomplete"
        variant="warning"
      >
        This source needs attention before it can run.
      </Banner>
    </div>
  ),
}

export const LongText: Story = {
  args: {
    action: (
      <Button.Container size="22" variant="secondary">
        <Button.Text>View details</Button.Text>
      </Button.Container>
    ),
    children:
      'Coral could not finish loading this source because the provider returned an unexpected response. Check the source configuration and try again after the provider is available.',
    title: 'The source could not be loaded',
    variant: 'error',
  },
  render: (args) => (
    <div style={{ maxWidth: 720 }}>
      <Banner {...args} />
    </div>
  ),
}
