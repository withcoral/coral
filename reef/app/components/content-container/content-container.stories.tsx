import type { Meta, StoryObj } from '@storybook/react-vite'

import { Typography } from '@/wax/components/typography'
import { theme } from '@/wax/theme/theme.css'

import { ContentContainer } from './content-container'

const meta = {
  component: ContentContainer,
  parameters: {
    layout: 'fullscreen',
  },
  render: (args) => (
    <div
      style={{
        backgroundColor: theme.surface.main,
        display: 'flex',
        minHeight: '100dvh',
        paddingLeft: 72,
      }}
    >
      <ContentContainer {...args} />
    </div>
  ),
  tags: ['autodocs'],
  title: 'Components/ContentContainer',
} satisfies Meta<typeof ContentContainer>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    children: (
      <section style={{ display: 'flex', flexDirection: 'column', gap: 8, padding: 24 }}>
        <Typography.HeadingMedium as="h2">Page content</Typography.HeadingMedium>
        <Typography.Body as="p" variant="secondary">
          ContentContainer owns the bordered main page frame used by app routes.
        </Typography.Body>
      </section>
    ),
  },
}
