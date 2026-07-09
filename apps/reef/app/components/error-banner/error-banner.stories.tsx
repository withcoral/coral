import type { Meta, StoryObj } from '@storybook/react-vite'

import { ErrorBanner } from './error-banner'

const meta = {
  component: ErrorBanner,
  tags: ['autodocs'],
  title: 'Components/ErrorBanner',
} satisfies Meta<typeof ErrorBanner>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    message: 'The source catalog could not be loaded.',
    title: 'Could not load sources',
  },
}

export const WithRetry: Story = {
  args: {
    message: 'Retry after the local Coral runtime is ready.',
    onRetry: () => undefined,
    title: 'Runtime unavailable',
  },
}
