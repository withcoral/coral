import type { Meta, StoryObj } from '@storybook/react-vite'

import { ErrorMessage } from './error-message'

const meta = {
  component: ErrorMessage,
  title: 'Wax/ErrorMessage',
} satisfies Meta<typeof ErrorMessage>

export default meta
type Story = StoryObj<typeof ErrorMessage>

export const Default: Story = {
  args: {
    message: 'Something went wrong. Please try again.',
  },
}
