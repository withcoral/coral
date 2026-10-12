import type { Meta, StoryObj } from '@storybook/react-vite'
import { MemoryRouter } from 'react-router'

import { ErrorPage } from './error-page'

const meta = {
  component: ErrorPage,
  title: 'Components/ErrorPage',
  parameters: { layout: 'fullscreen' },
  decorators: [
    (Story) => (
      <MemoryRouter>
        <Story />
      </MemoryRouter>
    ),
  ],
  args: {
    title: 'Coral cannot open this page',
    details: 'Coral encountered an unexpected error. Reload the page.',
  },
} satisfies Meta<typeof ErrorPage>
export default meta
type Story = StoryObj<typeof meta>

export const UnexpectedError: Story = {}
export const NotFound: Story = {
  args: { title: '404', details: 'This page does not exist. Check the URL.' },
}
export const DevelopmentStack: Story = {
  args: { stack: 'Error: Example development failure\n    at loader (example.ts:10:5)' },
}
export const Dark: Story = { globals: { backgrounds: { value: 'dark' } } }
