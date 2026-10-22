import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, waitFor } from 'storybook/test'

import { DocumentSubmitButton } from './document-submit-button'

const meta = {
  component: DocumentSubmitButton,
  parameters: { layout: 'centered' },
  title: 'Components/DocumentSubmitButton',
} satisfies Meta<typeof DocumentSubmitButton>

export default meta
type Story = StoryObj<typeof meta>

export const Submitted: Story = {
  args: { children: 'Save' },
  render: (args) => (
    <form aria-label="Example">
      <DocumentSubmitButton {...args} />
    </form>
  ),
  play: async ({ canvas }) => {
    const form = canvas.getByRole('form', { name: 'Example' })
    const button = canvas.getByRole('button', { name: 'Save' })
    form.dispatchEvent(
      new SubmitEvent('submit', { bubbles: true, cancelable: true, submitter: button }),
    )
    await waitFor(() => expect(button).toHaveAttribute('aria-busy', 'true'))
    await expect(button).toHaveAttribute('aria-disabled', 'true')
    await expect(button).not.toBeDisabled()
  },
}
