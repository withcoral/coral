import type { Meta, StoryObj } from '@storybook/react-vite'

import { TextButton } from '@/wax/components/button'

import { addToast, dismissAllToasts } from './toast'
import { ToastContainer } from './toast-container'

function ToastDemo() {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '12px', padding: '20px' }}>
      <TextButton
        onClick={() =>
          addToast('success', {
            description: 'Your changes have been saved successfully.',
            title: 'Saved!',
          })
        }
        variant="secondary"
      >
        Show success toast
      </TextButton>
      <TextButton
        onClick={() =>
          addToast('error', {
            description: 'Please try again or contact support.',
            title: 'Failed to save',
          })
        }
        variant="secondary"
      >
        Show error toast
      </TextButton>
      <TextButton
        onClick={() =>
          addToast('warning', {
            description: 'This action may impact other users.',
            title: 'Warning',
          })
        }
        variant="secondary"
      >
        Show warning toast
      </TextButton>
      <TextButton
        onClick={() =>
          addToast('neutral', {
            description: 'This may take a few moments.',
            title: 'Processing...',
          })
        }
        variant="secondary"
      >
        Show neutral toast
      </TextButton>
      <TextButton
        onClick={() =>
          addToast('success', {
            action: { label: 'View', onClick: () => alert('Action clicked') },
            description: 'Your document has been saved.',
            title: 'Document saved',
          })
        }
        variant="secondary"
      >
        Show toast with action
      </TextButton>
      <TextButton
        onClick={() =>
          addToast('warning', {
            description:
              'This is an extremely long description text that should test how the toast component handles overflow and wrapping of very long content. The description continues with more text to ensure we can see how multiple lines are handled and whether the toast maintains its layout and readability with extensive content that goes on and on and on.',
            title: 'This is an extremely long toast title that should test overflow behavior',
          })
        }
        variant="secondary"
      >
        Show toast with very long text
      </TextButton>
      <TextButton onClick={dismissAllToasts} variant="secondary">
        Dismiss all toasts
      </TextButton>
      <ToastContainer />
    </div>
  )
}

const meta = {
  component: ToastDemo,
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Wax/Toast',
} satisfies Meta<typeof ToastDemo>

export default meta
type Story = StoryObj<typeof meta>

export const AllVariants: Story = {}
