import { Button } from '@/wax/components'
import * as Dialog from '@/wax/components/dialog'
import { PromptPreview } from './prompt-preview'
import * as styles from './management.css'

/** Opens once on a new workspace's overview with a prompt for the viewer's MCP client. */
export function OnboardingDialog({
  prompt,
  open,
  onClose,
}: {
  /** The workspace's `onboardingPrompt`. */
  prompt: string
  /** Whether the viewer owns the workspace, so the prompt adds a provider. */
  owner: boolean
  open: boolean
  onClose: () => void
}) {
  return (
    <Dialog.Root open={open} onOpenChange={(next) => !next && onClose()}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup className={styles.stack}>
          <Dialog.Title>Ask your agent to set up this workspace</Dialog.Title>
          <Dialog.Description>
            Copy this prompt into your agent to connect Coral and add a provider.
          </Dialog.Description>
          <PromptPreview prompt={prompt} />
          <Dialog.Actions>
            <Button.TextButton type="button" variant="secondary" onClick={onClose}>
              Dismiss
            </Button.TextButton>
            <Button.CopyButton variant="primary" ariaLabel="Copy prompt" textToCopy={prompt}>
              <Button.Text>Copy prompt</Button.Text>
            </Button.CopyButton>
          </Dialog.Actions>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
