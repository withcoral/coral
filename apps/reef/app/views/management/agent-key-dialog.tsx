import { useId, useRef } from 'react'
import { Button, Typography } from '@/wax/components'
import * as Dialog from '@/wax/components/dialog'
import { SecretsInput } from '@/wax/components/inputs'
import { agentSteps, ClientSetup } from './quick-start'
import * as styles from './management.css'

/**
 * A new agent's API key, which Coral shows once. Leaving takes a button press,
 * not a click outside or Escape, which would lose a key nobody can read again.
 * Close stays available for when the clipboard fails and the key is copied by hand.
 * With the workspace's MCP endpoint, it also shows each client's setup with the
 * key already in it: the one moment Coral can fill it in.
 */
export function AgentKeyDialog({
  credential,
  agentName,
  endpoint,
  onClose,
}: {
  credential: string | null
  agentName?: string
  endpoint?: string
  onClose: () => void
}) {
  const actions = useRef<HTMLDivElement>(null)
  const keyField = useId()
  const setup = useId()
  return (
    <Dialog.Root open={Boolean(credential)} disablePointerDismissal>
      {credential && (
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup
            size={endpoint ? 'l' : 'm'}
            className={styles.stack}
            // Focus Copy, not the reveal toggle: the key stays masked until asked.
            initialFocus={() => actions.current?.querySelector('button:last-of-type') ?? true}
          >
            <Dialog.Title>Save the API key{agentName ? ` for ${agentName}` : ''}</Dialog.Title>
            <Dialog.Description>
              Coral shows this key only once. Save it before you close this dialog.
            </Dialog.Description>
            <div className={styles.intro}>
              <Typography.BodySmallStrong as="p" id={keyField}>
                API key
              </Typography.BodySmallStrong>
              <div role="group" aria-labelledby={keyField}>
                <SecretsInput content={credential} />
              </div>
            </div>
            {endpoint && (
              <section className={styles.stack} aria-labelledby={setup}>
                <Typography.BodySmallStrong as="h3" id={setup}>
                  Connect the agent's MCP client
                </Typography.BodySmallStrong>
                <ClientSetup steps={(client) => agentSteps(client, endpoint, credential)} />
              </section>
            )}
            <Dialog.Actions ref={actions}>
              <Button.TextButton type="button" variant="secondary" onClick={onClose}>
                Close
              </Button.TextButton>
              <Button.CopyButton
                variant="primary"
                ariaLabel="Copy API key and close"
                textToCopy={credential}
                onCopy={onClose}
              >
                <Button.Text>Copy API key and close</Button.Text>
              </Button.CopyButton>
            </Dialog.Actions>
          </Dialog.Popup>
        </Dialog.Portal>
      )}
    </Dialog.Root>
  )
}
