import { useState } from 'react'

import { Button, Typography } from '@/wax/components'
import * as Dialog from '@/wax/components/dialog'
import { Skeleton } from '@/wax/components/skeleton'
import * as styles from './device-code.css'

/**
 * The code to enter on the sign-in page. An empty code means the service has
 * not issued one yet: the panel keeps its size with a placeholder, so the code
 * arriving does not move the dialog.
 */
export function DeviceCodePanel({ code }: { code: string }) {
  return (
    <div className={styles.panel} aria-busy={!code}>
      <Typography.BodySmall variant="tertiary" aria-live="polite">
        {code ? 'Enter this code' : 'Requesting your code…'}
      </Typography.BodySmall>
      <div className={styles.row}>
        {code ? (
          <Typography.CodeLarge
            as="code"
            className={styles.code}
            size={24}
            variant="primary"
            weight={700}
          >
            {code}
          </Typography.CodeLarge>
        ) : (
          <Skeleton className={styles.code} height={32} width={148} borderRadius={6} />
        )}
        <Button.CopyButton ariaLabel="Copy sign-in code" textToCopy={code} disabled={!code} />
      </div>
    </div>
  )
}

/**
 * A device sign-in in flight. The dialog opens with the page and shows the code
 * to enter on the sign-in page. Its primary action submits `formId`, which
 * opens that page in a new tab, so the dialog stays here while Coral waits. An
 * empty code means the service has not issued one yet. The trigger reopens the
 * dialog after the person closes it.
 */
export function DeviceCodeDialog({
  code,
  formId,
  pending,
  triggerLabel,
}: {
  code: string
  formId: string
  pending: boolean
  triggerLabel: string
}) {
  const [open, setOpen] = useState(true)
  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Trigger render={<Button.Container variant="primary" disabled={pending} />}>
        <Button.Text>{triggerLabel}</Button.Text>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup>
          <Dialog.Title>Enter your sign-in code</Dialog.Title>
          <Dialog.Description>Open the sign-in page. Enter this code there.</Dialog.Description>
          <DeviceCodePanel code={code} />
          <Dialog.Actions>
            <Button.TextButton type="button" variant="secondary" onClick={() => setOpen(false)}>
              Close
            </Button.TextButton>
            <Button.Container
              type="submit"
              form={formId}
              variant="primary"
              disabled={pending || !code}
            >
              <Button.Text>Open sign-in page</Button.Text>
              {code ? (
                <Button.Icon name="ExternalLink" />
              ) : (
                <Button.SpinningButtonIcon name="Loader" />
              )}
            </Button.Container>
          </Dialog.Actions>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
