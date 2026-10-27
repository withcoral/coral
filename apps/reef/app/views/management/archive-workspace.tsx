import { useRef, useState } from 'react'
import { Button, Typography } from '@/wax/components'
import { ActionDialog } from './forms'
import type { FormContext, WorkspaceView } from './types'
import * as styles from './management.css'

/** An icon that asks you to confirm, then archives the Workspace. */
export function ArchiveWorkspaceButton({
  workspace: w,
  form,
}: {
  workspace: Pick<WorkspaceView, 'path' | 'etag' | 'displayName'>
  form: FormContext
}) {
  const [open, setOpen] = useState(false)
  const trigger = useRef<HTMLButtonElement>(null)
  return (
    <>
      <Button.IconButton
        ref={trigger}
        name="Archive"
        tooltipText={`Archive ${w.displayName}`}
        size="22"
        variant="bare"
        onClick={() => setOpen(true)}
      />
      <ActionDialog
        open={open}
        onOpenChange={setOpen}
        finalFocus={trigger}
        title={`Archive ${w.displayName}`}
        submit="Archive workspace"
        form={form}
        intent="archive"
        action={w.path}
        etag={w.etag}
        destructive
      >
        <div className={styles.consequences}>
          <Typography.Body as="p">This stops:</Typography.Body>
          <ul className={styles.bulletList}>
            <Typography.Body as="li">New runs</Typography.Body>
            <Typography.Body as="li">Unsent tool calls</Typography.Body>
            <Typography.Body as="li">
              Changes to providers, accounts, agents and members
            </Typography.Body>
          </ul>
          <Typography.Body as="p">
            Its history stays. You can restore it from the sidebar.
          </Typography.Body>
        </div>
      </ActionDialog>
    </>
  )
}
