import { useContext, useEffect } from 'react'
import { useFetcher } from 'react-router'
import { Button, Typography } from '@/wax/components'
import { addToast } from '@/wax/components/toast'
import { Fields, ShellFormContext } from './forms'
import type { ActionResult, FormContext, WorkspaceView } from './types'

/**
 * Restores an archived Workspace in one click. Restoring is reversible, so it
 * asks for no confirmation. The page behind it reloads into the active Workspace.
 * A failure shows beside the button, or as a toast from the compact icon.
 */
export function RestoreWorkspaceButton({
  workspace: w,
  compact = false,
  form: given,
}: {
  workspace: Pick<WorkspaceView, 'path' | 'etag' | 'displayName'>
  /** An icon button, for the sidebar. */
  compact?: boolean
  /** Defaults to the app shell's form context. */
  form?: FormContext
}) {
  const shell = useContext(ShellFormContext)
  const form = given ?? shell
  const fetcher = useFetcher<ActionResult>()
  const pending = fetcher.state !== 'idle'
  // Each finished attempt is a new object, so a repeated failure toasts again.
  const finished = pending ? undefined : fetcher.data
  useEffect(() => {
    if (compact && finished?.error) addToast('error', { title: finished.error })
  }, [compact, finished])
  if (!form) return null
  const error = fetcher.data?.error
  return (
    <fetcher.Form method="post" action={w.path}>
      <Fields form={form} intent="restore" etag={w.etag} />
      {compact ? (
        <Button.IconButton
          type="submit"
          name="ArchiveRestore"
          tooltipText={`Restore ${w.displayName}`}
          size="22"
          variant="bare"
          disabled={pending}
        />
      ) : (
        <Button.TextButton
          type="submit"
          size="22"
          variant="secondary"
          disabled={pending}
          busy={pending}
        >
          Restore workspace
        </Button.TextButton>
      )}
      {!compact && error && (
        <Typography.BodySmall as="p" role="alert">
          {error}
        </Typography.BodySmall>
      )}
    </fetcher.Form>
  )
}
