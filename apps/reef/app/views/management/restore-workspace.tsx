import { useContext } from 'react'
import { useFetcher } from 'react-router'
import { Button, Typography } from '@/wax/components'
import { Fields, ShellFormContext } from './forms'
import type { ActionResult, FormContext, WorkspaceView } from './types'

/**
 * Restores an archived Workspace in one click. Restoring is reversible, so it
 * asks for no confirmation. The page behind it reloads into the active Workspace.
 * A failure shows beside the button.
 */
export function RestoreWorkspaceButton({
  workspace: w,
  form: given,
}: {
  workspace: Pick<WorkspaceView, 'path' | 'etag'>
  /** Defaults to the app shell's form context. */
  form?: FormContext
}) {
  const shell = useContext(ShellFormContext)
  const form = given ?? shell
  const fetcher = useFetcher<ActionResult>()
  const pending = fetcher.state !== 'idle'
  if (!form) return null
  const error = fetcher.data?.error
  return (
    <fetcher.Form method="post" action={w.path}>
      <Fields form={form} intent="restore" etag={w.etag} />
      <Button.TextButton
        type="submit"
        size="22"
        variant="secondary"
        disabled={pending}
        busy={pending}
      >
        Restore workspace
      </Button.TextButton>
      {error && (
        <Typography.BodySmall as="p" role="alert">
          {error}
        </Typography.BodySmall>
      )}
    </fetcher.Form>
  )
}
