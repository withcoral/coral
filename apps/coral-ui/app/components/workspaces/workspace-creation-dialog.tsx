import { Form, useFetcher } from 'react-router'

import { Button, Dialog, Inputs, Typography } from '@/wax/components'
import type { CreateWorkspaceActionData } from '@/lib/workspace-name'
import { routePath } from '@/routing/routemap'

import * as styles from './workspace-creation-dialog.css'

export interface WorkspaceCreationDialogProps {
  fetcherKey: string
  onOpenChange: (open: boolean) => void
  open: boolean
}

export function WorkspaceCreationDialog({
  fetcherKey,
  onOpenChange,
  open,
}: WorkspaceCreationDialogProps) {
  const createWorkspaceFetcher = useFetcher<CreateWorkspaceActionData>({ key: fetcherKey })
  const isCreatingWorkspace = createWorkspaceFetcher.state !== 'idle'

  return (
    <Dialog.Root onOpenChange={onOpenChange} open={open}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="m">
          <Dialog.Title>Create workspace</Dialog.Title>
          <Dialog.Description>
            Choose a name for the local workspace. Use letters, numbers, and hyphens.
          </Dialog.Description>
          <Dialog.Close />
          <Form
            action={routePath('workspaces')}
            className={styles.createWorkspaceForm}
            fetcherKey={fetcherKey}
            method="post"
            navigate={false}
          >
            <input name="intent" type="hidden" value="create" />
            <label className={styles.createWorkspaceField}>
              <Typography.BodySmallStrong>Workspace name</Typography.BodySmallStrong>
              <Inputs.TextInput autoFocus name="name" placeholder="engineering" />
            </label>
            {createWorkspaceFetcher.data?.error && (
              <Typography.BodySmall as="p" role="alert" variant="error">
                {createWorkspaceFetcher.data.error}
              </Typography.BodySmall>
            )}
            <Dialog.Actions>
              <Button.TextButton
                disabled={isCreatingWorkspace}
                onClick={() => onOpenChange(false)}
                type="button"
                variant="secondary"
              >
                Cancel
              </Button.TextButton>
              <Button.TextButton disabled={isCreatingWorkspace} type="submit">
                {isCreatingWorkspace ? 'Creating…' : 'Create workspace'}
              </Button.TextButton>
            </Dialog.Actions>
          </Form>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
