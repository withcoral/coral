import { useRef, useState } from 'react'

import type { PolicyState } from '@/components/policy-toggle'
import { Button, Typography } from '@/wax/components'
import { ActionDialog } from '@/views/management/forms'
import type { FormContext } from '@/views/management/types'
import { PublishDialog } from './publish-dialog'
import { ToolMenu } from './tool-drawer'
import { ToolSource } from './tool-source'
import type { PrivateToolDetailView } from './types'

/**
 * What the reader can do about publishing a private tool: an owner of an
 * active workspace publishes, with the tool's own policy as the first choice;
 * a member of an active workspace reads that only an owner can. In an archived
 * workspace nobody can, so the drawer says nothing of it.
 */
export type PublishAccess =
  | { workspacePath: string; policy: PolicyState; currentUser: string }
  | 'owner-only'
  | null

/**
 * The drawer header's actions on one private tool, your own or an agent's:
 * with `publish`, an owner publishes it as a shared tool; with `canDelete`,
 * its menu deletes it.
 */
export function PrivateToolActions({
  detail,
  callPath,
  form,
  canDelete,
  publish,
}: {
  detail: PrivateToolDetailView
  callPath: string
  form: FormContext
  /** False in an archived workspace, which accepts no changes. */
  canDelete: boolean
  publish: PublishAccess
}) {
  const [publishing, setPublishing] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const menu = useRef<HTMLButtonElement>(null)
  return (
    <>
      {publish === 'owner-only' && (
        <Typography.BodySmall as="span" variant="secondary">
          Only an owner can publish to shared tools.
        </Typography.BodySmall>
      )}
      {publish && publish !== 'owner-only' && (
        <>
          <Button.TextButton type="button" variant="primary" onClick={() => setPublishing(true)}>
            Publish
          </Button.TextButton>
          <PublishDialog
            open={publishing}
            onOpenChange={setPublishing}
            workspacePath={publish.workspacePath}
            form={form}
            origin={{ kind: 'private', path: detail.relativePath, agent: detail.agent }}
            path={detail.relativePath}
            policy={publish.policy}
            currentUser={publish.currentUser}
          />
        </>
      )}
      {canDelete && (
        <>
          <ToolMenu
            trigger={menu}
            items={[
              { label: 'Delete private tool', icon: 'Trash', onSelect: () => setDeleting(true) },
            ]}
          />
          <ActionDialog
            open={deleting}
            onOpenChange={setDeleting}
            finalFocus={menu}
            title="Delete private tool"
            submit="Delete private tool"
            form={form}
            intent="deletePrivateTool"
            destructive
            description={`After you delete it, programs that call ${callPath} fail. You cannot undo this.`}
          >
            <input type="hidden" name="path" value={detail.relativePath} />
            {detail.agent && <input type="hidden" name="agent" value={detail.agent} />}
          </ActionDialog>
        </>
      )}
    </>
  )
}

/** What Coral saved of one private tool: its declaration, its source and any diagnostics. */
export function PrivateToolDetail({ detail }: { detail: PrivateToolDetailView }) {
  return (
    <ToolSource
      source={detail}
      diagnosticsNote="These are from the last save. They do not stop the private tool from running."
    />
  )
}
