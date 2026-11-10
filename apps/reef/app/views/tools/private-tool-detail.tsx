import { useState } from 'react'

import type { PolicyState } from '@/components/policy-toggle'
import { Button, Typography } from '@/wax/components'
import { SettingsRemoval } from '@/wax/components/settings-layout'
import { ActionDialog } from '@/views/management/forms'
import type { FormContext } from '@/views/management/types'
import { PublishDialog } from './publish-dialog'
import { ToolSource } from './tool-source'
import type { PrivateToolDetailView } from './types'
import * as styles from './tools.css'

/**
 * What Coral saved of one private tool, your own or an agent's: its
 * declaration, source and diagnostics. With `publish`, an owner can publish
 * it as a shared tool. With `canDelete`, you can delete it.
 */
export function PrivateToolDetail({
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
  /**
   * What the reader can do about publishing: an owner of an active workspace
   * publishes, with the tool's own policy as the first choice; a member of an
   * active workspace reads that only an owner can. In an archived workspace
   * nobody can, so the drawer says nothing of it.
   */
  publish: { workspacePath: string; policy: PolicyState; currentUser: string } | 'owner-only' | null
}) {
  const [publishing, setPublishing] = useState(false)
  return (
    <>
      {publish === 'owner-only' && (
        <Typography.BodySmall as="p" variant="secondary">
          Only an owner can publish to shared tools.
        </Typography.BodySmall>
      )}
      {publish && publish !== 'owner-only' && (
        <div className={styles.drawerActions}>
          <Button.TextButton type="button" variant="secondary" onClick={() => setPublishing(true)}>
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
        </div>
      )}
      <ToolSource
        source={detail}
        diagnosticsNote="These diagnostics are from the last save. They do not stop the private tool from running."
        noDiagnostics="This private tool has no diagnostics."
      />
      {canDelete && (
        <SettingsRemoval
          title="Delete private tool"
          description="Programs that call this private tool fail after you delete it."
          action={
            <ActionDialog
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
          }
        />
      )}
    </>
  )
}
