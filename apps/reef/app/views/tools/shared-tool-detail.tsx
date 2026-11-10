import { useRef, useState } from 'react'

import type { PolicyState } from '@/components/policy-toggle'
import { personName } from '@/views/management/person'
import { ActionDialog } from '@/views/management/forms'
import { SavedAt } from '@/views/management/saved-at'
import type { FormContext, PersonView, WorkspaceView } from '@/views/management/types'
import { Button, Tabs, Typography } from '@/wax/components'
import { Pill } from '@/wax/components/pill'

import { PublishDialog } from './publish-dialog'
import { ToolMenu } from './tool-drawer'
import { ToolSource } from './tool-source'
import type { SharedToolDetailView } from './types'
import * as styles from './tools.css'

/**
 * Who published or removed, as the page names them. A shared tool outlives
 * its author, and the page names only a current owner of the workspace.
 */
export function publisherName(person: PersonView | null, currentUser: string) {
  return person ? personName(person, currentUser) : 'a former owner'
}

/** The line under a shared tool's call path: its revision, who published it, and when. */
export function SharedToolProvenance({
  detail,
  currentUser,
}: {
  detail: SharedToolDetailView
  currentUser: string
}) {
  return (
    <>
      {`Revision ${detail.revision} · published by ${publisherName(detail.publisher, currentUser)} · `}
      <SavedAt at={detail.publishedAt} />
    </>
  )
}

/**
 * The revision the owner saw when the dialog opened, which a removal expects.
 * A refused removal reloads the page, maybe with another owner's revision. The
 * dialog keeps the first one, as it keeps its etag, so Coral refuses again
 * rather than remove a revision the owner never saw. The dialog mounts its
 * fields only while open, so each opening reads the revision again.
 */
function SeenRevision({ revision }: { revision: number }) {
  const [seen] = useState(revision)
  return <input type="hidden" name="expectedRevision" value={seen} />
}

/** The drawer header's actions on a shared tool: with `canChange`, its menu removes it. */
export function SharedToolActions({
  detail,
  callPath,
  form,
  canChange,
}: {
  detail: SharedToolDetailView
  callPath: string
  form: FormContext
  /** The reader may remove: an owner of an active workspace. */
  canChange: boolean
}) {
  const [removing, setRemoving] = useState(false)
  const menu = useRef<HTMLButtonElement>(null)
  if (!canChange) return null
  return (
    <>
      <ToolMenu
        trigger={menu}
        items={[{ label: 'Remove shared tool', icon: 'Trash', onSelect: () => setRemoving(true) }]}
      />
      <ActionDialog
        open={removing}
        onOpenChange={setRemoving}
        finalFocus={menu}
        title="Remove shared tool"
        submit="Remove shared tool"
        form={form}
        intent="removeSharedTool"
        destructive
        description={`After you remove it, programs and tools that call ${callPath} fail. You cannot undo this.`}
      >
        <input type="hidden" name="path" value={detail.relativePath} />
        <SeenRevision revision={detail.revision} />
      </ActionDialog>
    </>
  )
}

/**
 * One shared tool's source and its revisions, as tabs. With `canChange`, an
 * owner restores an earlier revision by publishing it again.
 */
export function SharedToolDetail({
  detail,
  form,
  workspace,
  canChange,
  policy,
}: {
  detail: SharedToolDetailView
  form: FormContext
  workspace: Pick<WorkspaceView, 'path' | 'currentUser'>
  /** The reader may publish: an owner of an active workspace. */
  canChange: boolean
  /** The shared tool's policy now, which a restore dialog falls back on. */
  policy: PolicyState
}) {
  const [restoring, setRestoring] = useState<number | null>(null)
  return (
    <>
      <Tabs.Root defaultValue="source" className={styles.stack}>
        <Tabs.List inset="none">
          <Tabs.Tab value="source">Source</Tabs.Tab>
          <Tabs.Tab value="revisions">
            Revisions{' '}
            <Typography.BodySmall as="span" variant="secondary">
              {detail.revisions.length.toLocaleString('en-US')}
            </Typography.BodySmall>
          </Tabs.Tab>
          <Tabs.Indicator />
        </Tabs.List>
        <Tabs.Panel value="source" className={styles.stack}>
          <ToolSource
            source={detail}
            diagnosticsNote="These are from the publish. They do not stop the shared tool from running."
          />
        </Tabs.Panel>
        <Tabs.Panel value="revisions">
          {/* Each publish makes one revision, so the list is the shared tool's history too. */}
          <ul aria-label="Revisions" className={styles.lines}>
            {detail.revisions.map((revision) => (
              <li key={revision.revision} className={styles.line}>
                <span className={styles.lineMain}>
                  <span className={styles.revisionCell}>
                    <Typography.BodyStrong as="span">{`Revision ${revision.revision}`}</Typography.BodyStrong>
                    {revision.revision === detail.revision && (
                      <Pill size="small" variant="outline" color="graySubtle">
                        Current
                      </Pill>
                    )}
                    {revision.restoredFrom !== null && (
                      <Typography.BodySmall as="span" variant="secondary">
                        {`Restores revision ${revision.restoredFrom}`}
                      </Typography.BodySmall>
                    )}
                  </span>
                  <Typography.BodySmall as="span" variant="secondary">
                    {`${publisherName(revision.publisher, workspace.currentUser)} · `}
                    <SavedAt at={revision.publishedAt} />
                  </Typography.BodySmall>
                </span>
                {canChange && revision.revision !== detail.revision && (
                  <Button.TextButton
                    type="button"
                    variant="secondary"
                    size="22"
                    aria-label={`Restore revision ${revision.revision}`}
                    onClick={() => setRestoring(revision.revision)}
                  >
                    Restore
                  </Button.TextButton>
                )}
              </li>
            ))}
          </ul>
        </Tabs.Panel>
      </Tabs.Root>
      {restoring !== null && (
        <PublishDialog
          open
          onOpenChange={(open) => {
            if (!open) setRestoring(null)
          }}
          workspacePath={workspace.path}
          form={form}
          origin={{ kind: 'restore', revision: restoring }}
          path={detail.relativePath}
          policy={policy}
          currentUser={workspace.currentUser}
        />
      )}
    </>
  )
}
