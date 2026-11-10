import { useState } from 'react'

import { personName } from '@/views/management/person'
import { ActionDialog } from '@/views/management/forms'
import { SavedAt } from '@/views/management/saved-at'
import type { FormContext, PersonView, WorkspaceView } from '@/views/management/types'
import { Button, Table, Tabs, Typography } from '@/wax/components'
import { Pill } from '@/wax/components/pill'
import { SettingsRemoval } from '@/wax/components/settings-layout'

import type { PolicyState } from '@/components/policy-toggle'
import { PublishDialog } from './publish-dialog'
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

const sentence = (text: string) => text.charAt(0).toUpperCase() + text.slice(1)

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

/**
 * One shared tool: its source, its revisions and its history. With
 * `canChange`, an owner restores a revision by publishing it again, or
 * removes the shared tool.
 */
export function SharedToolDetail({
  detail,
  callPath,
  form,
  workspace,
  canChange,
  policy,
}: {
  detail: SharedToolDetailView
  callPath: string
  form: FormContext
  workspace: Pick<WorkspaceView, 'path' | 'currentUser'>
  /** The reader may publish and remove: an owner of an active workspace. */
  canChange: boolean
  /** The shared tool's policy now, which a restore dialog falls back on. */
  policy: PolicyState
}) {
  const [restoring, setRestoring] = useState<number | null>(null)
  const name = (person: PersonView | null) => sentence(publisherName(person, workspace.currentUser))
  return (
    <>
      <Tabs.Root defaultValue="source" className={styles.stack}>
        <Tabs.List inset="none">
          <Tabs.Tab value="source">Source</Tabs.Tab>
          <Tabs.Tab value="revisions">Revisions</Tabs.Tab>
          <Tabs.Tab value="history">History</Tabs.Tab>
          <Tabs.Indicator />
        </Tabs.List>
        <Tabs.Panel value="source" className={styles.stack}>
          <ToolSource
            source={detail}
            diagnosticsNote="These diagnostics are from the publish. They do not stop the shared tool from running."
            noDiagnostics="This shared tool has no diagnostics."
          />
        </Tabs.Panel>
        <Tabs.Panel value="revisions" className={styles.stack}>
          <Table.Container
            ariaLabel="Revisions"
            layout="fixed"
            columns={[
              { label: 'Revision', width: { fill: 3 } },
              { label: 'Published by', width: { fill: 2 } },
              { label: 'Published', width: { fill: 2 } },
              ...(canChange ? [{ label: 'Actions', width: { fill: 1 } }] : []),
            ]}
          >
            <Table.Head />
            <Table.Body>
              {detail.revisions.map((revision) => (
                <Table.Row key={revision.revision}>
                  <Table.Cell wrap>
                    <span className={styles.revisionCell}>
                      {`Revision ${revision.revision}`}
                      {revision.revision === detail.revision && (
                        <Pill size="small" variant="outline" color="graySubtle">
                          Current
                        </Pill>
                      )}
                      {revision.restoredFrom !== null && (
                        <Typography.BodySmall as="span" variant="secondary">
                          {`Restored revision ${revision.restoredFrom}`}
                        </Typography.BodySmall>
                      )}
                    </span>
                  </Table.Cell>
                  <Table.Cell>{name(revision.publisher)}</Table.Cell>
                  <Table.Cell>
                    <SavedAt at={revision.publishedAt} />
                  </Table.Cell>
                  {canChange && (
                    <Table.Cell>
                      {revision.revision !== detail.revision && (
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
                    </Table.Cell>
                  )}
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Container>
        </Tabs.Panel>
        <Tabs.Panel value="history" className={styles.stack}>
          <Table.Container
            ariaLabel="History"
            layout="fixed"
            columns={[
              { label: 'Event', width: { fill: 2 } },
              { label: 'By', width: { fill: 2 } },
              { label: 'Time', width: { fill: 2 } },
            ]}
          >
            <Table.Head />
            <Table.Body>
              {detail.events.map((event) => (
                <Table.Row key={event.id}>
                  <Table.Cell>
                    {event.action === 'published'
                      ? `Published revision ${event.revision}`
                      : `Removed at revision ${event.revision}`}
                  </Table.Cell>
                  <Table.Cell>{name(event.actor)}</Table.Cell>
                  <Table.Cell>
                    <SavedAt at={event.at} />
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Container>
          {detail.eventsTruncated && (
            <Typography.BodySmall as="p" variant="secondary">
              {`Showing the latest ${detail.events.length.toLocaleString('en-US')} events only.`}
            </Typography.BodySmall>
          )}
        </Tabs.Panel>
      </Tabs.Root>
      {canChange && (
        <SettingsRemoval
          title="Remove shared tool"
          description="Programs and tools that call this shared tool fail after you remove it."
          action={
            <ActionDialog
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
          }
        />
      )}
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
