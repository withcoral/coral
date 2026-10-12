import { Form } from 'react-router'
import { Button, Typography } from '@/wax/components'
import * as Dialog from '@/wax/components/dialog'
import * as SettingRow from '@/wax/components/setting-row'
import { StatePill } from './access'
import { ActionNotice, ConfirmAction, Fields, NameInput } from './forms'
import { Section } from './layout'
import type { ActionResult, FormContext, WorkspaceView } from './types'
import * as styles from './management.css'

export interface SettingsDialogProps {
  workspace: WorkspaceView
  form: FormContext
  pending?: boolean
  result?: ActionResult
  onClose?: () => void
}

/** Workspace settings open over the page that the route renders behind them. */
export function SettingsDialog({
  workspace: w,
  form,
  pending = false,
  result,
  onClose,
}: SettingsDialogProps) {
  const active = w.state === 'active'
  return (
    <Dialog.Root
      open
      onOpenChange={(open) => {
        if (!open && !pending) onClose?.()
      }}
    >
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="l">
          <div className={styles.dialogHeading}>
            <Dialog.Title>Workspace settings</Dialog.Title>
            <StatePill workspace={w} />
          </div>
          <ActionNotice result={result} />
          <Section
            title="Name"
            description="Change the workspace name. Its ID and URL stay the same."
          >
            {active ? (
              <Form method="post" className={styles.formRow}>
                <Fields form={form} intent="rename" etag={w.etag} />
                <NameInput
                  key={w.displayName}
                  label="Workspace name"
                  defaultValue={w.displayName}
                />
                <Button.TextButton type="submit" disabled={pending}>
                  Save name
                </Button.TextButton>
              </Form>
            ) : (
              <Typography.Body as="p">{w.displayName}</Typography.Body>
            )}
          </Section>
          <Section title="Archive and restore">
            <SettingRow.List>
              <SettingRow.Row
                label="Workspace"
                value={<StatePill workspace={w} />}
                description={
                  active
                    ? 'Archiving blocks new runs, unsent operation calls and settings changes. Coral keeps the history. You can restore the workspace.'
                    : 'Restore the workspace to allow new runs and settings changes. Other access restrictions still apply.'
                }
                action={
                  active ? (
                    <ConfirmAction
                      form={form}
                      intent="archive"
                      etag={w.etag}
                      pending={pending}
                      label="Archive workspace"
                      confirm="Archive workspace"
                      explanation="Archiving blocks new runs, unsent operation calls and settings changes. Coral keeps the history. You can restore the workspace."
                    />
                  ) : (
                    <Form method="post">
                      <Fields form={form} intent="restore" etag={w.etag} />
                      <Button.TextButton type="submit" disabled={pending}>
                        Restore workspace
                      </Button.TextButton>
                    </Form>
                  )
                }
              />
            </SettingRow.List>
          </Section>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
