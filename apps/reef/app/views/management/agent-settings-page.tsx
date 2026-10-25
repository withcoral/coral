import { useState } from 'react'
import { Form } from 'react-router'
import { Button, Inputs, Typography } from '@/wax/components'
import { Card } from '@/wax/components/card/card'
import { PageLayout } from '@/wax/components/page-layout'
import { SettingsLayout, SettingsRemoval } from '@/wax/components/settings-layout'
import { Status } from '@/wax/components/status'
import type { ActionResult, AgentView, FormContext, RegistrationView, WorkspaceView } from './types'
import { ActionDialog, ActionNotice, Fields } from './forms'
import { AgentKeyDialog } from './agent-key-dialog'
import { ArchivedBanner, trail } from './layout'
import * as styles from './management.css'

export interface AgentSettingsPageProps {
  workspace: WorkspaceView
  agent: AgentView
  registration: RegistrationView
  form: FormContext
  result?: ActionResult
  pending?: boolean
}

/**
 * One agent's settings, laid out as provider settings are: one card per setting,
 * each with its action at the right, and deletion apart at the end.
 */
export function AgentSettingsPage({
  workspace: w,
  agent: a,
  registration: r,
  form,
  result,
  pending = false,
}: AgentSettingsPageProps) {
  const active = w.state === 'active'
  const enabled = a.state === 'enabled'
  const expired = a.accessBlockers.includes('Access expired')
  // The latest outcome wins, whether a form or a dialog sent it.
  const [notice, setNotice] = useState(result)
  const [seenResult, setSeenResult] = useState(result)
  if (result !== seenResult) {
    setSeenResult(result)
    setNotice(result)
  }
  // A replaced key shows once, in the same dialog as a new agent's key.
  const [savedKey, setSavedKey] = useState<string | null>(null)
  const key = notice?.credential && notice.credential !== savedKey ? notice.credential : null
  return (
    <PageLayout
      variant="wide"
      breadcrumbs={trail(
        [
          { label: 'Agents', to: `${w.path}/agents` },
          { label: a.displayName, to: a.path },
        ],
        'Settings',
      )}
    >
      <AgentKeyDialog
        credential={key}
        agentName={a.displayName}
        endpoint={w.connectEndpoint}
        onClose={() => setSavedKey(key)}
      />
      <SettingsLayout
        header={
          <>
            <div className={styles.heading}>
              <Typography.HeadingLarge as="h2">{a.displayName}</Typography.HeadingLarge>
              <Status blockers={a.accessBlockers} detail />
            </div>
            {!active && (
              <ArchivedBanner workspace={w}>
                You can read these settings. Restore the workspace to change them.
              </ArchivedBanner>
            )}
            <ActionNotice result={notice} />
          </>
        }
        removal={
          active && (
            <SettingsRemoval
              title="Delete agent"
              description="Delete this agent, its API key, connected accounts and saved functions. Coral keeps its run history."
              action={
                <ActionDialog
                  title="Delete agent"
                  submit="Delete agent"
                  form={form}
                  intent="delete"
                  etag={a.etag}
                  destructive
                  description="Deleting this agent removes its API key, connected accounts and saved functions. Coral keeps its run history. You cannot undo this."
                />
              }
            />
          )
        }
      >
        <Card
          title="Agent"
          description="Disabling the agent blocks new runs and calls that Coral has not sent."
          headerPill={
            enabled ? { label: 'Enabled', color: 'green' } : { label: 'Disabled', color: 'amber' }
          }
          action={
            active && (
              <Form method="post">
                <Fields form={form} intent="agentState" etag={a.etag} />
                <input type="hidden" name="state" value={enabled ? 'disabled' : 'enabled'} />
                <Button.TextButton
                  type="submit"
                  variant={enabled ? 'secondary' : 'primary'}
                  disabled={pending}
                >
                  {enabled ? 'Disable agent' : 'Enable agent'}
                </Button.TextButton>
              </Form>
            )
          }
        />
        <Card
          title="API access"
          description="Suspending API access blocks the key. Coral keeps the agent, its accounts and history."
          headerPill={
            r.enabled
              ? { label: 'Not suspended', color: 'green' }
              : { label: 'Suspended', color: 'amber' }
          }
          action={
            active && (
              <Form method="post">
                <Fields form={form} intent="registrationState" etag={r.etag} />
                <input name="enabled" type="hidden" value={r.enabled ? 'false' : 'true'} />
                <Button.TextButton
                  type="submit"
                  variant="secondary"
                  disabled={pending}
                  aria-label={r.enabled ? 'Suspend API access' : 'Resume API access'}
                >
                  {r.enabled ? 'Suspend' : 'Resume'}
                </Button.TextButton>
              </Form>
            )
          }
        />
        <Card
          title="Expiration"
          description="A past expiration date blocks access immediately."
          headerPill={
            r.expiration
              ? {
                  label: `${expired ? 'Expired' : 'Expires'} ${formatExpiration(r.expiration)}`,
                  color: expired ? 'amber' : 'gray',
                }
              : { label: 'Never', color: 'gray' }
          }
          action={
            active && (
              <ActionDialog
                title="Edit expiration"
                submit="Save expiration"
                form={form}
                intent="expiration"
                etag={r.etag}
                onSuccess={setNotice}
                description="Leave the field empty to allow access without an expiration date. A past date blocks access immediately."
              >
                <div className={styles.field}>
                  <Typography.BodySmallStrong as="label" htmlFor="access-expiration">
                    Access expires (UTC)
                  </Typography.BodySmallStrong>
                  <Inputs.TextInput
                    id="access-expiration"
                    name="expiration"
                    type="datetime-local"
                    step="1"
                    defaultValue={r.expiration.slice(0, 19)}
                  />
                </div>
              </ActionDialog>
            )
          }
        />
        <Card
          title="API key"
          description="Replacing the key invalidates the old key immediately."
          action={
            active && (
              <ActionDialog
                title="Replace API key"
                submit="Replace API key"
                form={form}
                intent="rotate"
                etag={r.etag}
                onSuccess={setNotice}
                destructive
                description="Replacing the API key invalidates the old key immediately. Save the new key when Coral shows it. Coral cannot show it again. The expiration date stays the same."
              />
            )
          }
        />
      </SettingsLayout>
    </PageLayout>
  )
}

function formatExpiration(iso: string) {
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return iso
  return `${date.toISOString().slice(0, 16).replace('T', ' ')} UTC`
}
