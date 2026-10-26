import { useEffect, useId, useRef, useState, type ReactNode } from 'react'
import { Form, useFetcher, useRevalidator } from 'react-router'
import { signInFormTarget } from '@/lib/device-sign-in'
import { Banner, Button, Inputs, Typography } from '@/wax/components'
import * as Dialog from '@/wax/components/dialog'
import { addToast } from '@/wax/components/toast'
import { oauthFailureMessage } from '@/views/accounts/oauth-failure'
import type { AuthorizationView } from '@/views/accounts/types'
import { DeviceCodePanel } from '@/views/accounts/device-code'
import { DocumentSubmitButton } from '@/components/document-submit-button'
import { CredentialFields } from './credential-fields'
import { Fields } from './forms'
import type { FormContext } from './types'
import * as styles from './management.css'

interface Started {
  error: string | null
  /** Where the dialog checks the authorization; sign-in returns to this agent. */
  progress?: string
}

/** One check of the authorization. Every outcome is data, never a thrown error. */
type Progress =
  | { view: AuthorizationView }
  | { succeeded: true }
  | { signedOut: true }
  | { error: string }

/**
 * Connects an account without leaving the page: one of an agent's, or your own
 * from a provider's settings. Opening the
 * dialog starts an authorization; the dialog then shows its next step. Sign-in
 * and secrets leave as document submissions, so secrets never enter router
 * state, and they return here once the account is connected.
 */
export function ConnectAccountDialog({
  provider,
  providerName,
  namespace,
  form,
  connectable,
  reconnect,
  ready,
  fallback,
  trigger,
  action,
  description = 'Connect this account for the agent. Your personal accounts stay the same.',
}: {
  /** Names the provider to a route that serves several; your account page needs none. */
  provider?: string
  providerName: string
  /** Its namespace names the trigger, since display names need not be unique. */
  namespace: string
  form: FormContext
  /** False shows `fallback` in place of the Connect button. */
  connectable: boolean
  reconnect: boolean
  /** The row now shows a ready account, so the connection is done. */
  ready: boolean
  fallback: ReactNode
  /** A labelled button in place of the row's small Connect; the label names the account. */
  trigger?: {
    label: string
    variant: 'primary' | 'secondary'
    size: '22' | '32' | '36'
    /** Names the account when the label alone does not, as in a list. */
    ariaLabel?: string
  }
  /** The route that starts the connection; this page's own by default. */
  action?: string
  /** Whose account this is, under the title. */
  description?: string
}) {
  const [open, setOpen] = useState(false)
  // One attempt per connection: reopening continues it, and a finished
  // connection starts the next attempt empty.
  const id = useId()
  const [attempt, setAttempt] = useState(0)
  const start = useFetcher<Started>({ key: `connect-account-${id}-${attempt}` })
  const begin = () =>
    start.submit(
      {
        csrf: form.csrf,
        // A fresh attempt each time: the same request ID replays the last one.
        requestId: crypto.randomUUID(),
        intent: 'connectAccount',
        ...(provider && { provider }),
      },
      { method: 'post', action },
    )
  // Every finished connection reloads this page, so the row says when it is done.
  const [seenReady, setSeenReady] = useState(ready)
  const [connections, setConnections] = useState(0)
  if (ready !== seenReady) {
    setSeenReady(ready)
    if (ready) setAttempt(attempt + 1)
    if (ready && open) {
      setOpen(false)
      setConnections(connections + 1)
    }
  }
  useEffect(() => {
    if (connections) addToast('success', { title: 'Account connected.' })
  }, [connections])
  return (
    <Dialog.Root
      open={open}
      onOpenChange={(next) => {
        setOpen(next)
        // A refused start has nothing to continue, so it starts again. One still
        // in flight already reached Coral, so the dialog waits for its answer.
        if (next && start.state === 'idle' && !start.data?.progress) void begin()
      }}
    >
      {connectable ? (
        <Dialog.Trigger
          render={
            <Button.Container
              variant={trigger?.variant ?? 'secondary'}
              size={trigger?.size ?? '22'}
              // The name starts with the visible label, so voice control finds it.
              ariaLabel={
                trigger
                  ? trigger.ariaLabel
                  : `${reconnect ? 'Reconnect' : 'Connect'} account for ${namespace}`
              }
            />
          }
        >
          <Button.Text>{trigger?.label ?? (reconnect ? 'Reconnect' : 'Connect')}</Button.Text>
        </Dialog.Trigger>
      ) : (
        fallback
      )}
      {open && (
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup>
            <Dialog.Title>
              {reconnect ? 'Reconnect' : 'Connect'} {providerName}
            </Dialog.Title>
            <Dialog.Description>{description}</Dialog.Description>
            {start.state !== 'idle' || !start.data ? (
              <Typography.BodySmall as="p" aria-live="polite">
                Starting the connection…
              </Typography.BodySmall>
            ) : start.data.error || !start.data.progress ? (
              <>
                <Banner variant="error">
                  {start.data.error ?? 'Coral could not start the connection.'}
                </Banner>
                <Dialog.Actions>
                  <Button.TextButton
                    type="button"
                    variant="secondary"
                    onClick={() => setOpen(false)}
                  >
                    Close
                  </Button.TextButton>
                  <Button.TextButton type="button" onClick={() => void begin()}>
                    Try again
                  </Button.TextButton>
                </Dialog.Actions>
              </>
            ) : (
              <AuthorizationStep
                key={start.data.progress}
                progress={start.data.progress}
                close={() => setOpen(false)}
                retry={() => void begin()}
              />
            )}
          </Dialog.Popup>
        </Dialog.Portal>
      )}
    </Dialog.Root>
  )
}

/** One authorization's next step, kept current while Coral waits on the provider. */
function AuthorizationStep({
  progress,
  close,
  retry,
}: {
  progress: string
  close: () => void
  retry: () => void
}) {
  const check = useFetcher<Progress>()
  const revalidator = useRevalidator()
  const formId = useId()
  // The last view Coral returned. A failed check keeps it, so the dialog goes on waiting.
  const [v, setView] = useState<AuthorizationView>()
  const result = check.data
  if (result && 'view' in result && result.view !== v) setView(result.view)
  const failed = result && 'error' in result ? result.error : null
  const succeeded = (result !== undefined && 'succeeded' in result) || v?.state === 'succeeded'
  // Your Coral sign-in lapsed. Checking again cannot help, so the page signs you in again.
  const signedOut = result !== undefined && 'signedOut' in result
  const waiting =
    !succeeded &&
    !signedOut &&
    (v?.state === 'exchanging' || (v?.state === 'pending' && v.userCode !== undefined))
  useEffect(() => {
    if (check.state === 'idle' && !check.data) void check.load(progress)
  }, [check, progress])
  // A device sign-in finishes in another tab, so the dialog checks on it here.
  useEffect(() => {
    if (!waiting) return
    const timer = window.setInterval(() => {
      if (check.state === 'idle') void check.load(progress)
    }, 2000)
    return () => window.clearInterval(timer)
  }, [waiting, check, progress])
  // The page reloads its data in place, once. After a success the row turns ready
  // and the dialog closes; after a lapsed sign-in the page's own read starts sign-in.
  const reloaded = useRef(false)
  useEffect(() => {
    if (!(succeeded || signedOut) || reloaded.current) return
    reloaded.current = true
    void revalidator.revalidate()
  }, [succeeded, signedOut, revalidator])
  const closeButton = (
    <Button.TextButton type="button" variant="secondary" onClick={close}>
      Close
    </Button.TextButton>
  )
  if (signedOut)
    return (
      <Typography.BodySmall as="p" aria-live="polite">
        Your sign-in expired. Opening the sign-in page…
      </Typography.BodySmall>
    )
  if (succeeded)
    return (
      <>
        <Typography.BodySmall as="p" aria-live="polite">
          Account connected.
        </Typography.BodySmall>
        <Dialog.Actions>{closeButton}</Dialog.Actions>
      </>
    )
  if (!v)
    return failed ? (
      <>
        <Banner variant="error">{failed}</Banner>
        <Dialog.Actions>
          {closeButton}
          <Button.TextButton type="button" onClick={() => void check.load(progress)}>
            Try again
          </Button.TextButton>
        </Dialog.Actions>
      </>
    ) : (
      <Typography.BodySmall as="p" aria-live="polite">
        Starting the connection…
      </Typography.BodySmall>
    )
  // Coral checks again on the next tick while it waits.
  const recheck = failed && waiting && (
    <Typography.BodySmall as="p" variant="tertiary" aria-live="polite">
      {failed} Trying again in a moment…
    </Typography.BodySmall>
  )
  if (v.state === 'failed' || v.state === 'expired' || v.state === 'canceled')
    return (
      <>
        <Banner variant={v.state === 'failed' ? 'error' : 'warning'}>
          {v.state === 'failed'
            ? v.applicationFailure
              ? 'Ask the provider owner to repair the OAuth application.'
              : oauthFailureMessage(v.failure)
            : v.state === 'expired'
              ? 'This connection expired.'
              : 'This connection was canceled.'}
        </Banner>
        <Dialog.Actions>
          {closeButton}
          <Button.TextButton type="button" onClick={retry}>
            Try again
          </Button.TextButton>
        </Dialog.Actions>
      </>
    )
  if (v.state !== 'pending')
    return (
      <>
        <Typography.BodySmall as="p" aria-live="polite">
          Completing sign-in…
        </Typography.BodySmall>
        {recheck}
        <Dialog.Actions>{closeButton}</Dialog.Actions>
      </>
    )
  if (!v.oauth)
    return (
      // A document form: the secret goes straight to Coral, never into router state.
      <form id={formId} method="post" action={v.path} className={styles.section}>
        <Fields form={v.form} intent="secret" />
        <CredentialFields fields={v.credentialFields}>
          {(input) => <Inputs.CredentialInput {...input} />}
        </CredentialFields>
        <Typography.BodySmall as="p" variant="tertiary">
          Enter the credentials from {v.providerName}.
        </Typography.BodySmall>
        <Dialog.Actions>
          {closeButton}
          <DocumentSubmitButton>Connect account</DocumentSubmitButton>
        </Dialog.Actions>
      </form>
    )
  const device = v.userCode !== undefined
  return (
    <>
      <Form id={formId} method="post" action={v.path} reloadDocument {...signInFormTarget(device)}>
        <Fields form={v.form} intent="navigate" />
      </Form>
      {device ? (
        <>
          <DeviceCodePanel code={v.userCode ?? ''} />
          {recheck}
        </>
      ) : (
        <Typography.BodySmall as="p">
          Sign in to {v.providerName} to approve access.
        </Typography.BodySmall>
      )}
      <Dialog.Actions>
        {closeButton}
        {device ? (
          <Button.Container type="submit" form={formId} variant="primary" disabled={!v.userCode}>
            <Button.Text>Open sign-in page</Button.Text>
            <Button.Icon name="ExternalLink" />
          </Button.Container>
        ) : (
          <DocumentSubmitButton form={formId}>Continue to sign in</DocumentSubmitButton>
        )}
      </Dialog.Actions>
    </>
  )
}
