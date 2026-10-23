import { useId, useRef, useState, type ReactNode } from 'react'
import { Form } from 'react-router'
import { signInFormTarget } from '@/lib/device-sign-in'

import { Banner, Button, Inputs, Typography } from '@/wax/components'
import { EmptyPage } from '@/components/empty-page'
import { DocumentSubmitButton } from '@/components/document-submit-button'
import * as Dialog from '@/wax/components/dialog'
import { Fields } from '@/views/management/forms'
import { DeviceCodeDialog } from '@/views/accounts/device-code'
import { oauthFailureMessage } from '@/views/accounts/oauth-failure'
import { credentialHosts, OAuthApplicationBlock } from './oauth-application'

import { CredentialFields } from '@/views/management/credential-fields'
import { accountLabels } from './account-labels'
import type { AccountChoice, FormContext, SetupView, StepperResult } from './types'
import { StepFrame } from './step-frame'
import * as styles from './providers.css'

/** The hidden fields every action on one setup revision carries. */
export function SetupFields({ setup, extra }: { setup: SetupView; extra?: ReactNode }) {
  return (
    <>
      <input type="hidden" name="setup" value={setup.id} />
      <input
        type="hidden"
        name="configurationRevision"
        value={setup.configuration?.revision ?? ''}
      />
      {extra}
    </>
  )
}

/**
 * Where typed-in credentials go, shown where you enter them. An agent's
 * addProvider can choose the servers, so you check them here.
 */
export function CredentialDestination({ setup }: { setup: SetupView }) {
  const hosts = credentialHosts(setup)
  if (!hosts.length) return null
  return (
    <Typography.BodySmall as="p" variant="tertiary">
      Coral sends what you enter here to {hosts.join(', ')}.
    </Typography.BodySmall>
  )
}

/** What one sign-in attempt reports, before the person acts on it again. */
export function SignInStatus({ setup }: { setup: SetupView }) {
  const a = setup.authorization
  if (!a) return null
  if (a.state === 'failed' && a.failure === 'insufficient_scope')
    return (
      <Banner variant="error">
        {setup.serverName || 'The service'} asks for more permissions than you approved. Sign in
        again. Approve the permissions it asks for.
      </Banner>
    )
  if (a.state === 'failed')
    return <Banner variant="error">{oauthFailureMessage(a.failure, true)}</Banner>
  if (a.state === 'expired' || a.state === 'canceled')
    return (
      <Banner variant="warning">
        {a.state === 'expired' ? 'Your sign-in expired.' : 'Sign-in canceled.'} Sign in again.
      </Banner>
    )
  return null
}

/** The primary action that starts or restarts a sign-in with the service. */
export function signInLabel(setup: SetupView) {
  const a = setup.authorization
  if (a && a.state !== 'pending') return 'Sign in again'
  return setup.serverName ? `Sign in with ${setup.serverName}` : 'Sign in'
}

/**
 * One sign-in in flight: continue a pending consent, wait for the exchange,
 * and show when it expires. Shared by discovery and account sign-ins.
 */
export function SignInProgress({
  setup,
  form,
  pending,
  extra,
  aside,
}: {
  setup: SetupView
  form: FormContext
  pending: boolean
  /** Hidden fields the resumed sign-in must carry, such as the account choice. */
  extra?: ReactNode
  /** The way back, shown beside Continue sign-in. */
  aside?: ReactNode
}) {
  const a = setup.authorization
  if (!a) return null
  return (
    <>
      {a.state === 'pending' && (
        <div className={styles.stepActions}>
          {aside}
          <ContinueSignIn setup={setup} form={form} pending={pending} extra={extra} />
        </div>
      )}
      {a.state === 'exchanging' && <Typography.Body as="p">Completing sign-in…</Typography.Body>}
    </>
  )
}

/** Resumes a pending consent, carrying the hidden fields the sign-in needs. */
export function ContinueSignIn({
  setup,
  form,
  pending,
  extra,
}: {
  setup: SetupView
  form: FormContext
  pending: boolean
  extra?: ReactNode
}) {
  const code = setup.authorization?.userCode
  const formId = useId()
  return (
    <Form id={formId} method="post" reloadDocument {...signInFormTarget(code !== undefined)}>
      <Fields form={form} intent="oauth-resume" />
      <SetupFields setup={setup} extra={extra} />
      {code !== undefined ? (
        <DeviceCodeDialog
          code={code}
          formId={formId}
          pending={pending}
          triggerLabel="Continue sign-in"
        />
      ) : (
        <DocumentSubmitButton disabled={pending}>Continue sign-in</DocumentSubmitButton>
      )}
    </Form>
  )
}

/**
 * What a discovery sign-in is for, said where the credential is actually asked
 * for. The account question is a later and separate decision; this sign-in only
 * reads the list.
 */
const DISCOVERY_ONLY = 'This sign-in only reads the operations. It is not shared with anyone.'

/**
 * The operations panel with nothing to list yet: why it is empty, and the one
 * action that fills it. A protected MCP server answers nobody until someone
 * signs in, so that sign-in belongs in the panel rather than on a step of its
 * own. Update keeps DiscoveryStep, which also offers the connected account.
 */
export function DiscoveryPrompt({
  setup,
  form,
  pending,
  result,
  callbackUrl,
}: {
  setup: SetupView
  form: FormContext
  pending: boolean
  result?: StepperResult
  callbackUrl: string
}) {
  const formId = useId()
  const secretId = useId()
  const oauthForm = useRef<HTMLFormElement>(null)
  const [asking, setAsking] = useState(false)
  if (setup.state === 'awaiting_discovery')
    return (
      <EmptyPage
        iconName="TriangleAlert"
        title="Coral cannot read the operations"
        description={`${setup.serverName || 'The server'} did not list its operations. ${setup.discoveryFailure || ''} You do not need to sign in again.`.replace(
          '  ',
          ' ',
        )}
        action={
          <div className={styles.emptyActions}>
            {result?.error && <Banner variant="error">{result.error}</Banner>}
            <Form method="post">
              <Fields form={form} intent="oauth-retry" />
              <SetupFields setup={setup} />
              <Button.TextButton type="submit" size="36" disabled={pending} busy={pending}>
                Try again
              </Button.TextButton>
            </Form>
          </div>
        }
      />
    )
  const a = setup.authorization
  const bearer = setup.credentialMethod === 'bearer'
  const canStart = !a || a.state === 'failed' || a.state === 'expired' || a.state === 'canceled'
  const inFlight = !bearer && (a?.state === 'pending' || a?.state === 'exchanging')
  // A token is always enterable; a sign-in only starts from a settled
  // authorization. Without one there is no form for the dialog to submit.
  const canAsk = bearer || canStart
  return (
    <EmptyPage
      iconName="Lock"
      title="Sign in to see what this provider can do"
      description={`${setup.serverName || 'This server'} lists its operations only to a signed-in caller. ${DISCOVERY_ONLY}`}
      action={
        <div className={styles.emptyActions}>
          <SignInStatus setup={setup} />
          {result?.error && <Banner variant="error">{result.error}</Banner>}
          {/* The OAuth form stays out here, because the portal renders the dialog
          elsewhere and its button names this form. A secret is typed in the
          dialog, so that form goes in with it. */}
          {!bearer && canStart && (
            <Form ref={oauthForm} id={formId} method="post" reloadDocument aria-busy={pending}>
              <Fields form={form} intent="oauth-start" />
              <SetupFields setup={setup} />
              <OAuthApplicationBlock
                setup={setup}
                callbackUrl={callbackUrl}
                clientSecretInput={
                  <>
                    <Typography.BodySmallStrong as="label" htmlFor={secretId}>
                      Client secret
                    </Typography.BodySmallStrong>
                    <Inputs.CredentialInput id={secretId} name="clientSecret" required />
                  </>
                }
              />
            </Form>
          )}
          {!bearer && !canStart && !inFlight && (
            <OAuthApplicationBlock setup={setup} callbackUrl={callbackUrl} />
          )}
          {canAsk && !inFlight && (
            <Dialog.Root
              open={asking}
              onOpenChange={(open) => {
                // The dialog covers the sign-in form's fields, such as a client
                // secret, so they must be complete before it opens.
                if (open && oauthForm.current && !oauthForm.current.reportValidity()) return
                setAsking(open)
              }}
            >
              <Dialog.Trigger
                render={<Button.Container variant="primary" size="36" disabled={pending} />}
              >
                <Button.Text>
                  {bearer ? 'Enter a token to read the operations' : signInLabel(setup)}
                </Button.Text>
              </Dialog.Trigger>
              <Dialog.Portal>
                <Dialog.Backdrop />
                <Dialog.Popup>
                  <Dialog.Title>Read the operations</Dialog.Title>
                  <Dialog.Description>{DISCOVERY_ONLY}</Dialog.Description>
                  {bearer && (
                    <Form
                      id={formId}
                      method="post"
                      reloadDocument
                      className={styles.stack}
                      aria-busy={pending}
                    >
                      <Fields form={form} intent="secret" />
                      <SetupFields setup={setup} />
                      <CredentialFields fields={setup.credentialFields}>
                        {(input) => <Inputs.CredentialInput {...input} />}
                      </CredentialFields>
                      <CredentialDestination setup={setup} />
                    </Form>
                  )}
                  <Dialog.Actions>
                    <Button.TextButton
                      type="button"
                      variant="secondary"
                      disabled={pending}
                      onClick={() => setAsking(false)}
                    >
                      Cancel
                    </Button.TextButton>
                    <DocumentSubmitButton form={formId} disabled={pending}>
                      {bearer ? 'List operations' : signInLabel(setup)}
                    </DocumentSubmitButton>
                  </Dialog.Actions>
                </Dialog.Popup>
              </Dialog.Portal>
            </Dialog.Root>
          )}
          {/* A consent in flight takes the CTA's place, so the panel keeps one
              action in one spot instead of growing a second row. */}
          {a?.state === 'pending' && <ContinueSignIn setup={setup} form={form} pending={pending} />}
          {a?.state === 'exchanging' && (
            <Typography.Body as="p">Completing sign-in…</Typography.Body>
          )}
        </div>
      }
    />
  )
}

/**
 * After configuration review: authorize protected MCP discovery before listing
 * operations. Every button is one Coral call.
 */
export function DiscoveryStep({
  setup,
  form,
  pending,
  result,
  callbackUrl,
  backPath,
  existingAccount,
}: {
  setup: SetupView
  form: FormContext
  pending: boolean
  result?: StepperResult
  callbackUrl: string
  /** Where to revise the confirmed configuration. */
  backPath?: string
  existingAccount?: AccountChoice
}) {
  const formId = useId()
  const secretId = useId()
  const back = backPath ? (
    <Button.InternalLink size="small" to={backPath} variant="secondary">
      Back
    </Button.InternalLink>
  ) : null
  const expiry = setup.expiresAt ? (
    <Typography.BodySmall as="p" variant="tertiary">
      This attempt expires at {formatTime(setup.expiresAt)}.
    </Typography.BodySmall>
  ) : null
  if (setup.state === 'awaiting_discovery')
    return (
      <div className={styles.stack}>
        <Banner variant="warning" title="Coral cannot read the operations">
          {setup.serverName || 'The server'} did not list its operations.{' '}
          {setup.discoveryFailure || ''} Select Try again to request the list. You do not need to
          sign in again.
        </Banner>
        {result?.error && <Banner variant="error">{result.error}</Banner>}
        {expiry}
        <div className={styles.stepActions}>
          {back}
          <Form method="post">
            <Fields
              form={form}
              intent={
                setup.configuration?.updating && !setup.authorized
                  ? 'borrow-account'
                  : 'oauth-retry'
              }
            />
            <SetupFields setup={setup} />
            <Button.TextButton type="submit" size="36" disabled={pending} busy={pending}>
              Try again
            </Button.TextButton>
          </Form>
        </div>
      </div>
    )
  // Retrying discovery needs no credentials, so only signing in stops here.
  if (setup.credentialMethod === null) return <UnknownMethod back={back} />
  const a = setup.authorization
  const scopes = a?.requestedScopes ?? []
  const canStart = !a || a.state === 'failed' || a.state === 'expired' || a.state === 'canceled'
  return (
    <div className={styles.stack}>
      {setup.configuration?.updating && (
        <Typography.BodyStrong as="h2">Access to read operations</Typography.BodyStrong>
      )}
      <Typography.Body as="p">
        Sign in to let Coral read the operations. You choose how to use this account after
        discovery.
      </Typography.Body>
      {setup.configuration?.updating && existingAccount !== 'none' && (
        <Form method="post">
          <Fields form={form} intent="borrow-account" />
          <SetupFields setup={setup} />
          <Typography.BodySmall as="p">
            Reading operations does not change your connected accounts. Coral checks compatibility
            before using this account.
          </Typography.BodySmall>
          <Button.TextButton type="submit" disabled={pending}>
            {existingAccount === 'fixed' ? 'Use the existing shared account' : 'Use your account'}
          </Button.TextButton>
        </Form>
      )}
      {setup.configuration?.updating && (
        <Typography.BodySmall as="p">
          Or sign in temporarily to read operations. This does not connect an account for future
          calls.
        </Typography.BodySmall>
      )}
      <SignInStatus setup={setup} />
      {result?.error && <Banner variant="error">{result.error}</Banner>}
      {scopes.length > 0 && (
        <Typography.BodySmall as="p">
          The server asks for these permissions: {scopes.join(', ')}.
        </Typography.BodySmall>
      )}
      {setup.credentialMethod === 'bearer' ? (
        <>
          <Form
            id={formId}
            method="post"
            reloadDocument
            className={styles.stack}
            aria-busy={pending}
          >
            <Fields form={form} intent="secret" />
            <SetupFields setup={setup} />
            <CredentialFields fields={setup.credentialFields}>
              {(input) => <Inputs.CredentialInput {...input} />}
            </CredentialFields>
            <CredentialDestination setup={setup} />
          </Form>
          <div className={styles.stepActions}>
            {back}
            <DocumentSubmitButton form={formId} size="36" disabled={pending}>
              List operations
            </DocumentSubmitButton>
          </div>
        </>
      ) : canStart ? (
        <>
          <Form
            id={formId}
            method="post"
            reloadDocument
            className={styles.stack}
            aria-busy={pending}
          >
            <Fields form={form} intent="oauth-start" />
            <SetupFields setup={setup} />
            <OAuthApplicationBlock
              setup={setup}
              callbackUrl={callbackUrl}
              clientSecretInput={
                <>
                  <Typography.BodySmallStrong as="label" htmlFor={secretId}>
                    Client secret
                  </Typography.BodySmallStrong>
                  <Inputs.CredentialInput id={secretId} name="clientSecret" required />
                </>
              }
            />
          </Form>
          {expiry}
          <div className={styles.stepActions}>
            {back}
            <DocumentSubmitButton form={formId} size="36" disabled={pending}>
              {signInLabel(setup)}
            </DocumentSubmitButton>
          </div>
        </>
      ) : (
        <>
          <OAuthApplicationBlock setup={setup} callbackUrl={callbackUrl} />
          {expiry}
        </>
      )}
      <SignInProgress setup={setup} form={form} pending={pending} aside={back} />
    </div>
  )
}

/** Coral chose a sign-in method this Reef cannot collect, as during a rolling deploy. */
export function UnknownMethod({ title, back }: { title?: string; back: ReactNode }) {
  return (
    <StepFrame title={title} actions={back}>
      <Banner variant="warning" title="You cannot sign in to this provider here yet">
        This version of Coral does not support the provider's sign-in method. Reload the page in a
        few minutes. If you still see this message, contact your organization owner.
      </Banner>
    </StepFrame>
  )
}

/** A few words for the rail; the Review row carries the full sentence. */
export function accountShortSummary(account: AccountChoice) {
  if (account === 'none') return 'No account needed'
  return accountLabels[account]
}

/** What the account step decided, once it is done. */
export function accountSummary(setup: SetupView, account: AccountChoice, retained = false) {
  const granted =
    (setup.grantedScope ? ` Granted permissions: ${setup.grantedScope}.` : '') +
    (setup.automaticRefresh === false
      ? ' This account cannot refresh automatically. Reconnect when access expires.'
      : '')
  if (account === 'none') return 'No account needed'
  return account === 'fixed'
    ? `${accountLabels.fixed}. Signed in.${granted}`
    : retained
      ? `${accountLabels.caller}. You keep this sign-in for your account.${granted}`
      : `${accountLabels.caller}. Connect your account when you first use the provider.`
}

/**
 * What the account choice means, without restating the choice itself: Review
 * shows the short summary as the value and this under it, so neither repeats.
 */
export function accountDetail(setup: SetupView, account: AccountChoice, retained = false) {
  const granted =
    (setup.grantedScope ? `Granted permissions: ${setup.grantedScope}.` : '') +
    (setup.automaticRefresh === false
      ? ' This account cannot refresh automatically. Reconnect when access expires.'
      : '')
  if (account === 'none') return ''
  return account === 'fixed'
    ? `Signed in. ${granted}`.trim()
    : retained
      ? `You keep this sign-in for your account. ${granted}`.trim()
      : 'Connect your account when you first use the provider.'
}

export function formatTime(iso: string) {
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return iso
  return `${date.toISOString().slice(11, 16)} UTC`
}
