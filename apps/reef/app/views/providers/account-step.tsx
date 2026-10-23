import { useId, useState } from 'react'
import { Form } from 'react-router'
import {
  accountStep,
  attemptProgress,
  isOAuthMethod,
  needsOAuthApplication,
  type AccountChoice,
  type AccountPlan,
  type CreationAttempt,
} from '@/lib/provider-publication-attempt/model'
import { Banner, Button, Inputs, Radio, Typography } from '@/wax/components'
import * as Dialog from '@/wax/components/dialog'
import { DocumentSubmitButton } from '@/components/document-submit-button'
import { Fields } from '@/views/management/forms'
import { accountLabels, useAccountAudience } from './account-labels'
import { CredentialFields } from '@/views/management/credential-fields'
import { OAuthApplicationBlock } from './oauth-application'
import { usePublication } from './publication-form'
import {
  accountSummary,
  ContinueSignIn,
  CredentialDestination,
  SetupFields,
  SignInProgress,
  SignInStatus,
  signInLabel,
  UnknownMethod,
} from './setup-steps'
import type { FormContext, SetupView, StepperResult } from './types'
import { StepFrame } from './step-frame'
import * as styles from './providers.css'

/**
 * Account: choose runtime use after discovery. Shared use requires sign-in;
 * separate use can connect now, retain the reviewed grant, or connect later.
 */
const SIGN_IN_ACTION = 'Sign in and share the account'

/**
 * Whose account the provider uses, as two columns of outcomes: one shared
 * account on the left, individual accounts on the right. A sign-in made to read
 * the operations is already in hand by now, so keeping it and replacing it are
 * options under the route they belong to rather than a control beside one.
 */
export function AccountStep({
  attempt,
  setup,
  basePath,
  form,
  pending,
  result,
  callbackUrl,
  backPath,
  documentationUrl = null,
  disabled = false,
}: {
  /** The attempt as the page holds it, with this step's own edits already applied. */
  attempt: CreationAttempt
  setup: SetupView
  basePath: string
  form: FormContext
  pending: boolean
  result?: StepperResult
  callbackUrl: string
  /** The step before this one, which accepted the operations. */
  backPath: string
  /** Where the recipe says to find the credentials to enter. */
  documentationUrl?: string | null
  disabled?: boolean
}) {
  const { account: settled, credentialFree } = attemptProgress(attempt, setup, result)
  const audience = useAccountAudience()
  const grant = setup.provisionalGrantId
  const held = setup.authorized && Boolean(grant)
  const authorization = setup.authorization
  const opening = accountStep(attempt, setup)
  // Giving up a sign-in in hand is a choice someone makes, not one a reload makes.
  const [plan, setPlan] = useState<AccountPlan>(opening.plan)
  // A device sign-in finishes in another tab while this page revalidates, so
  // the new account arrives without a reload. Keep it, as a reload would.
  const [seenGrant, setSeenGrant] = useState(held ? grant : undefined)
  if (held && grant !== seenGrant) {
    setSeenGrant(grant)
    setPlan((current) =>
      current === 'shared-connect'
        ? 'shared-keep'
        : current === 'own-connect'
          ? 'own-keep'
          : current,
    )
  }
  const formId = useId()
  const secretId = useId()
  const secretFormId = useId()
  const [confirming, setConfirming] = useState(false)
  const method = setup.credentialMethod
  const oauth = isOAuthMethod(method)
  const shared = plan === 'shared-keep' || plan === 'shared-connect'
  const choice: AccountChoice = shared ? 'fixed' : 'caller'
  const frozen = attempt.state === 'frozen'
  // The two outcomes that use the sign-in in hand, and the two that replace it.
  const keeping = plan === 'shared-keep' || (plan === 'own-keep' && held)
  const connecting = plan === 'shared-connect' || plan === 'own-connect'
  const replacing = connecting && held
  // A committed attempt is being retried: nothing here is a question any more.
  const signingIn = !frozen && !credentialFree && connecting
  // Whatever needs no sign-in is added from this page. There is no page after it.
  const submission: CreationAttempt = frozen
    ? attempt
    : credentialFree
      ? { ...attempt, account: 'none', accountGrant: undefined }
      : { ...attempt, account: choice, accountGrant: keeping ? grant : undefined }
  const inFlight =
    signingIn &&
    oauth &&
    (authorization?.state === 'pending' || authorization?.state === 'exchanging')
  // A token, API key or password is entered in the confirmation, the same place
  // OAuth confirms before leaving for the service; neither belongs on the step,
  // where it would ask for a credential nobody has chosen to give yet.
  const entering = signingIn && method !== null && method !== 'none' && !oauth
  const unstartable = signingIn && needsOAuthApplication(setup)
  const intent = signingIn ? (oauth ? 'oauth-start' : 'secret') : 'account'
  // Said on the options it qualifies, not as a line of its own under them.
  const refresh =
    held && setup.automaticRefresh === false
      ? ' This account cannot refresh automatically. Reconnect when access expires.'
      : ''
  const accountField = <input type="hidden" name="account" value={choice} />
  const back = (
    <Button.InternalLink size="small" to={backPath} variant="secondary">
      Back
    </Button.InternalLink>
  )
  // Starting a sign-in from this question can be the answer. Once it lands,
  // add the provider as Add provider would, rather than ask again.
  const answered =
    !frozen && oauth && keeping && opening.publishOnReturn && authorization?.state === 'succeeded'
  const publication = usePublication({
    submitNow: answered,
    attempt: submission,
    setup,
    basePath,
    form,
    result,
    pending,
    disabled,
    backPath,
  })
  if (method === null) return <UnknownMethod title="Account" back={back} />
  const question = (
    <>
      {/* A provider that takes no credentials has one answer, shown as the one
          option it is rather than as a sentence about the question. */}
      {credentialFree && !frozen ? (
        <div className={styles.wideField}>
          <Radio.Group layout="stack" aria-label="Whose account does it use?" value="none">
            <Radio.Item variant="card" value="none" description={audience.free}>
              No account needed
            </Radio.Item>
          </Radio.Group>
        </div>
      ) : settled && frozen ? (
        <Typography.Body as="p">
          {accountSummary(setup, settled, Boolean(attempt.accountGrant))}
        </Typography.Body>
      ) : (
        <Form id={formId} method="post" reloadDocument className={styles.stack} aria-busy={pending}>
          <Fields form={form} intent={intent} />
          <SetupFields
            setup={setup}
            extra={
              <>
                {accountField}
                {/* Keeping the sign-in names it; replacing it names the one it replaces. */}
                {keeping && <input type="hidden" name="accountGrant" value={grant} />}
                {replacing && <input type="hidden" name="replacementGrant" value={grant} />}
              </>
            }
          />
          {oauth && <SignInStatus setup={setup} />}
          {signingIn && result?.error && <Banner variant="error">{result.error}</Banner>}
          <div className={styles.accountQuestion}>
            <Radio.Group
              layout="grid"
              className={styles.accountRoutes}
              aria-label="Whose account does it use?"
              value={plan}
              onValueChange={(next) => setPlan(next as AccountPlan)}
            >
              <div className={styles.accountColumn}>
                <Typography.BodyLargeStrong as="h3">
                  {accountLabels.fixed}
                </Typography.BodyLargeStrong>
                {held && (
                  <Radio.Item
                    variant="card"
                    value="shared-keep"
                    description={
                      `${audience.sharedCallers} the account you signed in to.` + refresh
                    }
                  >
                    Use this account
                  </Radio.Item>
                )}
                <Radio.Item
                  variant="card"
                  value="shared-connect"
                  description={`${held ? 'You sign in again.' : 'You sign in once.'} ${audience.sharedCallers} that account.`}
                >
                  {held ? 'Connect another account' : 'Connect an account'}
                </Radio.Item>
              </div>
              <div className={styles.accountColumn}>
                <Typography.BodyLargeStrong as="h3">
                  {accountLabels.caller}
                </Typography.BodyLargeStrong>
                {held && (
                  <Radio.Item
                    variant="card"
                    value="own-keep"
                    description={'This account becomes yours. ' + audience.others + refresh}
                  >
                    Connect now with this account
                  </Radio.Item>
                )}
                {/* Individual accounts start with one too, and the sign-in already
                    made was for reading the operations, not for using them. */}
                <Radio.Item
                  variant="card"
                  value="own-connect"
                  description={`${held ? 'You sign in again' : 'You sign in now'}, and that account becomes yours. ${audience.others}`}
                >
                  {held ? 'Connect now with another account' : 'Connect now'}
                </Radio.Item>
                {/* Coral says when nobody could connect after an add with no sign-in. */}
                {setup.connectLaterAvailable && (
                  <Radio.Item
                    variant="card"
                    value="own-later"
                    description={
                      held ? `Coral discards this sign-in. ${audience.later}` : audience.later
                    }
                  >
                    Connect later
                  </Radio.Item>
                )}
              </div>
            </Radio.Group>
          </div>
          {oauth && signingIn && (
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
          )}
        </Form>
      )}
    </>
  )
  // A sign-in in flight replaces the step's own action with the way to finish it.
  const signInActions = !inFlight ? (
    <>
      {back}
      {entering ? (
        <Dialog.Root open={confirming} onOpenChange={setConfirming}>
          <Dialog.Trigger render={<Button.Container variant="primary" disabled={pending} />}>
            <Button.Text>
              {shared ? 'Connect the shared account' : 'Connect your account'}
            </Button.Text>
          </Dialog.Trigger>
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup>
              <Dialog.Title>{shared ? audience.shareTitle : 'Connect your account'}</Dialog.Title>
              <Dialog.Description>
                {shared ? audience.shareBody : audience.onlyYou}
              </Dialog.Description>
              {/* Its own form: the portal renders outside the step's, so an
                      input here would never be submitted with it. */}
              <Form
                id={secretFormId}
                method="post"
                reloadDocument
                className={styles.stack}
                aria-busy={pending}
              >
                <Fields form={form} intent="secret" />
                <SetupFields setup={setup} extra={accountField} />
                <CredentialFields fields={setup.credentialFields}>
                  {(input) => <Inputs.CredentialInput {...input} />}
                </CredentialFields>
                <CredentialDestination setup={setup} />
                {documentationUrl && (
                  <Button.ExternalLink href={documentationUrl} className={styles.inlineControl}>
                    Find your credentials in the{' '}
                    {setup.configuration?.values.displayName || 'provider'} docs
                  </Button.ExternalLink>
                )}
              </Form>
              <Dialog.Actions>
                <Button.TextButton
                  type="button"
                  variant="secondary"
                  disabled={pending}
                  onClick={() => setConfirming(false)}
                >
                  Cancel
                </Button.TextButton>
                <DocumentSubmitButton form={secretFormId} disabled={pending}>
                  {shared ? 'Save and share the account' : 'Save your account'}
                </DocumentSubmitButton>
              </Dialog.Actions>
            </Dialog.Popup>
          </Dialog.Portal>
        </Dialog.Root>
      ) : signingIn && oauth && shared ? (
        <Dialog.Root open={confirming} onOpenChange={setConfirming}>
          <Dialog.Trigger
            render={<Button.Container variant="primary" disabled={pending || unstartable} />}
          >
            <Button.Text>{signInLabel(setup)}</Button.Text>
          </Dialog.Trigger>
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup>
              <Dialog.Title>{audience.shareTitle}</Dialog.Title>
              <Dialog.Description>{audience.shareBody}</Dialog.Description>
              <Dialog.Actions>
                <Button.TextButton
                  type="button"
                  variant="secondary"
                  disabled={pending}
                  onClick={() => setConfirming(false)}
                >
                  Cancel
                </Button.TextButton>
                {/* The step's own form is submitted from here; the portal
                        puts this button outside it, so it names the form. */}
                <DocumentSubmitButton form={formId} disabled={pending}>
                  {SIGN_IN_ACTION}
                </DocumentSubmitButton>
              </Dialog.Actions>
            </Dialog.Popup>
          </Dialog.Portal>
        </Dialog.Root>
      ) : (
        <DocumentSubmitButton form={formId} disabled={pending || unstartable}>
          {signingIn && oauth ? signInLabel(setup) : 'Next'}
        </DocumentSubmitButton>
      )}
    </>
  ) : (
    // Exchanging finishes on its own; the button stays, but has nothing to do.
    <>
      {back}
      <ContinueSignIn
        setup={setup}
        form={form}
        pending={pending || authorization?.state === 'exchanging'}
        extra={accountField}
      />
    </>
  )
  // One frame for every answer, so changing the answer keeps the question mounted.
  return (
    <StepFrame title="Account" actions={signingIn ? signInActions : publication.actions}>
      {question}
      {!signingIn
        ? publication.body
        : oauth &&
          authorization?.state === 'exchanging' && (
            <SignInProgress setup={setup} form={form} pending={pending} />
          )}
    </StepFrame>
  )
}
