import { useEffect, useId, useRef, useState } from 'react'
import { Form, useBeforeUnload, useBlocker, useNavigation, useSubmit } from 'react-router'
import {
  attemptPath,
  attemptProgress,
  isCredentialFree,
  isOAuthMethod,
  reviseAttempt,
} from '@/lib/provider-publication-attempt/model'
import { Banner, Button, Radio, Typography } from '@/wax/components'
import * as Dialog from '@/wax/components/dialog'
import { PageLayout } from '@/wax/components/page-layout'
import { trail } from '@/views/management/layout'
import { Fields } from '@/views/management/forms'
import { DocumentSubmitButton } from '@/components/document-submit-button'
import { ConfigurationStep } from './configuration-step'
import { ToolsSection, ToolsSummary } from './tools-section'
import { RefreshLayout } from './update-source'
import { DiscoveryStep, SignInStatus } from './setup-steps'
import { usePublication } from './publication-form'
import { StepFrame, StepPage } from './step-frame'
import { UpdateAuthorization } from './update-authorization'
import { accountLabels, useAccountAudience } from './account-labels'
import { AttemptEnded } from './attempt-ended'
import type { AccountChoice, SetupView, UpdatePageProps } from './types'
import * as styles from './providers.css'

/**
 * One outcome for whose account calls use: keep the shared account, use the
 * sign-in in hand, connect another, or let each person connect their own.
 */
type AccountPlan = 'shared-current' | 'shared-held' | 'shared-connect' | 'caller'

/**
 * Edit a provider's configuration or account setting on one page. Save
 * publishes it. A sign-in, an account step or a confirmation follows only when
 * Coral needs one.
 */
export function EditConfigurationPage({
  currentAccount = 'fixed',
  displayName,
  callbackUrl = '',
  basePath,
  cancelPath,
  settingsPath = `${cancelPath}/settings`,
  providersPath,
  currentTools,
  source,
  etag,
  setup,
  attempt,
  form,
  result,
  pending = false,
}: UpdatePageProps & { setup: SetupView }) {
  const [formDirty, setFormDirty] = useState(false)
  const [signingIn, setSigningIn] = useState(false)
  const signInFormId = useId()
  const held = setup.authorized && Boolean(setup.provisionalGrantId)
  const [plan, setPlan] = useState<AccountPlan>(() =>
    (attempt.account ?? currentAccount) === 'caller'
      ? 'caller'
      : held
        ? 'shared-held'
        : // A sign-in in progress or one that failed survives a reload or Back,
          // with its recovery controls in view.
          setup.authorization && setup.authorization.state !== 'succeeded'
          ? 'shared-connect'
          : currentAccount === 'fixed' && setup.configuration?.interruptsAccess !== true
            ? 'shared-current'
            : 'shared-connect',
  )
  // A device sign-in finishes while this page revalidates. Use it, as a reload would.
  const [seenGrant, setSeenGrant] = useState(held ? setup.provisionalGrantId : undefined)
  if (held && setup.provisionalGrantId !== seenGrant) {
    setSeenGrant(setup.provisionalGrantId)
    if (plan === 'shared-connect') setPlan('shared-held')
  }
  const setting: AccountChoice = plan === 'caller' ? 'caller' : 'fixed'
  const audience = useAccountAudience()
  const frozen = attempt.state === 'frozen'
  const { stage, ended, requiresInterruptionAcceptance } = attemptProgress(attempt, setup, result)
  // Coral says when a source takes no credential, and so no account.
  const credentialFree = isCredentialFree(setup)
  const route = frozen ? (attempt.account ?? currentAccount) : credentialFree ? 'none' : setting
  const settingChanged = route !== currentAccount
  // A provider without accounts that now needs one asks whose account it uses.
  // Change account setting asks it first.
  const accounts = attempt.task === 'accounts'
  const refresh = (attempt.task ?? 'refresh') === 'refresh'
  const title = refresh
    ? 'Check for updates'
    : accounts
      ? 'Change account setting'
      : 'Edit configuration'
  const needsChoice = accounts || (currentAccount === 'none' && route !== 'none')
  const needsReplacement = route === 'fixed' && (settingChanged || requiresInterruptionAcceptance)
  // A sign-in from reading the tools can become the shared account.
  const sharedDecision = route === 'fixed' && Boolean(setup.authorized || setup.authorization)
  const grant = frozen
    ? attempt.accountGrant
    : route === 'fixed' && plan === 'shared-held'
      ? setup.provisionalGrantId
      : undefined
  // Coral could not check whether existing accounts keep working, so a save
  // must accept that they may not.
  const accessUnknown =
    !frozen && currentAccount !== 'none' && setup.configuration?.interruptsAccess === null
  const needsConsent =
    settingChanged || requiresInterruptionAcceptance || Boolean(grant) || accessUnknown
  const view = ended
    ? 'ended'
    : frozen || result?.failed
      ? 'outcome'
      : stage === 'discovery'
        ? 'tools'
        : stage !== 'configuration' && (needsChoice || needsReplacement || sharedDecision)
          ? 'account'
          : refresh && stage !== 'configuration'
            ? 'catalog'
            : 'configuration'
  // A confirmed configuration is a change too, until Save publishes it. Change
  // account setting confirms the saved one before the page opens.
  const unsaved =
    !ended &&
    !result?.failed &&
    (formDirty ||
      (accounts
        ? settingChanged || Boolean(setup.authorized || setup.authorization)
        : setup.state !== 'awaiting_configuration' || Boolean(result?.configurationDraft)))
  const [confirming, setConfirming] = useState(view === 'configuration' && stage === 'publish')
  const publication = usePublication({
    attempt: {
      ...attempt,
      account: route,
      accountGrant: grant,
      allowIncompatibleConfiguration: frozen
        ? attempt.allowIncompatibleConfiguration
        : needsConsent,
    },
    setup,
    basePath,
    // A change opens from settings, so you start it over there.
    startOverPath: settingsPath,
    form,
    result,
    pending,
  })
  const saveDisabled =
    pending ||
    // The same setting again changes nothing.
    (accounts && !settingChanged && !requiresInterruptionAcceptance) ||
    (needsReplacement && !grant) ||
    (route === 'fixed' && plan === 'shared-connect')
  const cancelForm = useRef<HTMLFormElement>(null)
  const leave = useLeaveGuard(unsaved, cancelForm)
  const cancel = (
    <Form method="post" ref={cancelForm} onSubmit={leave.onCancel}>
      <Fields form={form} intent="cancel" />
      <input type="hidden" name="setup" value={setup.id} />
      <Button.TextButton type="submit" variant="secondary" disabled={pending}>
        Cancel
      </Button.TextButton>
    </Form>
  )
  // Start again opens a new Setup for the same task, as settings does. A refresh
  // reads the URL this Setup read, not the saved one: Replace source reads a new
  // URL. An uploaded document needs a new upload in settings.
  const rereads = refresh && setup.sourceHost ? setup.sourceLabel : ''
  const restart =
    refresh && !rereads ? (
      <Button.InternalLink to={settingsPath}>Choose a source</Button.InternalLink>
    ) : (
      <Form method="post">
        <Fields form={form} intent="prepare" />
        <input type="hidden" name="etag" value={etag ?? attempt.etag} />
        <input type="hidden" name="task" value={attempt.task ?? 'refresh'} />
        {rereads ? (
          <>
            <input type="hidden" name="source" value={setup.kind === 'mcp' ? 'mcp' : 'url'} />
            <input type="hidden" name="sourceUrl" value={rereads} />
          </>
        ) : (
          <input type="hidden" name="source" value="current" />
        )}
        <Button.TextButton type="submit" disabled={pending} busy={pending}>
          Start again
        </Button.TextButton>
      </Form>
    )
  const configurationPath = attemptPath(basePath, reviseAttempt(attempt, 'configuration'))
  const back = (
    <Button.InternalLink size="small" variant="secondary" to={configurationPath}>
      Back
    </Button.InternalLink>
  )
  const save = (
    <Button.TextButton
      disabled={saveDisabled}
      busy={pending}
      {...(needsConsent
        ? { onClick: () => setConfirming(true) }
        : { type: 'submit', form: publication.formId })}
    >
      Save
    </Button.TextButton>
  )
  // Connecting the shared account asks first, as Add provider does. The sign-in
  // or the credentials go in that dialog. A sign-in in progress stays on the page.
  const connecting = view === 'account' && route === 'fixed' && plan === 'shared-connect'
  const signInOpen =
    setup.authorization?.state === 'pending' || setup.authorization?.state === 'exchanging'
  const oauth = isOAuthMethod(setup.credentialMethod)
  const connect = (
    <Dialog.Root open={signingIn} onOpenChange={setSigningIn}>
      <Dialog.Trigger render={<Button.Container variant="primary" disabled={pending} />}>
        <Button.Text>Connect the shared account</Button.Text>
      </Dialog.Trigger>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup>
          <Dialog.Title>{audience.shareTitle}</Dialog.Title>
          <Dialog.Description>{audience.shareBody}</Dialog.Description>
          <UpdateAuthorization
            setup={setup}
            form={form}
            pending={pending}
            callbackUrl={callbackUrl}
            formId={signInFormId}
          />
          <Dialog.Actions>
            <Button.TextButton
              type="button"
              variant="secondary"
              disabled={pending}
              onClick={() => setSigningIn(false)}
            >
              Cancel
            </Button.TextButton>
            <DocumentSubmitButton form={signInFormId} disabled={pending}>
              {oauth ? 'Sign in and share the account' : 'Save and share the account'}
            </DocumentSubmitButton>
          </Dialog.Actions>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
  const content =
    view === 'ended' ? (
      <AttemptEnded ended={ended!} error={result?.error}>
        <Button.InternalLink to={settingsPath} variant="secondary">
          Back to settings
        </Button.InternalLink>
        {restart}
      </AttemptEnded>
    ) : view === 'outcome' ? (
      <StepFrame title={title} actions={publication.actions}>
        {publication.body}
      </StepFrame>
    ) : view === 'catalog' ? (
      <StepFrame
        title={title}
        meta={<ToolsSummary setup={setup} discovering={false} />}
        actions={
          <>
            {cancel}
            {save}
          </>
        }
      >
        {publication.body}
        <RefreshLayout source={source}>
          <ToolsSection setup={setup} previous={currentTools} />
        </RefreshLayout>
      </StepFrame>
    ) : view === 'tools' ? (
      <StepFrame title="Tools" actions={cancel}>
        <DiscoveryStep
          setup={setup}
          form={form}
          pending={pending}
          result={result}
          callbackUrl={callbackUrl}
          backPath={configurationPath}
          existingAccount={currentAccount}
        />
      </StepFrame>
    ) : view === 'account' ? (
      <StepFrame
        title={accounts ? title : 'Account'}
        actions={
          <>
            {cancel}
            {!accounts && back}
            {connecting && !signInOpen ? connect : save}
          </>
        }
      >
        {publication.body}
        {needsChoice && credentialFree ? (
          <Typography.Body as="p">This provider does not need an account.</Typography.Body>
        ) : (
          <div className={styles.accountQuestion}>
            {/* As in Add provider: one shared account on the left, individual
                accounts on the right. Each option is an outcome. */}
            <Radio.Group
              layout={needsChoice ? 'grid' : 'stack'}
              className={styles.accountRoutes}
              aria-label="Whose account does it use?"
              value={plan}
              onValueChange={(next) => setPlan(next as AccountPlan)}
            >
              <div className={styles.accountColumn}>
                <Typography.BodyLargeStrong as="h3">
                  {accountLabels.fixed}
                </Typography.BodyLargeStrong>
                {currentAccount === 'fixed' && !requiresInterruptionAcceptance && (
                  <Radio.Item
                    variant="card"
                    value="shared-current"
                    description={audience.sharedAccount}
                  >
                    Keep the current account
                  </Radio.Item>
                )}
                {held && (
                  <Radio.Item
                    variant="card"
                    value="shared-held"
                    description={`${audience.sharedCallers} the account you signed in to.${
                      setup.grantedScope ? ` Granted permissions: ${setup.grantedScope}.` : ''
                    }`}
                  >
                    Use this account
                  </Radio.Item>
                )}
                <Radio.Item
                  variant="card"
                  value="shared-connect"
                  description={`You sign in${held ? ' again' : ' once'}. ${audience.sharedCallers} that account.`}
                >
                  {held || currentAccount === 'fixed'
                    ? 'Connect another account'
                    : 'Connect an account'}
                </Radio.Item>
              </div>
              {needsChoice && (
                <div className={styles.accountColumn}>
                  <Typography.BodyLargeStrong as="h3">
                    {accountLabels.caller}
                  </Typography.BodyLargeStrong>
                  <Radio.Item
                    variant="card"
                    value="caller"
                    description={audience.individualAccounts}
                  >
                    {currentAccount === 'caller'
                      ? 'Keep individual accounts'
                      : 'Use individual accounts'}
                  </Radio.Item>
                </div>
              )}
            </Radio.Group>
          </div>
        )}
        {connecting && !signInOpen && <SignInStatus setup={setup} />}
        {connecting && signInOpen && (
          <UpdateAuthorization
            setup={setup}
            form={form}
            pending={pending}
            callbackUrl={callbackUrl}
          />
        )}
      </StepFrame>
    ) : (
      <>
        <ConfigurationStep
          key={`${setup.id}:${setup.configuration?.revision}`}
          setup={setup}
          form={form}
          pending={pending}
          result={result}
          callbackUrl={callbackUrl}
          title={title}
          actions={cancel}
          // A refresh shows the tools next, so its form only confirms.
          confirmLabel={refresh ? 'Next' : 'Save'}
          // Only a new read can differ from what is saved. Edit configuration
          // reads the saved source, so its defaults are no change.
          compare={refresh}
          unchanged={!refresh && !unsaved}
          retry={restart}
          onDirtyChange={setFormDirty}
        />
        {/* Save confirms the configuration first. When Coral needs your
            confirmation to publish it, the dialog asks for it. */}
        {stage === 'publish' && <div hidden>{publication.body}</div>}
      </>
    )
  return (
    <PageLayout
      variant="wide"
      breadcrumbs={trail(
        [
          { label: 'Providers', to: providersPath },
          { label: displayName, to: cancelPath },
          { label: 'Settings', to: settingsPath },
        ],
        title,
      )}
    >
      <div className={styles.setupPage} onSubmitCapture={leave.onSubmit}>
        <StepPage.Provider value>{content}</StepPage.Provider>
        <Dialog.Root open={confirming && view !== 'outcome'} onOpenChange={setConfirming}>
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup>
              <Dialog.Title>Save changes?</Dialog.Title>
              <Dialog.Description>
                {settingChanged
                  ? 'Save removes the existing account credentials. To switch back, connect new accounts.'
                  : accessUnknown && !requiresInterruptionAcceptance && !grant
                    ? 'Accounts may need to reconnect after you save.'
                    : !needsConsent
                      ? 'Save the confirmed configuration.'
                      : route === 'caller'
                        ? audience.reconnect
                        : 'This change replaces the shared account authorization. Pending calls cannot switch accounts.'}
              </Dialog.Description>
              {setup.configuration?.diagnostic && (
                <Banner variant="warning">{setup.configuration.diagnostic}</Banner>
              )}
              {setup.configuration?.interruptsAccess === null && (
                <Typography.BodySmall as="p">
                  Coral cannot check account access for this configuration. Read the diagnostics
                  before you save.
                </Typography.BodySmall>
              )}
              {result?.error && <Banner variant="error">{result.error}</Banner>}
              <Dialog.Actions>
                <Button.TextButton variant="secondary" onClick={() => setConfirming(false)}>
                  Keep editing
                </Button.TextButton>
                <Button.TextButton
                  type="submit"
                  form={publication.formId}
                  variant={needsConsent ? 'destructive' : 'primary'}
                  disabled={saveDisabled}
                  busy={pending}
                >
                  Save
                </Button.TextButton>
              </Dialog.Actions>
            </Dialog.Popup>
          </Dialog.Portal>
        </Dialog.Root>
        <Dialog.Root open={leave.asking} onOpenChange={(open) => !open && leave.stay()}>
          <Dialog.Portal>
            <Dialog.Backdrop />
            <Dialog.Popup>
              <Dialog.Title>Discard your changes?</Dialog.Title>
              <Dialog.Description>Your changes are not saved.</Dialog.Description>
              <Dialog.Actions>
                <Button.TextButton variant="secondary" onClick={leave.stay}>
                  Keep editing
                </Button.TextButton>
                <Button.TextButton variant="destructive" onClick={leave.discard}>
                  Discard changes
                </Button.TextButton>
              </Dialog.Actions>
            </Dialog.Popup>
          </Dialog.Portal>
        </Dialog.Root>
      </div>
    </PageLayout>
  )
}

/**
 * Asks before unsaved changes are lost: Cancel, a link to another page, or
 * closing the tab. Steps of this page and form submissions pass. Discarding
 * always cancels the Setup.
 */
function useLeaveGuard(unsaved: boolean, cancelForm: React.RefObject<HTMLFormElement | null>) {
  const submitting = useRef(false)
  const [cancelAsked, setCancelAsked] = useState(false)
  const submit = useSubmit()
  const navigation = useNavigation()
  useEffect(() => {
    if (navigation.state === 'idle') submitting.current = false
  }, [navigation.state])
  const blocker = useBlocker(
    ({ currentLocation, nextLocation }) =>
      unsaved && !submitting.current && currentLocation.pathname !== nextLocation.pathname,
  )
  useBeforeUnload((event) => {
    if (unsaved && !submitting.current) {
      event.preventDefault()
      event.returnValue = ''
    }
  })
  return {
    asking: cancelAsked || blocker.state === 'blocked',
    /** Cancel with unsaved changes asks first; Discard submits it. */
    onCancel: (event: React.FormEvent) => {
      if (!unsaved) return
      event.preventDefault()
      setCancelAsked(true)
    },
    /** A form on the page that submits leaves without a question; Cancel asks first. */
    onSubmit: (event: React.FormEvent) => {
      if (event.target !== cancelForm.current) submitting.current = true
    },
    stay: () => {
      setCancelAsked(false)
      if (blocker.state === 'blocked') blocker.reset()
    },
    /** Cancels the Setup, then goes where you were going: the link, or settings. */
    discard: () => {
      if (!cancelForm.current) return
      const fields = new FormData(cancelForm.current)
      if (blocker.state === 'blocked') {
        const { pathname, search, hash } = blocker.location
        fields.set('leaveTo', `${pathname}${search}${hash}`)
        blocker.reset()
      }
      setCancelAsked(false)
      submitting.current = true
      void submit(fields, { method: 'post' })
    },
  }
}
