import { useState } from 'react'
import { Form } from 'react-router'
import {
  attemptPath,
  attemptProgress,
  reviseAttempt,
} from '@/lib/provider-publication-attempt/model'
import { Banner, Button, Inputs, Radio, Typography } from '@/wax/components'
import { Stepper, type Step } from '@/wax/components/stepper'
import { ManagementLayout, trail } from '@/views/management/layout'
import { Fields } from '@/views/management/forms'
import { Disclosure } from '@/wax/components/disclosure'
import { useArrivalToast } from '@/lib/use-arrival-toast'
import { DiscoveryStep } from './setup-steps'
import { UpdateSource } from './update-source'
import { ConfigurationStep } from './configuration-step'
import {
  authenticationLabels,
  ConfigurationFact,
  ConfigurationPreview,
} from './configuration-details'
import { PublicationForm } from './publication-form'
import type { AccountChoice, UpdatePageProps } from './types'
import { UpdateAuthorization } from './update-authorization'
import { accountLabels, useAccountAudience } from './account-labels'
import * as styles from './providers.css'

type SharedAccountChoice =
  | { kind: 'unanswered' | 'keep' | 'connect' }
  | { kind: 'retain'; grantId: string }

const taskTitles = {
  refresh: 'Refresh operations',
  configuration: 'Edit configuration',
  accounts: 'Change account setting',
}

/** Task-specific entry, conditional access work, then one atomic reviewed change. */
export function UpdatePage({
  currentAccount = 'fixed',
  application,
  applicationProblem,
  displayName,
  callbackUrl = '',
  basePath,
  cancelPath,
  providersPath,
  setup,
  attempt,
  form,
  result,
  source,
  replacingSource = false,
  pending = false,
}: UpdatePageProps) {
  const task = attempt.task ?? 'refresh'
  const title = !setup && replacingSource ? 'Replace source' : taskTitles[task]
  const [setting, setSetting] = useState<AccountChoice>(attempt.account ?? currentAccount)
  const [accountChoice, setAccountChoice] = useState<SharedAccountChoice>({ kind: 'unanswered' })
  const [accepted, setAccepted] = useState(false)
  // Replacing the client secret reloads this page with `secretRotated` in the query.
  useArrivalToast(SECRET_ROTATED, () => ({
    title: 'Application secret updated',
    description: 'Your connected accounts keep their authorization.',
  }))
  const audience = useAccountAudience()
  const frozen = attempt.state === 'frozen'
  const { stage, terminal, canRevise, requiresInterruptionAcceptance } = attemptProgress(
    attempt,
    setup,
    result,
  )
  const configuring = stage === 'configuration'
  const discovering = stage === 'discovery'
  const route = frozen
    ? (attempt.account ?? currentAccount)
    : setup?.credentialMethod === 'none'
      ? 'none'
      : setting === 'none'
        ? 'caller'
        : setting
  const settingChanged = route !== currentAccount
  const needsReplacement = route === 'fixed' && (settingChanged || requiresInterruptionAcceptance)
  // A ready Setup's authorization also carries pending/failed replacement work.
  // Keep its recovery controls visible after OAuth redirects and browser Back.
  const sharedDecision = route === 'fixed' && Boolean(setup?.authorized || setup?.authorization)
  const grant =
    frozen || attempt.review
      ? attempt.accountGrant
      : accountChoice.kind === 'retain' && accountChoice.grantId === setup?.provisionalGrantId
        ? accountChoice.grantId
        : undefined
  const needsChoice = task === 'accounts' || (currentAccount === 'none' && route !== 'none')
  const reviewing =
    frozen ||
    Boolean(result?.failed) ||
    (!configuring &&
      !discovering &&
      (attempt.review || (!needsChoice && !needsReplacement && !sharedDecision)))
  const editingAccount =
    !configuring && !discovering && !reviewing && needsChoice && !attempt.access
  const accessRequired = discovering || needsReplacement || sharedDecision
  const choosingAccess = editingAccount && accessRequired
  const configurationPath = attemptPath(basePath, reviseAttempt(attempt, 'configuration'))
  const accountPath = attemptPath(basePath, reviseAttempt(attempt, 'account'))
  const confirmation = settingChanged
    ? currentAccount === 'fixed'
      ? 'Remove existing shared account credentials'
      : currentAccount === 'caller'
        ? 'Remove existing individual account credentials'
        : 'Apply the new account setting'
    : route === 'fixed'
      ? 'Replace the shared account authorization'
      : 'Require each account to reconnect'
  const needsConsent = settingChanged || requiresInterruptionAcceptance || Boolean(grant)
  const accountFields = setup ? (
    <>
      <input type="hidden" name="setup" value={setup.id} />
      <input type="hidden" name="configurationRevision" value={setup.configuration?.revision} />
      <input type="hidden" name="account" value={route} />
      {grant && <input type="hidden" name="accountGrant" value={grant} />}
    </>
  ) : null
  const reviewButton = (
    <Form method="post">
      <Fields form={form} intent={choosingAccess ? 'update-access' : 'review-update'} />
      {accountFields}
      <Button.TextButton
        type="submit"
        disabled={
          pending ||
          (!choosingAccess &&
            ((needsReplacement && !grant) ||
              (sharedDecision && !grant && accountChoice.kind !== 'keep')))
        }
      >
        {choosingAccess ? 'Next' : 'Review changes'}
      </Button.TextButton>
    </Form>
  )
  const steps: Step[] = [
    {
      title: 'Changes',
      state: !setup || configuring || editingAccount ? 'current' : 'done',
      summary: setup ? taskTitles[task] : undefined,
      change:
        setup && !configuring && canRevise && !frozen
          ? {
              to: task === 'accounts' ? accountPath : configurationPath,
              label: task === 'refresh' ? 'Edit configuration' : 'Change',
            }
          : undefined,
      content:
        setup && configuring ? (
          <>
            <Typography.BodyStrong as="h2">Configuration</Typography.BodyStrong>
            {task === 'refresh' && setup.state === 'awaiting_configuration' && (
              <Banner variant="warning">
                Your saved settings need review for this source. Coral keeps the provider unchanged
                until you apply changes.
              </Banner>
            )}
            <ConfigurationStep
              key={`${setup.id}:${setup.configuration?.revision}`}
              setup={setup}
              form={form}
              pending={pending}
              result={result}
              callbackUrl={callbackUrl}
            />
          </>
        ) : setup && editingAccount ? (
          <div className={styles.stack}>
            <Typography.Body as="p">Choose whose account future calls use.</Typography.Body>
            {setup.credentialMethod === 'none' ? (
              <Typography.Body as="p">This provider does not need an account.</Typography.Body>
            ) : (
              <Radio.Group
                aria-label="Whose account does it use?"
                layout="stack"
                value={route}
                onValueChange={(value) => {
                  setSetting(value as AccountChoice)
                  setAccountChoice({ kind: 'unanswered' })
                }}
              >
                <Radio.Item value="fixed">{accountLabels.fixed}</Radio.Item>
                <Radio.Item value="caller">{accountLabels.caller}</Radio.Item>
              </Radio.Group>
            )}
            {reviewButton}
            {result?.error && <Banner variant="error">{result.error}</Banner>}
          </div>
        ) : undefined,
    },
    ...(accessRequired
      ? [
          {
            title: 'Access',
            state: reviewing
              ? ('done' as const)
              : configuring || editingAccount
                ? ('upcoming' as const)
                : ('current' as const),
            summary: reviewing
              ? grant
                ? 'New shared account authorized'
                : 'Keep existing accounts'
              : undefined,
            content:
              !configuring && !reviewing && setup ? (
                discovering ? (
                  <DiscoveryStep
                    setup={setup}
                    form={form}
                    pending={pending}
                    result={result}
                    callbackUrl={callbackUrl}
                    backPath={configurationPath}
                    existingAccount={currentAccount}
                  />
                ) : (
                  <div className={styles.stack}>
                    <Typography.BodyStrong as="h2">Access for future calls</Typography.BodyStrong>
                    <Typography.Body as="p">
                      {needsReplacement
                        ? 'Authorize the new shared account.'
                        : 'Choose whether to keep your existing account or use this sign-in.'}{' '}
                      Your current account stays active until you apply the changes.
                    </Typography.Body>
                    {sharedDecision && (
                      <>
                        {setup.grantedScope && (
                          <Typography.BodySmall as="p">
                            Granted permissions: {setup.grantedScope}.
                          </Typography.BodySmall>
                        )}
                        <Radio.Group
                          aria-label="Choose the shared account"
                          layout="stack"
                          value={accountChoice.kind}
                          onValueChange={(value) => {
                            if (value === 'retain' && setup.provisionalGrantId)
                              setAccountChoice({
                                kind: 'retain',
                                grantId: setup.provisionalGrantId,
                              })
                            else if (value === 'keep' || value === 'connect')
                              setAccountChoice({ kind: value })
                          }}
                        >
                          {!needsReplacement && (
                            <Radio.Item value="keep">Keep the existing shared account</Radio.Item>
                          )}
                          {setup.authorized && (
                            <Radio.Item value="retain">
                              Use this sign-in as the shared account
                            </Radio.Item>
                          )}
                          <Radio.Item value="connect">Connect a different account</Radio.Item>
                        </Radio.Group>
                      </>
                    )}
                    {accountChoice.kind !== 'keep' &&
                      (!setup.authorized || accountChoice.kind === 'connect') && (
                        <UpdateAuthorization
                          setup={setup}
                          form={form}
                          pending={pending}
                          callbackUrl={callbackUrl}
                        />
                      )}
                    {result?.error && <Banner variant="error">{result.error}</Banner>}
                    {reviewButton}
                  </div>
                )
              ) : undefined,
          },
        ]
      : []),
    {
      title: 'Review',
      state: reviewing ? 'current' : 'upcoming',
      content:
        setup && reviewing && !terminal ? (
          <PublicationForm
            attempt={{
              ...attempt,
              account: route,
              accountGrant: grant,
              allowIncompatibleConfiguration: frozen
                ? attempt.allowIncompatibleConfiguration
                : accepted,
            }}
            setup={setup}
            basePath={basePath}
            form={form}
            result={result}
            pending={pending}
            disabled={!frozen && ((needsConsent && !accepted) || (needsReplacement && !grant))}
          >
            <Typography.BodyStrong as="h2">Review changes</Typography.BodyStrong>
            <ConfigurationFact
              label="Name"
              value={
                setup.configuration?.values.displayName !== displayName
                  ? `${displayName} → ${setup.configuration?.values.displayName}`
                  : `${displayName} · unchanged`
              }
            />
            <ConfigurationFact
              label="Operations"
              value={
                task === 'refresh'
                  ? `Read from ${setup.sourceLabel}`
                  : task === 'accounts' && !requiresInterruptionAcceptance
                    ? 'Keep the current catalog'
                    : 'Use the reviewed catalog'
              }
            />
            <ConfigurationFact
              label="Authentication"
              value={authenticationLabels[setup.configuration?.values.authentication ?? '']}
            />
            <Disclosure title="Configuration details">
              <ConfigurationPreview setup={setup} />
            </Disclosure>
            <ConfigurationFact
              label="Account setting"
              value={
                settingChanged
                  ? `${accountLabels[currentAccount]} → ${accountLabels[route]}`
                  : `${accountLabels[route]} · unchanged`
              }
            />
            <Typography.Body as="p">
              {grant
                ? 'New shared account: Authorized.'
                : settingChanged
                  ? 'Connect new accounts after you apply the changes.'
                  : requiresInterruptionAcceptance
                    ? 'Each account must reconnect after you apply the changes.'
                    : 'Keep existing accounts.'}
            </Typography.Body>
            {setup.authorized && !grant && (
              <Typography.BodySmall as="p">
                Coral discards the temporary sign-in after applying these changes.
              </Typography.BodySmall>
            )}
            {setup.configuration?.diagnostic && (
              <Banner variant="warning">{setup.configuration.diagnostic}</Banner>
            )}
            {setup.configuration?.interruptsAccess === null && (
              <Typography.BodySmall as="p">
                Coral cannot check account access for this configuration. Review the diagnostics
                before you apply changes.
              </Typography.BodySmall>
            )}
            {needsConsent && (
              <>
                <Banner variant="warning">
                  {settingChanged
                    ? 'This change removes existing account credentials. Switching back requires new connections.'
                    : route === 'caller'
                      ? audience.reconnect
                      : 'This change replaces the shared account authorization. Pending calls cannot switch accounts.'}
                </Banner>
                <Inputs.Checkbox
                  checked={frozen ? attempt.allowIncompatibleConfiguration : accepted}
                  disabled={frozen}
                  onCheckedChange={setAccepted}
                >
                  {confirmation}
                </Inputs.Checkbox>
              </>
            )}
            <Typography.Body as="p">The namespace and policy stay the same.</Typography.Body>
            <Typography.Body as="p">
              Your current configuration stays active until all changes succeed.
            </Typography.Body>
          </PublicationForm>
        ) : undefined,
    },
  ]
  return (
    <ManagementLayout
      breadcrumbs={trail(
        [
          { label: 'Providers', to: providersPath },
          { label: displayName, to: cancelPath },
        ],
        title,
      )}
    >
      {setup && !terminal && !frozen && (
        <Form method="post">
          <Fields form={form} intent="cancel" />
          <input type="hidden" name="setup" value={setup.id} />
          <Button.TextButton type="submit" variant="secondary" disabled={pending}>
            Cancel changes
          </Button.TextButton>
        </Form>
      )}
      {!setup && applicationProblem && (
        <Banner variant="error">
          The service rejected the OAuth application. Repair its configuration before you reconnect
          accounts.
        </Banner>
      )}
      {!setup && application?.secretConfigured && (
        <Disclosure title="OAuth application secret">
          {application.problem && (
            <Banner variant="error">
              The service rejected the application credentials. Replace the client secret to restore
              access.
            </Banner>
          )}
          <Form method="post" reloadDocument className={styles.stack}>
            <Fields form={form} intent="rotate-client-secret" />
            <input type="hidden" name="clientId" value={application.clientId} />
            <Typography.BodySmall as="p">Client ID: {application.clientId}</Typography.BodySmall>
            <Typography.BodySmallStrong as="label" htmlFor="replacement-client-secret">
              Replacement client secret
            </Typography.BodySmallStrong>
            <Inputs.CredentialInput id="replacement-client-secret" name="clientSecret" />
            <Button.TextButton type="submit">Replace secret</Button.TextButton>
          </Form>
        </Disclosure>
      )}
      {terminal ? (
        <Banner
          variant="warning"
          action={
            <Button.InternalLink to={basePath} size="small">
              Start over
            </Button.InternalLink>
          }
        >
          {terminal}
        </Banner>
      ) : !setup ? (
        <UpdateSource
          source={source}
          replacingSource={replacingSource}
          basePath={basePath}
          attempt={attempt}
          form={form}
          pending={pending}
          result={result}
        />
      ) : (
        <Stepper label={`${taskTitles[task]} · ${displayName}`} steps={steps} />
      )}
    </ManagementLayout>
  )
}

const SECRET_ROTATED = ['secretRotated'] as const
