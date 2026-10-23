import { useId } from 'react'
import { Form } from 'react-router'
import { signInFormTarget } from '@/lib/device-sign-in'
import { Banner, Button, Inputs, Typography } from '@/wax/components'
import { CredentialFields } from '@/views/management/credential-fields'
import { Fields } from '@/views/management/forms'
import { ManagementLayout, trail } from '@/views/management/layout'
import type { AuthorizationView } from './types'
import { DeviceCodeDialog } from './device-code'
import { oauthFailureMessage } from './oauth-failure'
import { DocumentSubmitButton } from '@/components/document-submit-button'
import * as styles from './accounts.css'

export function AuthorizationPage({
  view: v,
  pending = false,
}: {
  view: AuthorizationView
  pending?: boolean
}) {
  const open = v.state === 'pending' || v.state === 'exchanging'
  const formId = useId()
  // An agent's connect dialog returns to the agent's page, where you can start again.
  const returnsToAgent =
    v.scope === 'agent' && /^\/workspaces\/[^/?]+\/agents\/[^/?]+$/.test(v.returnTo)
  const returnsToRun = !returnsToAgent && !/^\/workspaces\/[^/?]+\/activity$/.test(v.returnTo)
  const startAgain = returnsToAgent
    ? 'Return to the agent to start another connection.'
    : 'Return to the account settings to start another connection.'
  return (
    <ManagementLayout breadcrumbs={trail(v.parents, 'Connect account')}>
      {v.scope === 'agent' && (
        <Typography.BodySmall as="p">
          Connect this account for the agent. Your personal account stays unchanged.
        </Typography.BodySmall>
      )}
      {v.state === 'succeeded' ? (
        <Banner variant="success">Account connected.</Banner>
      ) : v.state === 'failed' ? (
        <Banner variant="error">
          {v.applicationFailure
            ? 'Ask the provider owner to repair the OAuth application.'
            : oauthFailureMessage(v.failure)}{' '}
          {!v.applicationFailure && !returnsToRun && startAgain}
        </Banner>
      ) : v.state === 'expired' || v.state === 'canceled' ? (
        <Banner variant="warning">
          {v.state === 'expired' ? 'This connection expired.' : 'This connection was canceled.'}{' '}
          {returnsToRun ? 'Return to your run to check its current state.' : startAgain}
        </Banner>
      ) : null}
      {v.retryPath && (v.state === 'failed' || v.state === 'expired' || v.state === 'canceled') && (
        <Button.InternalLink to={v.retryPath}>Try again</Button.InternalLink>
      )}
      {v.state === 'pending' &&
        (v.oauth ? (
          <Form
            id={formId}
            method="post"
            action={v.path}
            reloadDocument
            {...signInFormTarget(v.userCode !== undefined)}
          >
            <Fields form={v.form} intent="navigate" />
          </Form>
        ) : (
          <form id={formId} method="post" action={v.path} className={styles.stack}>
            <Fields form={v.form} intent="secret" />
            <Typography.BodySmall as="p">
              Enter the credentials from {v.providerName}.
            </Typography.BodySmall>
            <CredentialFields fields={v.credentialFields}>
              {(input) => <Inputs.CredentialInput {...input} />}
            </CredentialFields>
          </form>
        ))}
      {v.state === 'exchanging' && <Banner variant="info">Completing sign-in…</Banner>}
      {open && (
        // The main action names its form by ID, so it can sit beside Cancel.
        <div className={styles.actions}>
          {v.state === 'pending' &&
            (v.oauth && v.userCode !== undefined ? (
              <DeviceCodeDialog
                code={v.userCode}
                formId={formId}
                pending={pending}
                triggerLabel="Continue to sign in"
              />
            ) : (
              <DocumentSubmitButton form={formId}>
                {v.oauth ? 'Continue to sign in' : 'Connect account'}
              </DocumentSubmitButton>
            ))}
          <Form method="post" action={v.path}>
            <Fields form={v.form} intent="cancel" />
            <Button.TextButton type="submit" variant="secondary" disabled={pending}>
              Cancel
            </Button.TextButton>
          </Form>
        </div>
      )}
      {v.state === 'succeeded' && v.automaticRefresh === false && (
        <Banner variant="info">
          This account cannot refresh automatically. Reconnect when access expires.
        </Banner>
      )}
      {v.grantedScope && (
        <Typography.BodySmall as="p">Granted permissions: {v.grantedScope}</Typography.BodySmall>
      )}
      {v.state === 'succeeded' && (
        <Typography.BodySmall as="p">
          {returnsToRun
            ? 'Your run may still need approval. Return to your run to check its current state.'
            : 'Return to the account settings. Check the account before you make calls.'}
        </Typography.BodySmall>
      )}
      {!open && (
        <div className={styles.actions}>
          <Button.InternalLink to={v.returnTo} variant="secondary">
            {returnsToAgent ? 'Back to agent' : returnsToRun ? 'Return to run' : 'Back to activity'}
          </Button.InternalLink>
        </div>
      )}
    </ManagementLayout>
  )
}
