import { Form } from 'react-router'
import { isOAuthMethod } from '@/lib/provider-publication-attempt/model'
import { Inputs } from '@/wax/components'
import { Fields } from '@/views/management/forms'
import { OAuthApplicationBlock } from './oauth-application'
import { SignInProgress, SignInStatus, signInLabel } from './setup-steps'
import { CredentialFields } from '@/views/management/credential-fields'
import type { FormContext, SetupView } from './types'
import { DocumentSubmitButton } from '@/components/document-submit-button'
import * as styles from './providers.css'

/** Authorization stays provisional until the owner explicitly selects it for publication. */
export function UpdateAuthorization({
  setup,
  form,
  pending,
  callbackUrl,
  formId,
}: {
  setup: SetupView
  form: FormContext
  pending: boolean
  callbackUrl: string
  /** Names the sign-in form for a submit button outside it, which then replaces its own. */
  formId?: string
}) {
  const oauth = isOAuthMethod(setup.credentialMethod)
  const inFlight =
    setup.authorization?.state === 'pending' || setup.authorization?.state === 'exchanging'
  return (
    <div className={styles.stack}>
      <SignInStatus setup={setup} />
      {inFlight ? (
        <SignInProgress
          setup={setup}
          form={form}
          pending={pending}
          extra={<input type="hidden" name="account" value="fixed" />}
        />
      ) : (
        <Form id={formId} method="post" reloadDocument className={styles.stack}>
          <Fields form={form} intent={oauth ? 'oauth-start' : 'secret'} />
          <input type="hidden" name="setup" value={setup.id} />
          <input type="hidden" name="configurationRevision" value={setup.configuration?.revision} />
          <input type="hidden" name="account" value="fixed" />
          {setup.authorized && (
            <input type="hidden" name="replacementGrant" value={setup.provisionalGrantId} />
          )}
          {oauth ? (
            <OAuthApplicationBlock setup={setup} callbackUrl={callbackUrl} />
          ) : (
            <CredentialFields fields={setup.credentialFields}>
              {(input) => <Inputs.CredentialInput {...input} />}
            </CredentialFields>
          )}
          {!formId && (
            <DocumentSubmitButton
              busyStatus={oauth ? 'Opening sign-in…' : 'Authorizing…'}
              disabled={pending}
            >
              {oauth ? signInLabel(setup) : 'Authorize replacement'}
            </DocumentSubmitButton>
          )}
        </Form>
      )}
    </div>
  )
}
