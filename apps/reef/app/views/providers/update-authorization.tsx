import { Form } from 'react-router'
import { isOAuthMethod } from '@/lib/provider-publication-attempt/model'
import { Fields } from '@/views/management/forms'
import { OAuthApplicationBlock } from './oauth-application'
import {
  SetupFields,
  SetupSecretForm,
  SignInProgress,
  SignInStatus,
  signInLabel,
} from './setup-steps'
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
  const replacement = (
    <>
      <input type="hidden" name="account" value="fixed" />
      {setup.authorized && (
        <input type="hidden" name="replacementGrant" value={setup.provisionalGrantId} />
      )}
    </>
  )
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
      ) : oauth ? (
        <Form id={formId} method="post" reloadDocument className={styles.stack}>
          <Fields form={form} intent="oauth-start" />
          <SetupFields setup={setup} extra={replacement} />
          <OAuthApplicationBlock setup={setup} callbackUrl={callbackUrl} />
          {!formId && (
            <DocumentSubmitButton disabled={pending}>{signInLabel(setup)}</DocumentSubmitButton>
          )}
        </Form>
      ) : (
        <SetupSecretForm
          setup={setup}
          form={form}
          pending={pending}
          id={formId}
          extra={replacement}
        >
          {!formId && (
            <DocumentSubmitButton disabled={pending}>Authorize replacement</DocumentSubmitButton>
          )}
        </SetupSecretForm>
      )}
    </div>
  )
}
