import { useId, useState, type ReactNode } from 'react'
import { Button, Inputs, Radio, Typography } from '@/wax/components'
import type { ConfigurationReviewView, SetupView } from './types'
import * as styles from './providers.css'

type ApplicationMode = 'automatic' | 'explicit'

/**
 * One application editor and its confirmed sign-in fields, shared by Add and Update.
 * Owns conditional authentication and advanced settings; callers supply the write-only
 * input inside their document form, never a secret value or secret-bearing draft.
 */
export function OAuthApplicationBlock({
  setup,
  device = setup.credentialMethod === 'device',
  application: configured = setup.configuration?.application,
  callbackUrl,
  configuring = false,
  clientSecretInput,
}: {
  setup: SetupView
  device?: boolean
  application?: ConfigurationReviewView['application']
  callbackUrl: string
  configuring?: boolean
  clientSecretInput?: ReactNode
}) {
  const automaticAllowed = !device && setup.configuration?.automaticClientAvailable === true
  // The OAuth host holds the registration; it can differ from the API host.
  const registrar = oauthHost(setup.configuration?.values.tokenUrl) || 'the service'
  const used = setup.authorization
  const [client, setClient] = useState(
    configured
      ? configured.mode === 'manual'
        ? configured.clientId
        : ''
      : used?.bootstrap === 'manual'
        ? used.clientId
        : '',
  )
  const mode: ApplicationMode = automaticAllowed && !client.trim() ? 'automatic' : 'explicit'
  const [authentication, setAuthentication] = useState(configured?.authentication ?? 'none')
  const clientId = useId()
  const callbackId = useId()
  const parameterId = useId()
  const confidential = mode === 'explicit' && authentication !== 'none'
  const parameters = configured?.parameters ?? ''
  // Once the application is settled, it is a fact about the sign-in rather than
  // a section of the page: one muted line, and the id in the shape of an id.
  const settled = configured
    ? configured.mode === 'manual'
      ? configured.clientId
      : ''
    : used?.bootstrap === 'manual'
      ? used.clientId
      : ''
  const note = (text: string) => (
    <Typography.BodySmall as="p" variant="tertiary">
      {text}
      {settled ? ' ' : ''}
      {settled ? (
        <Typography.CodeInline as="code" className={styles.longValue}>
          {settled}
        </Typography.CodeInline>
      ) : null}
    </Typography.BodySmall>
  )
  return (
    <div className={styles.stack}>
      {configuring && (
        <Typography.BodySmallStrong as="span">OAuth application</Typography.BodySmallStrong>
      )}
      {configuring ? (
        <>
          {/* Coral establishes the client at sign-in, by a client ID metadata
              document or dynamic registration as the server and the operator's
              policy allow, so there is no id to show until then. Supplying one
              is what makes the application yours. */}
          <input type="hidden" name="application" value={mode} />
          {!device && (
            <div className={styles.choices}>
              <Typography.BodySmallStrong as="label" htmlFor={callbackId}>
                Callback URL
              </Typography.BodySmallStrong>
              <div className={styles.rowActions}>
                {/* The input takes the row; Copy sits after it. */}
                <div className={styles.grow}>
                  <Inputs.TextInput id={callbackId} value={callbackUrl} readOnly />
                </div>
                <Button.CopyButton textToCopy={callbackUrl} ariaLabel="Copy callback URL">
                  Copy
                </Button.CopyButton>
              </div>
              <Typography.BodySmall as="p" variant="tertiary">
                Add it to the application's allowed redirect URLs.
              </Typography.BodySmall>
            </div>
          )}
          <div className={styles.field}>
            <Typography.BodySmallStrong as="label" htmlFor={clientId}>
              Client ID
            </Typography.BodySmallStrong>
            <Inputs.TextInput
              id={clientId}
              name="clientId"
              required={!automaticAllowed}
              maxLength={1024}
              value={client}
              onChange={setClient}
            />
            <Typography.BodySmall as="p" variant="tertiary">
              {automaticAllowed
                ? 'Leave empty to use automatic client setup.'
                : `From your application's registration with ${registrar}.`}
            </Typography.BodySmall>
          </div>
          {mode === 'explicit' && (
            <>
              <Typography.BodySmallStrong as="span">
                Client authentication
              </Typography.BodySmallStrong>
              <Radio.Group
                layout="stack"
                aria-label="Client authentication"
                value={authentication}
                onValueChange={(value) => setAuthentication(value as 'none' | 'basic' | 'post')}
              >
                <Radio.Item value="none">Public client</Radio.Item>
                <Radio.Item value="basic">Secret in Basic header</Radio.Item>
                <Radio.Item value="post">Secret in request body</Radio.Item>
              </Radio.Group>
              <input type="hidden" name="clientAuthentication" value={authentication} />
              {confidential && (
                <div className={styles.field}>
                  {clientSecretInput}
                  <Typography.BodySmall as="p" variant="tertiary">
                    {setup.clientSecretConfigured
                      ? 'Leave this field empty to keep the saved secret for the same client ID.'
                      : `Enter the application secret from your registration with ${registrar}.`}
                  </Typography.BodySmall>
                </div>
              )}
            </>
          )}
          {/* S256 is the only option unless an OpenAPI document's client is
              confidential, so the choice appears only where there is one. */}
          {!device && setup.kind === 'openapi' && confidential && (
            <div className={styles.field}>
              <Typography.BodySmallStrong as="span">PKCE</Typography.BodySmallStrong>
              <Radio.Group
                layout="stack"
                aria-label="PKCE"
                name="pkce"
                defaultValue={configured?.pkce ?? 's256'}
              >
                <Radio.Item value="s256">Use S256 PKCE</Radio.Item>
                <Radio.Item value="disabled">Disable PKCE for this service</Radio.Item>
              </Radio.Group>
            </div>
          )}
          {!device && (
            <div className={styles.field}>
              <Typography.BodySmallStrong as="label" htmlFor={parameterId}>
                Additional authorization parameters
              </Typography.BodySmallStrong>
              <Inputs.TextInput
                id={parameterId}
                name="authorizationParameters"
                maxLength={8192}
                defaultValue={parameters}
                placeholder="access_type=offline"
              />
              <Typography.BodySmall as="p" variant="tertiary">
                Enter query parameters, such as access_type=offline. Do not enter secrets.
              </Typography.BodySmall>
            </div>
          )}
        </>
      ) : configured ? (
        <>
          <input
            type="hidden"
            name="application"
            value={configured.mode === 'manual' ? 'explicit' : 'automatic'}
          />
          <input type="hidden" name="clientId" value={configured.clientId} />
          <input
            type="hidden"
            name="clientAuthentication"
            value={configured.authentication ?? 'none'}
          />
          <input type="hidden" name="pkce" value={configured.pkce ?? 's256'} />
          <input type="hidden" name="authorizationParameters" value={parameters} />
          {configured.mode === 'manual' &&
            note('Signs in through your own OAuth application, client ID')}
        </>
      ) : used ? (
        used.bootstrap === 'manual' &&
        note('Signs in through your own OAuth application, client ID')
      ) : (
        <Typography.BodySmall as="p" variant="tertiary">
          Choose the OAuth application in configuration before you sign in.
        </Typography.BodySmall>
      )}
    </div>
  )
}

function oauthHost(tokenUrl = '') {
  try {
    return new URL(tokenUrl).host
  } catch {
    return ''
  }
}
