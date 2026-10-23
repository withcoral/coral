import { useId, useState, type ReactNode } from 'react'
import { automaticApplicationAllowed } from '@/lib/provider-publication-attempt/model'
import { Button, Inputs, Radio, Typography } from '@/wax/components'
import { Markdown } from '@/wax/components/markdown'
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
  clientConfigHintMarkdown,
  configuring = false,
  clientSecretInput,
}: {
  setup: SetupView
  device?: boolean
  application?: ConfigurationReviewView['application']
  callbackUrl: string
  /** Extra help below the client ID field, such as where to register the application. */
  clientConfigHintMarkdown?: string
  configuring?: boolean
  clientSecretInput?: ReactNode
}) {
  const automaticAllowed = automaticApplicationAllowed(setup, device)
  // The OAuth host holds the registration; it can differ from the API host.
  const registrar = hostOf(setup.configuration?.values.tokenUrl) || 'the service'
  // A device sign-in happens on the verification page the service returns when
  // it starts, not at the device authorization endpoint, so it is not named.
  const signInHost = device ? '' : hostOf(setup.configuration?.values.authorizationUrl)
  const tokenHost = hostOf(setup.configuration?.values.tokenUrl)
  const callHosts = credentialHosts(setup)
  // An agent's addProvider can choose these endpoints and the servers, so
  // you see them before you sign in.
  const destination = [
    signInHost && `You sign in at ${signInHost}.`,
    tokenHost && `Coral gets the token from ${tokenHost}.`,
    callHosts.length > 0 && `Coral sends the token to ${callHosts.join(', ')}.`,
  ]
    .filter(Boolean)
    .join(' ')
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
  const settled = configured?.mode === 'manual' ? configured.clientId : ''
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
            {clientConfigHintMarkdown && (
              <Markdown color="tertiary">{clientConfigHintMarkdown}</Markdown>
            )}
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
          {destination && (
            <Typography.BodySmall as="p" variant="tertiary">
              {destination}
            </Typography.BodySmall>
          )}
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
          {/* An agent's addProvider names a confidential application but
              never its secret, so you enter it here before you sign in. The
              sign-in form supplies the field, so it stays a document form. */}
          {configured.authentication &&
            configured.authentication !== 'none' &&
            !setup.clientSecretConfigured &&
            clientSecretInput && (
              <div className={styles.field}>
                {clientSecretInput}
                <Typography.BodySmall as="p" variant="tertiary">
                  Enter the secret of this OAuth application. Coral sends it only to {registrar}.
                </Typography.BodySmall>
              </div>
            )}
        </>
      ) : automaticAllowed ? (
        <>
          {destination && (
            <Typography.BodySmall as="p" variant="tertiary">
              {destination}
            </Typography.BodySmall>
          )}
          {/* An agent's addProvider confirms the configuration without an
              application. Coral chooses one at sign-in: the one it registered
              for this setup, or a new one. It never reuses a client ID you
              entered, so no client ID is named here. */}
          <input type="hidden" name="application" value="automatic" />
        </>
      ) : (
        <Typography.BodySmall as="p" variant="tertiary">
          Enter the client ID of your OAuth application in configuration before you sign in.
        </Typography.BodySmall>
      )}
    </div>
  )
}

/** The host of a URL, or empty when there is none to name. */
export function hostOf(url = '') {
  try {
    return new URL(url).host
  } catch {
    return ''
  }
}

/** Where Coral sends a credential: each host the operations call, or the MCP server. */
export function credentialHosts(setup: SetupView) {
  return [
    ...new Set(
      setup.kind === 'mcp'
        ? [setup.sourceHost]
        : Object.values(setup.configuration?.destinations ?? {}).map((url) => hostOf(url)),
    ),
  ].filter(Boolean)
}
