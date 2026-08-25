import classNames from 'classnames'
import { type FormEvent, type RefObject, useEffect, useId, useState } from 'react'
import { Form, useNavigation } from 'react-router'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { SpinningButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Dialog, Radio } from '@/wax/components'
import { TextArea, TextInput } from '@/wax/components/inputs/text'
import { Pill } from '@/wax/components/pill'
import { Typography } from '@/wax/components/typography'

import { OAuthProgressDialog } from '@/components/sources/install/oauth-progress-dialog'
import { oauthActionLabel, useOAuthInstallFlow } from '@/lib/source-oauth-install-flow'
import type { SourcesActionData } from '@/routes/sources-action'
import type {
  SourceDetectedAuth,
  SourceDiscoveryData,
  SourceDocumentFormat,
} from '@/routes/source-discovery'

import type { DiscardGuard } from './source-add'
import * as styles from './source-create.css'
import { formatFieldName, SourceError, SourceField, SourceHeader } from './source-presentation'

const SECRET_KEY = 'API_TOKEN'
const NAME_PATTERN = /^[a-z][a-z0-9_]*$/
const RESERVED_SOURCE_NAMES = new Set(['coral', 'coral_admin', 'public'])

type SurfaceType = 'openapi' | 'mcp'
type AuthChoice = 'none' | 'bearer' | 'header' | 'oauthDevice'

interface Draft {
  name: string
  description: string
  surfaceType: SurfaceType
  url: string
  baseUrl: string
  auth: AuthChoice
  headerName: string
  oauthClientId: string
  oauthDeviceAuthorizationUrl: string
  oauthScopes: string
  oauthTokenUrl: string
  token: string
}

const EMPTY_DRAFT: Draft = {
  name: '',
  description: '',
  surfaceType: 'openapi',
  url: '',
  baseUrl: '',
  auth: 'bearer',
  headerName: '',
  oauthClientId: '',
  oauthDeviceAuthorizationUrl: '',
  oauthScopes: '',
  oauthTokenUrl: '',
  token: '',
}

const STEP_COUNT = 3

type DiscoveredSource = Extract<SourceDiscoveryData, { status: 'success' }>

/**
 * The steps that follow a discovered URL. It is mounted per discovery, so its
 * draft starts from what Coral detected and a second URL never inherits the
 * first one's answers.
 */
export function SourceCreateFlow({
  actionData,
  discardRef,
  discovery,
  fetchOAuthImport,
  oauthImportPath,
  onBack,
  onCancel,
  onOAuthImportComplete,
  openAuthorization,
  requestCancel,
  url,
}: {
  actionData: SourcesActionData
  discardRef: RefObject<DiscardGuard | null>
  discovery: DiscoveredSource
  fetchOAuthImport: typeof fetch
  oauthImportPath: string
  onBack: () => void
  onCancel: () => void
  onOAuthImportComplete?: (name: string, signal: AbortSignal) => Promise<void> | void
  openAuthorization: (url: string) => unknown
  requestCancel: () => void
  url: string
}) {
  const [step, setStep] = useState(0)
  const [draft, setDraft] = useState<Draft>(() => draftFromDiscovery(discovery, url))
  const formId = useId()
  const oauth = useOAuthInstallFlow({
    fetchOAuthInstall: fetchOAuthImport,
    openAuthorization,
    onComplete: async (name, signal) => {
      if (onOAuthImportComplete) await onOAuthImportComplete(name, signal)
      else onCancel()
    },
  })

  const navigation = useNavigation()
  const navigationSubmitting =
    navigation.state !== 'idle' && navigation.formData?.get('_intent') === 'import'
  const submitting = navigationSubmitting || oauth.busy
  const importError =
    actionData?.status === 'error' && actionData.intent === 'import' ? actionData.message : null

  // Past discovery there is always a draft to lose, so this branch answers for
  // what closing the dialog costs for as long as it is mounted.
  useEffect(() => {
    discardRef.current = { discard: oauth.cancel, isDirty: () => draftIsDirty(draft) }
    return () => {
      discardRef.current = null
    }
  })

  const update = (patch: Partial<Draft>) => setDraft((prev) => ({ ...prev, ...patch }))

  const stepValid = (() => {
    if (step === 0) {
      if (!sourceNameIsValid(draft.name.trim())) return false
      if (draft.surfaceType === 'openapi' && baseUrlValidationError(draft.baseUrl)) return false
      return true
    }
    if (draft.auth === 'header' && draft.headerName.trim().length === 0) return false
    if (draft.auth === 'oauthDevice') {
      return (
        isHttpsUrl(draft.oauthDeviceAuthorizationUrl) &&
        isHttpsUrl(draft.oauthTokenUrl) &&
        draft.oauthClientId.trim().length > 0
      )
    }
    return draft.auth === 'none' || draft.token.trim().length > 0
  })()

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    if (step === 1) {
      if (draft.auth !== 'oauthDevice') return
      event.preventDefault()
      if (!stepValid || submitting) return
      void oauth.start(oauthImportPath, new FormData(event.currentTarget))
      return
    }

    event.preventDefault()
    if (!stepValid || submitting) return
    setStep(1)
  }

  return (
    // The steps are portalled popups, so the form holds only the hidden fields
    // and the submit buttons reach it by id.
    <Form className={styles.stepForm} id={formId} method="post" onSubmit={handleSubmit}>
      <input type="hidden" name="_intent" value="import" />
      <input type="hidden" name="name" value={draft.name.trim()} />
      <input type="hidden" name="manifest_yaml" value={buildManifestYaml(draft)} />
      {draft.auth === 'bearer' || draft.auth === 'header' ? (
        <input type="hidden" name={`sec:${SECRET_KEY}`} value={draft.token.trim()} />
      ) : null}

      <Dialog.Root
        open
        onOpenChange={(open) => {
          if (!open) onBack()
        }}
      >
        <Dialog.Portal>
          <Dialog.Popup size="xl">
            <div className={styles.dialogContent}>
              <StepHeader step={1} />
              <DetailsStep discovery={discovery} draft={draft} update={update} />

              <Dialog.Actions>
                <ButtonContainer
                  disabled={navigationSubmitting}
                  onClick={requestCancel}
                  size="32"
                  variant="bare"
                >
                  <ButtonText>Cancel</ButtonText>
                </ButtonContainer>
                <ButtonContainer
                  disabled={submitting}
                  onClick={onBack}
                  size="32"
                  variant="secondary"
                >
                  <ButtonText>Back</ButtonText>
                </ButtonContainer>
                <ButtonContainer
                  disabled={!stepValid}
                  onClick={() => setStep(1)}
                  size="32"
                  variant="primary"
                >
                  <ButtonText>Next</ButtonText>
                </ButtonContainer>
              </Dialog.Actions>

              <Dialog.Root open={step >= 1}>
                <Dialog.Portal>
                  <Dialog.Popup size="xl">
                    <div className={styles.dialogContent}>
                      <StepHeader step={2} />
                      <CredentialsStep
                        discovery={discovery}
                        draft={draft}
                        update={update}
                        disabled={submitting}
                      />

                      {importError ? (
                        <SourceError className={styles.importError}>{importError}</SourceError>
                      ) : null}
                      <OAuthProgressDialog
                        error={oauth.error}
                        inputLabel={formatFieldName}
                        onCancel={oauth.cancel}
                        progress={oauth.progress}
                      />

                      <Dialog.Actions>
                        <ButtonContainer
                          disabled={navigationSubmitting}
                          onClick={requestCancel}
                          size="32"
                          variant="bare"
                        >
                          <ButtonText>Cancel</ButtonText>
                        </ButtonContainer>
                        <ButtonContainer
                          disabled={submitting}
                          onClick={() => setStep(0)}
                          size="32"
                          variant="secondary"
                        >
                          <ButtonText>Back</ButtonText>
                        </ButtonContainer>
                        <ButtonContainer
                          disabled={submitting || !stepValid}
                          form={formId}
                          size="32"
                          type="submit"
                          variant="primary"
                        >
                          {submitting ? <SpinningButtonIcon name="Loader" /> : null}
                          <ButtonText>
                            {draft.auth === 'oauthDevice'
                              ? oauthActionLabel(oauth.progress, {
                                  busy: 'Adding…',
                                  idle: 'Add source',
                                })
                              : navigationSubmitting
                                ? 'Adding…'
                                : 'Add source'}
                          </ButtonText>
                        </ButtonContainer>
                      </Dialog.Actions>
                    </div>
                  </Dialog.Popup>
                </Dialog.Portal>
              </Dialog.Root>
            </div>
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
    </Form>
  )
}

function StepHeader({ step }: { step: number }) {
  return (
    <SourceHeader
      className={styles.header}
      pill={
        <Pill as="span" color="graySubtle">
          Step {step + 1}/{STEP_COUNT}
        </Pill>
      }
      title={<Typography.HeadingMedium as="span">Add source</Typography.HeadingMedium>}
    />
  )
}

/** Seed the draft from what discovery detected, leaving the rest at its default. */
function draftFromDiscovery(discovery: DiscoveredSource, url: string): Draft {
  const detectedAuth = authChoiceFromDiscovery(discovery.auth)
  return {
    ...EMPTY_DRAFT,
    ...(detectedAuth ? { auth: detectedAuth } : {}),
    baseUrl: discovery.serverUrl || EMPTY_DRAFT.baseUrl,
    description: discovery.description || EMPTY_DRAFT.description,
    headerName: discovery.auth.headerName || EMPTY_DRAFT.headerName,
    name: discovery.name,
    surfaceType:
      discovery.format === 'mcp'
        ? 'mcp'
        : discovery.format === 'unknown'
          ? EMPTY_DRAFT.surfaceType
          : 'openapi',
    url,
  }
}

function DetailsStep({
  discovery,
  draft,
  update,
}: {
  discovery: DiscoveredSource
  draft: Draft
  update: (patch: Partial<Draft>) => void
}) {
  const idName = useId()
  const idDescription = useId()
  const idBaseUrl = useId()
  const [nameTouched, setNameTouched] = useState(false)
  const [baseUrlTouched, setBaseUrlTouched] = useState(false)
  const name = draft.name.trim()
  const nameError = nameTouched ? sourceNameValidationError(name) : null
  const mcp = draft.surfaceType === 'mcp'
  const baseUrlError = !mcp && baseUrlTouched ? baseUrlValidationError(draft.baseUrl) : null

  return (
    <div className={styles.fieldGroup}>
      <Typography.BodySmall variant="primary">{discoverySummary(discovery)}</Typography.BodySmall>
      <SourceField
        className={styles.fieldItem}
        hint={
          <Typography.BodySmall variant={nameError ? 'error' : 'tertiary'}>
            {nameError ?? 'Used as the schema name in queries.'}
          </Typography.BodySmall>
        }
        htmlFor={idName}
        label="Name"
      >
        <TextInput
          id={idName}
          value={draft.name}
          onBlur={() => setNameTouched(true)}
          onChange={(value) => update({ name: value })}
          placeholder="my_api"
        />
      </SourceField>
      <SourceField
        className={styles.fieldItem}
        htmlFor={idDescription}
        label="Description (optional)"
      >
        <TextArea
          id={idDescription}
          value={draft.description}
          onChange={(value) => update({ description: value })}
          placeholder="What this source connects to"
        />
      </SourceField>
      <SourceField
        className={styles.fieldItem}
        hint={
          <Typography.BodySmall variant={baseUrlError ? 'error' : 'tertiary'}>
            {mcp
              ? 'MCP servers are reached at the source URL, so they have no separate base URL.'
              : (baseUrlError ?? 'Requests are sent to this URL.')}
          </Typography.BodySmall>
        }
        htmlFor={idBaseUrl}
        label="Base URL"
      >
        <TextInput
          disabled={mcp}
          id={idBaseUrl}
          value={draft.baseUrl}
          onBlur={() => setBaseUrlTouched(true)}
          onChange={(value) => update({ baseUrl: value })}
          placeholder="https://api.example.com/v1"
        />
      </SourceField>
      <SourceField className={styles.fieldItem} label="Type">
        <Radio.Group
          aria-label="Source type"
          value={draft.surfaceType}
          onValueChange={(surfaceType) =>
            update({
              surfaceType,
              // Custom-header auth only applies to OpenAPI surfaces.
              auth: surfaceType === 'mcp' && draft.auth === 'header' ? 'bearer' : draft.auth,
            })
          }
        >
          <Radio.Item value="openapi">REST API (OpenAPI)</Radio.Item>
          <Radio.Item value="mcp">MCP server</Radio.Item>
        </Radio.Group>
      </SourceField>
    </div>
  )
}

function discoverySummary(discovery: {
  description: string
  format: SourceDocumentFormat
  inspectionError?: string
  serverUrl: string
  title: string
}): string {
  if (discovery.format === 'mcp') {
    return 'Detected an MCP endpoint from its URL. Review the details below.'
  }
  if (discovery.format === 'unknown') {
    const reason = discovery.inspectionError
      ? `The source document could not be inspected. ${asSentence(discovery.inspectionError)}`
      : 'No OpenAPI document was detected.'
    return `${reason} Fill in the details below.`
  }
  const fields = [
    ...(discovery.title ? ['name'] : []),
    ...(discovery.description ? ['description'] : []),
    ...(discovery.serverUrl ? ['base URL'] : []),
    'type',
  ]
  return `Detected the ${sentenceList(fields)} from the URL provided. Review the details below.`
}

function sentenceList(items: string[]): string {
  if (items.length < 3) return items.join(' and ')
  return `${items.slice(0, -1).join(', ')}, and ${items.at(-1)}`
}

function asSentence(value: string): string {
  return /[.!?]$/.test(value) ? value : `${value}.`
}

function CredentialsStep({
  discovery,
  draft,
  update,
  disabled,
}: {
  discovery: DiscoveredSource
  draft: Draft
  update: (patch: Partial<Draft>) => void
  disabled: boolean
}) {
  const idBearerToken = useId()
  const idHeaderName = useId()
  const idHeaderToken = useId()
  const idOAuthClientId = useId()
  const idOAuthDeviceAuthorizationUrl = useId()
  const idOAuthScopes = useId()
  const idOAuthTokenUrl = useId()
  const authChoices: { key: AuthChoice; label: string }[] =
    draft.surfaceType === 'openapi'
      ? [
          { key: 'none', label: 'None' },
          { key: 'bearer', label: 'Bearer token' },
          { key: 'oauthDevice', label: 'OAuth device flow' },
          { key: 'header', label: 'Custom header' },
        ]
      : [
          { key: 'none', label: 'None' },
          { key: 'bearer', label: 'Bearer token' },
          { key: 'oauthDevice', label: 'OAuth device flow' },
        ]
  return (
    <div className={styles.fieldGroup}>
      {discovery.auth.kind !== 'unknown' ? (
        <Typography.BodySmall variant="primary">{authSummary(discovery.auth)}</Typography.BodySmall>
      ) : null}
      <SourceField className={styles.fieldItem} label="Authentication">
        <Radio.Group
          aria-label="Authentication"
          value={draft.auth}
          onValueChange={(auth) => update({ auth })}
        >
          {authChoices.map((choice) => (
            <Radio.Item key={choice.key} value={choice.key}>
              {choice.label}
            </Radio.Item>
          ))}
        </Radio.Group>
      </SourceField>
      <div className={styles.authPanelStack}>
        <div
          aria-hidden={draft.auth !== 'none'}
          className={classNames(styles.authPanel, draft.auth !== 'none' && styles.authPanelHidden)}
        >
          <Typography.BodySmall variant="tertiary">
            This endpoint doesn’t require credentials.
          </Typography.BodySmall>
        </div>
        <div
          aria-hidden={draft.auth !== 'bearer'}
          className={classNames(
            styles.authPanel,
            draft.auth !== 'bearer' && styles.authPanelHidden,
          )}
        >
          <SourceField className={styles.fieldItem} htmlFor={idBearerToken} label="Bearer token">
            <TextInput
              id={idBearerToken}
              type="password"
              value={draft.token}
              onChange={(value) => update({ token: value })}
              placeholder="Paste token"
              disabled={disabled || draft.auth !== 'bearer'}
            />
          </SourceField>
        </div>
        <div
          aria-hidden={draft.auth !== 'oauthDevice'}
          className={classNames(
            styles.oauthDevicePanel,
            draft.auth !== 'oauthDevice' && styles.authPanelHidden,
          )}
        >
          <SourceField
            className={styles.fieldItem}
            htmlFor={idOAuthDeviceAuthorizationUrl}
            label="Device authorization URL"
          >
            <TextInput
              disabled={disabled || draft.auth !== 'oauthDevice'}
              id={idOAuthDeviceAuthorizationUrl}
              onChange={(value) => update({ oauthDeviceAuthorizationUrl: value })}
              placeholder="https://provider.example/oauth/device/code"
              value={draft.oauthDeviceAuthorizationUrl}
            />
          </SourceField>
          <SourceField className={styles.fieldItem} htmlFor={idOAuthTokenUrl} label="Token URL">
            <TextInput
              disabled={disabled || draft.auth !== 'oauthDevice'}
              id={idOAuthTokenUrl}
              onChange={(value) => update({ oauthTokenUrl: value })}
              placeholder="https://provider.example/oauth/token"
              value={draft.oauthTokenUrl}
            />
          </SourceField>
          <SourceField className={styles.fieldItem} htmlFor={idOAuthClientId} label="Client ID">
            <TextInput
              disabled={disabled || draft.auth !== 'oauthDevice'}
              id={idOAuthClientId}
              onChange={(value) => update({ oauthClientId: value })}
              placeholder="OAuth public client ID"
              value={draft.oauthClientId}
            />
          </SourceField>
          <SourceField
            className={styles.fieldItem}
            hint={
              <Typography.BodySmall variant="tertiary">
                Optional, separated by spaces.
              </Typography.BodySmall>
            }
            htmlFor={idOAuthScopes}
            label="Scopes"
          >
            <TextInput
              disabled={disabled || draft.auth !== 'oauthDevice'}
              id={idOAuthScopes}
              onChange={(value) => update({ oauthScopes: value })}
              placeholder="read profile"
              value={draft.oauthScopes}
            />
          </SourceField>
        </div>
        <div
          aria-hidden={draft.auth !== 'header'}
          className={classNames(
            styles.authPanel,
            draft.auth !== 'header' && styles.authPanelHidden,
          )}
        >
          <SourceField className={styles.fieldItem} htmlFor={idHeaderName} label="Header name">
            <TextInput
              id={idHeaderName}
              value={draft.headerName}
              onChange={(value) => update({ headerName: value })}
              placeholder="X-Api-Key"
              disabled={disabled || draft.auth !== 'header'}
            />
          </SourceField>
          <SourceField
            className={styles.fieldItem}
            htmlFor={idHeaderToken}
            label={`${draft.headerName.trim() || 'Header'} value`}
          >
            <TextInput
              id={idHeaderToken}
              type="password"
              value={draft.token}
              onChange={(value) => update({ token: value })}
              placeholder="Paste token"
              disabled={disabled || draft.auth !== 'header'}
            />
          </SourceField>
        </div>
      </div>
    </div>
  )
}

function authSummary(auth: SourceDetectedAuth): string {
  if (auth.kind === 'unsupported') {
    return `Detected ${auth.label}, which isn’t supported. Choose another method below.`
  }
  return `Detected ${auth.label}.`
}

function authChoiceFromDiscovery(auth: SourceDetectedAuth): AuthChoice | null {
  if (auth.kind === 'bearer' || auth.kind === 'header' || auth.kind === 'none') return auth.kind
  return null
}

function draftIsDirty(draft: Draft): boolean {
  return Object.keys(EMPTY_DRAFT).some((key) => {
    const draftKey = key as keyof Draft
    return draft[draftKey] !== EMPTY_DRAFT[draftKey]
  })
}

function sourceNameIsValid(name: string): boolean {
  return sourceNameValidationError(name) === null
}

function isHttpsUrl(value: string): boolean {
  try {
    return new URL(value.trim()).protocol === 'https:'
  } catch {
    return false
  }
}

/** Base URLs also allow http:// so sources can point at a local API. */
function baseUrlValidationError(value: string): string | null {
  const baseUrl = value.trim()
  if (!baseUrl) return 'Enter the base URL requests are sent to.'
  let protocol: string
  try {
    protocol = new URL(baseUrl).protocol
  } catch {
    return 'Enter a valid URL, including the scheme.'
  }
  if (protocol !== 'https:' && protocol !== 'http:') return 'Use an http:// or https:// URL.'
  return null
}

function sourceNameValidationError(name: string): string | null {
  if (!name) return 'Enter a source name.'
  if (RESERVED_SOURCE_NAMES.has(name)) {
    return `“${name}” is reserved by Coral. Choose another source name.`
  }
  if (!NAME_PATTERN.test(name)) {
    return 'Use lowercase letters, digits, and underscores; the name must start with a letter.'
  }
  return null
}

/** Quote a scalar as a JSON string, which YAML parses as a flow scalar. */
const s = (value: string) => JSON.stringify(value)

/** Build a DSL v4 source manifest from the wizard fields. */
function buildManifestYaml(draft: Draft): string {
  const name = draft.name.trim()
  const url = draft.url.trim()
  const lines: string[] = [`name: ${s(name)}`, 'dsl_version: 4']
  if (draft.description.trim()) lines.push(`description: ${s(draft.description.trim())}`)
  if (draft.auth !== 'none') {
    lines.push(
      'inputs:',
      `  ${SECRET_KEY}:`,
      '    kind: secret',
      `    hint: ${s(`API token for ${name}`)}`,
    )
    if (draft.auth === 'oauthDevice') {
      lines.push(
        '    credential:',
        '      methods:',
        '        - type: oauth',
        '          label: Connect with OAuth device flow',
        '          description: Sign in with a device code.',
        '          oauth:',
        '            flow:',
        '              type: device_code',
        '            endpoints:',
        `              device_authorization_url: ${s(draft.oauthDeviceAuthorizationUrl.trim())}`,
        `              token_url: ${s(draft.oauthTokenUrl.trim())}`,
        '            client:',
        '              id:',
        `                default: ${s(draft.oauthClientId.trim())}`,
      )
      const scopes = oauthScopes(draft.oauthScopes)
      if (scopes.length > 0) {
        lines.push(
          '            scopes:',
          '              scope:',
          '                delimiter: space',
          '                values:',
          ...scopes.map((scope) => `                  - ${s(scope)}`),
        )
      }
      lines.push(
        '        - type: source_config',
        '          label: Paste token',
        '          description: Paste an existing access token.',
      )
    }
  }
  lines.push('surface:')

  if (draft.surfaceType === 'openapi') {
    lines.push('  type: openapi', `  url: ${s(url)}`)
    // Omitted base_url leaves coral-app to derive it from the document's servers block.
    if (draft.baseUrl.trim()) lines.push(`  base_url: ${s(draft.baseUrl.trim())}`)
    if (draft.auth !== 'none') {
      const bearerAuth = draft.auth === 'bearer' || draft.auth === 'oauthDevice'
      const headerName = bearerAuth ? 'Authorization' : draft.headerName.trim()
      const template = bearerAuth ? `Bearer {{input.${SECRET_KEY}}}` : `{{input.${SECRET_KEY}}}`
      lines.push(
        '  auth:',
        '    type: HeaderAuth',
        '    headers:',
        `      - name: ${s(headerName)}`,
        '        from: template',
        `        template: ${s(template)}`,
      )
    }
  } else {
    lines.push('  type: mcp', '  server:', '    transport: streamable_http', `    url: ${s(url)}`)
    if (draft.auth !== 'none') {
      lines.push('    auth:', '      type: bearer', '      from: input', `      key: ${SECRET_KEY}`)
    }
  }
  return lines.join('\n') + '\n'
}

function oauthScopes(value: string): string[] {
  return [
    ...new Set(
      value
        .split(/[\s,]+/)
        .map((scope) => scope.trim())
        .filter(Boolean),
    ),
  ]
}
