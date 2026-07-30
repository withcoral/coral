import classNames from 'classnames'
import { type FormEvent, type RefObject, useEffect, useId, useRef, useState } from 'react'
import { Form, useFetcher, useNavigation } from 'react-router'

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

export function SourceCreateDialog({
  actionData,
  discoveryPath,
  fetchOAuthImport = fetch,
  oauthImportPath = discoveryPath.replace(/\/discover$/, '/oauth-import'),
  onOAuthImportComplete,
  open,
  openAuthorization = (url) => window.open(url, '_blank', 'noopener,noreferrer'),
  onOpenChange,
}: {
  actionData: SourcesActionData
  discoveryPath: string
  fetchOAuthImport?: typeof fetch
  oauthImportPath?: string
  onOAuthImportComplete?: (name: string, signal: AbortSignal) => Promise<void> | void
  open: boolean
  openAuthorization?: (url: string) => unknown
  onOpenChange: (open: boolean) => void
}) {
  const requestCancelRef = useRef<() => void>(() => onOpenChange(false))

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(nextOpen) => {
        if (!nextOpen) requestCancelRef.current()
      }}
    >
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="xl">
          {open ? (
            <SourceCreateDialogContent
              actionData={actionData}
              discoveryPath={discoveryPath}
              fetchOAuthImport={fetchOAuthImport}
              oauthImportPath={oauthImportPath}
              onOAuthImportComplete={onOAuthImportComplete}
              onCancel={() => onOpenChange(false)}
              openAuthorization={openAuthorization}
              requestCancelRef={requestCancelRef}
            />
          ) : null}
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function SourceCreateDialogContent({
  actionData,
  discoveryPath,
  fetchOAuthImport,
  oauthImportPath,
  onOAuthImportComplete,
  onCancel,
  openAuthorization,
  requestCancelRef,
}: {
  actionData: SourcesActionData
  discoveryPath: string
  fetchOAuthImport: typeof fetch
  oauthImportPath: string
  onOAuthImportComplete?: (name: string, signal: AbortSignal) => Promise<void> | void
  onCancel: () => void
  openAuthorization: (url: string) => unknown
  requestCancelRef: RefObject<() => void>
}) {
  const [step, setStep] = useState(0)
  const [draft, setDraft] = useState<Draft>(EMPTY_DRAFT)
  const [confirmingCancel, setConfirmingCancel] = useState(false)
  const formId = useId()
  const discovery = useFetcher<SourceDiscoveryData>()
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
  const discovering = discovery.state !== 'idle'
  const discoveryError =
    discovery.data?.status === 'error' && discovery.data.url === draft.url.trim()
      ? discovery.data.message
      : null
  const discoveryResult =
    discovery.data?.status === 'success' && discovery.data.url === draft.url.trim()
      ? discovery.data
      : null

  const appliedDiscovery = useRef<SourceDiscoveryData | undefined>(undefined)
  useEffect(() => {
    const result = discovery.data
    if (!result || result === appliedDiscovery.current || result.status !== 'success') return
    appliedDiscovery.current = result
    if (step !== 0 || result.url !== draft.url.trim()) return
    const detectedAuth = authChoiceFromDiscovery(result.auth)
    setDraft((current) => ({
      ...current,
      ...(detectedAuth ? { auth: detectedAuth } : {}),
      baseUrl: result.serverUrl || current.baseUrl,
      description: result.description || current.description,
      headerName: result.auth.headerName || current.headerName,
      name: result.name,
      surfaceType:
        result.format === 'mcp'
          ? 'mcp'
          : result.format === 'unknown'
            ? current.surfaceType
            : 'openapi',
    }))
    setStep(1)
  }, [discovery.data, draft.url, step])

  const update = (patch: Partial<Draft>) => setDraft((prev) => ({ ...prev, ...patch }))
  const updateUrl = (url: string) =>
    setDraft((current) =>
      url.trim() === current.url.trim() ? { ...current, url } : { ...EMPTY_DRAFT, url },
    )
  const inspectUrl = () =>
    discovery.load(`${discoveryPath}?url=${encodeURIComponent(draft.url.trim())}`)
  const requestCancel = () => {
    if (draftIsDirty(draft)) {
      setConfirmingCancel(true)
      return
    }
    onCancel()
  }
  useEffect(() => {
    requestCancelRef.current = requestCancel
  })

  const stepValid = (() => {
    if (step === 0) return draft.url.trim().startsWith('https://')
    if (step === 1) {
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
    if (step === 2) {
      if (draft.auth !== 'oauthDevice') return
      event.preventDefault()
      if (!stepValid || submitting) return
      void oauth.start(oauthImportPath, new FormData(event.currentTarget))
      return
    }

    event.preventDefault()
    if (!stepValid || discovering || submitting) return
    if (step === 0) {
      inspectUrl()
      return
    }
    setStep(2)
  }

  return (
    <Form className={styles.dialogContent} id={formId} method="post" onSubmit={handleSubmit}>
      <input type="hidden" name="_intent" value="import" />
      <input type="hidden" name="name" value={draft.name.trim()} />
      <input type="hidden" name="manifest_yaml" value={buildManifestYaml(draft)} />
      {draft.auth === 'bearer' || draft.auth === 'header' ? (
        <>
          <input type="hidden" name="secret_key" value={SECRET_KEY} />
          <input type="hidden" name="secret_value" value={draft.token.trim()} />
        </>
      ) : null}
      {draft.auth === 'oauthDevice' ? (
        <>
          <input type="hidden" name="oauth_input_key" value={SECRET_KEY} />
          <input type="hidden" name="oauth_method_index" value="0" />
        </>
      ) : null}

      <StepHeader step={0} />
      <UrlStep draft={draft} updateUrl={updateUrl} />

      {discoveryError ? <SourceError>{discoveryError}</SourceError> : null}

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
          disabled={!stepValid || discovering}
          onClick={inspectUrl}
          size="32"
          variant="primary"
        >
          {discovering ? <SpinningButtonIcon name="Loader" /> : null}
          <ButtonText>{discovering ? 'Inspecting…' : 'Next'}</ButtonText>
        </ButtonContainer>
      </Dialog.Actions>

      <Dialog.Root
        open={step >= 1}
        onOpenChange={(open) => {
          if (!open) setStep(0)
        }}
      >
        <Dialog.Portal>
          <Dialog.Popup size="xl">
            <div className={styles.dialogContent}>
              <StepHeader step={1} />
              <DetailsStep discovery={discoveryResult} draft={draft} update={update} />

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
                  disabled={!stepValid}
                  onClick={() => setStep(2)}
                  size="32"
                  variant="primary"
                >
                  <ButtonText>Next</ButtonText>
                </ButtonContainer>
              </Dialog.Actions>

              <Dialog.Root open={step >= 2}>
                <Dialog.Portal>
                  <Dialog.Popup size="xl">
                    <div className={styles.dialogContent}>
                      <StepHeader step={2} />
                      <CredentialsStep
                        discovery={discoveryResult}
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
                          onClick={() => setStep(1)}
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
                                  busy: 'Creating…',
                                  idle: 'Create source',
                                })
                              : navigationSubmitting
                                ? 'Creating…'
                                : 'Create source'}
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
      <Dialog.Root open={confirmingCancel} onOpenChange={setConfirmingCancel}>
        <Dialog.Portal>
          <Dialog.Popup size="m">
            <Dialog.Title>Discard source draft?</Dialog.Title>
            <Dialog.Description>The information you entered will be lost.</Dialog.Description>
            <Dialog.Actions>
              <ButtonContainer
                onClick={() => setConfirmingCancel(false)}
                size="32"
                variant="secondary"
              >
                <ButtonText>Keep editing</ButtonText>
              </ButtonContainer>
              <ButtonContainer
                onClick={() => {
                  oauth.cancel()
                  onCancel()
                }}
                size="32"
                variant="destructive"
              >
                <ButtonText>Discard</ButtonText>
              </ButtonContainer>
            </Dialog.Actions>
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
      title={<Typography.HeadingMedium as="span">Create source</Typography.HeadingMedium>}
    />
  )
}

function UrlStep({ draft, updateUrl }: { draft: Draft; updateUrl: (url: string) => void }) {
  const idUrl = useId()
  return (
    <div className={styles.fieldGroup}>
      <SourceField
        className={styles.fieldItem}
        hint={
          <Typography.BodySmall variant="tertiary">
            Enter an OpenAPI document or streamable HTTP MCP endpoint.
          </Typography.BodySmall>
        }
        htmlFor={idUrl}
        label="Source URL"
      >
        <TextInput
          id={idUrl}
          value={draft.url}
          onChange={updateUrl}
          placeholder="https://example.com/openapi.yaml"
        />
      </SourceField>
    </div>
  )
}

function DetailsStep({
  discovery,
  draft,
  update,
}: {
  discovery: Extract<SourceDiscoveryData, { status: 'success' }> | null
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
      {discovery ? (
        <Typography.BodySmall variant="tertiary">
          {discoverySummary(discovery)} Review the source details.
        </Typography.BodySmall>
      ) : null}
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
  format: SourceDocumentFormat
  inspectionError?: string
}): string {
  if (discovery.format === 'mcp') return 'Detected an MCP endpoint from its URL.'
  if (discovery.format === 'openapi-json') return 'Detected an OpenAPI JSON document.'
  if (discovery.format === 'openapi-yaml') return 'Detected an OpenAPI YAML document.'
  if (discovery.inspectionError) {
    return `The source document could not be inspected. ${asSentence(discovery.inspectionError)}`
  }
  return 'No OpenAPI document was detected.'
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
  discovery: Extract<SourceDiscoveryData, { status: 'success' }> | null
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
      {discovery && discovery.auth.kind !== 'unknown' ? (
        <Typography.BodySmall variant="tertiary">
          Detected authentication: {discovery.auth.label}.
        </Typography.BodySmall>
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
