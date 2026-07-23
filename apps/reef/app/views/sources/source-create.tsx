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

import type { SourcesActionData } from '@/routes/sources-action'
import type {
  SourceDetectedAuth,
  SourceDiscoveryData,
  SourceDocumentFormat,
} from '@/routes/source-discovery'

import * as styles from './source-create.css'
import { SourceError, SourceField, SourceHeader } from './source-presentation'

const SECRET_KEY = 'API_TOKEN'
const NAME_PATTERN = /^[a-z][a-z0-9_]*$/
const RESERVED_SOURCE_NAMES = new Set(['coral', 'coral_admin', 'public'])

type SurfaceType = 'openapi' | 'mcp'
type AuthChoice = 'none' | 'bearer' | 'header'

interface Draft {
  name: string
  description: string
  surfaceType: SurfaceType
  url: string
  auth: AuthChoice
  headerName: string
  token: string
}

const EMPTY_DRAFT: Draft = {
  name: '',
  description: '',
  surfaceType: 'openapi',
  url: '',
  auth: 'bearer',
  headerName: '',
  token: '',
}

const STEP_COUNT = 3

export function SourceCreateDialog({
  actionData,
  discoveryPath,
  open,
  onOpenChange,
}: {
  actionData: SourcesActionData
  discoveryPath: string
  open: boolean
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
        <Dialog.Popup size="l">
          {open ? (
            <SourceCreateDialogContent
              actionData={actionData}
              discoveryPath={discoveryPath}
              onCancel={() => onOpenChange(false)}
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
  onCancel,
  requestCancelRef,
}: {
  actionData: SourcesActionData
  discoveryPath: string
  onCancel: () => void
  requestCancelRef: RefObject<() => void>
}) {
  const [step, setStep] = useState(0)
  const [draft, setDraft] = useState<Draft>(EMPTY_DRAFT)
  const [confirmingCancel, setConfirmingCancel] = useState(false)
  const formId = useId()
  const discovery = useFetcher<SourceDiscoveryData>()

  const navigation = useNavigation()
  const submitting = navigation.state !== 'idle' && navigation.formData?.get('_intent') === 'import'
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
    if (step !== 0) return
    const detectedAuth = authChoiceFromDiscovery(result.auth)
    setDraft((current) => ({
      ...current,
      ...(detectedAuth ? { auth: detectedAuth } : {}),
      description: result.description || current.description,
      headerName: result.auth.headerName || current.headerName,
      name: result.name || current.name,
      surfaceType:
        result.format === 'mcp'
          ? 'mcp'
          : result.format === 'unknown'
            ? current.surfaceType
            : 'openapi',
    }))
    setStep(1)
  }, [discovery.data, step])

  const update = (patch: Partial<Draft>) => setDraft((prev) => ({ ...prev, ...patch }))
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
      return true
    }
    if (draft.auth === 'header' && draft.headerName.trim().length === 0) return false
    return draft.auth === 'none' || draft.token.trim().length > 0
  })()

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    if (step === 2) return

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
      {draft.auth !== 'none' ? (
        <>
          <input type="hidden" name="secret_key" value={SECRET_KEY} />
          <input type="hidden" name="secret_value" value={draft.token.trim()} />
        </>
      ) : null}

      <StepHeader step={0} />
      <UrlStep draft={draft} update={update} />

      {discoveryError ? <SourceError>{discoveryError}</SourceError> : null}

      <Dialog.Actions>
        <ButtonContainer disabled={submitting} onClick={requestCancel} size="32" variant="bare">
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
          <Dialog.Popup size="l">
            <div className={styles.dialogContent}>
              <StepHeader step={1} />
              <DetailsStep discovery={discoveryResult} draft={draft} update={update} />

              <Dialog.Actions>
                <ButtonContainer
                  disabled={submitting}
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
                  <Dialog.Popup size="l">
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

                      <Dialog.Actions>
                        <ButtonContainer
                          disabled={submitting}
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
                          <ButtonText>{submitting ? 'Creating…' : 'Create source'}</ButtonText>
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
              <ButtonContainer onClick={onCancel} size="32" variant="destructive">
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

function UrlStep({ draft, update }: { draft: Draft; update: (patch: Partial<Draft>) => void }) {
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
          onChange={(value) => update({ url: value })}
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
  const [nameTouched, setNameTouched] = useState(false)
  const name = draft.name.trim()
  const nameError = nameTouched ? sourceNameValidationError(name) : null

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
  const authChoices: { key: AuthChoice; label: string }[] =
    draft.surfaceType === 'openapi'
      ? [
          { key: 'none', label: 'None' },
          { key: 'bearer', label: 'Bearer token' },
          { key: 'header', label: 'Custom header' },
        ]
      : [
          { key: 'none', label: 'None' },
          { key: 'bearer', label: 'Bearer token' },
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
  }
  lines.push('surface:')

  if (draft.surfaceType === 'openapi') {
    lines.push('  type: openapi', `  url: ${s(url)}`)
    if (draft.auth !== 'none') {
      const headerName = draft.auth === 'bearer' ? 'Authorization' : draft.headerName.trim()
      const template =
        draft.auth === 'bearer' ? `Bearer {{input.${SECRET_KEY}}}` : `{{input.${SECRET_KEY}}}`
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
