import classNames from 'classnames'
import { useEffect, useId, useRef, useState } from 'react'
import { Form, useNavigation } from 'react-router'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { SpinningButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Dialog } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { addToast } from '@/wax/components/toast'
import { Typography } from '@/wax/components/typography'

import type { SourcesActionData } from '@/routes/sources-action'

import * as styles from './source-create.css'

const SECRET_KEY = 'API_TOKEN'
const NAME_PATTERN = /^[a-z][a-z0-9_]*$/

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
  open,
  onOpenChange,
}: {
  actionData: SourcesActionData
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="l">
          {open ? (
            <SourceCreateDialogContent
              actionData={actionData}
              onCancel={() => onOpenChange(false)}
              onCreated={() => onOpenChange(false)}
            />
          ) : null}
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function SourceCreateDialogContent({
  actionData,
  onCancel,
  onCreated,
}: {
  actionData: SourcesActionData
  onCancel: () => void
  onCreated: () => void
}) {
  const [step, setStep] = useState(0)
  const [draft, setDraft] = useState<Draft>(EMPTY_DRAFT)
  const formId = useId()

  const navigation = useNavigation()
  const submitting = navigation.state !== 'idle' && navigation.formData?.get('_intent') === 'import'
  const importError =
    actionData?.status === 'error' && actionData.intent === 'import' ? actionData.message : null

  // The action redirects on success, so a submit that settles without an
  // import error means the source was created.
  const wasSubmitting = useRef(false)
  useEffect(() => {
    if (submitting) {
      wasSubmitting.current = true
      return
    }
    if (!wasSubmitting.current || navigation.state !== 'idle') return
    wasSubmitting.current = false
    if (!importError) {
      addToast('neutral', {
        title: `Created ${draft.name.trim()}`,
        description: 'The source was validated and installed.',
      })
      onCreated()
    }
  }, [submitting, navigation.state, importError, draft.name, onCreated])

  const update = (patch: Partial<Draft>) => setDraft((prev) => ({ ...prev, ...patch }))

  const stepValid = (() => {
    if (step === 0) return NAME_PATTERN.test(draft.name.trim())
    if (step === 1) {
      if (!draft.url.trim().startsWith('https://')) return false
      if (draft.auth === 'header' && draft.headerName.trim().length === 0) return false
      return true
    }
    return draft.auth === 'none' || draft.token.trim().length > 0
  })()

  return (
    <Form className={styles.dialogContent} id={formId} method="post">
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
      <BasicsStep draft={draft} update={update} />

      <Dialog.Actions>
        <ButtonContainer disabled={submitting} onClick={onCancel} size="32" variant="bare">
          <ButtonText>Cancel</ButtonText>
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
              <ConnectionStep draft={draft} update={update} />

              <Dialog.Actions>
                <ButtonContainer disabled={submitting} onClick={onCancel} size="32" variant="bare">
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

              <Dialog.Root
                open={step >= 2}
                onOpenChange={(open) => {
                  if (!open) setStep(1)
                }}
              >
                <Dialog.Portal>
                  <Dialog.Popup size="l">
                    <div className={styles.dialogContent}>
                      <StepHeader step={2} />
                      <CredentialsStep draft={draft} update={update} disabled={submitting} />

                      {importError ? (
                        <div className={classNames(styles.alertBox, styles.alertError)}>
                          <Icon color="inherit" name="CircleAlert" size="14" />
                          <Typography.BodySmall>{importError}</Typography.BodySmall>
                        </div>
                      ) : null}

                      <Dialog.Actions>
                        <ButtonContainer
                          disabled={submitting}
                          onClick={onCancel}
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
    </Form>
  )
}

function StepHeader({ step }: { step: number }) {
  return (
    <div className={styles.header}>
      <Dialog.Title>
        <Typography.HeadingMedium as="span">Create source</Typography.HeadingMedium>
      </Dialog.Title>
      <Dialog.Description render={<div />}>
        <Typography.BodySmall className={styles.stepLabel}>
          Step {step + 1} of {STEP_COUNT} — {stepTitle(step)}
        </Typography.BodySmall>
      </Dialog.Description>
    </div>
  )
}

function stepTitle(step: number): string {
  if (step === 0) return 'Basics'
  if (step === 1) return 'Connection'
  return 'Credentials'
}

function BasicsStep({ draft, update }: { draft: Draft; update: (patch: Partial<Draft>) => void }) {
  const idName = useId()
  const idDescription = useId()
  const name = draft.name.trim()
  const nameInvalid = name.length > 0 && !NAME_PATTERN.test(name)
  return (
    <div className={styles.fieldGroup}>
      <div className={styles.fieldItem}>
        <Typography.Body as="label" htmlFor={idName} className={styles.fieldLabel}>
          Name
        </Typography.Body>
        <TextInput
          id={idName}
          value={draft.name}
          onChange={(value) => update({ name: value })}
          placeholder="my_api"
        />
        <Typography.BodySmall variant={nameInvalid ? 'primary' : 'tertiary'}>
          Lowercase letters, digits, and underscores; must start with a letter. Used as the schema
          name in queries.
        </Typography.BodySmall>
      </div>
      <div className={styles.fieldItem}>
        <Typography.Body as="label" htmlFor={idDescription} className={styles.fieldLabel}>
          Description (optional)
        </Typography.Body>
        <TextInput
          id={idDescription}
          value={draft.description}
          onChange={(value) => update({ description: value })}
          placeholder="What this source connects to"
        />
      </div>
    </div>
  )
}

function ConnectionStep({
  draft,
  update,
}: {
  draft: Draft
  update: (patch: Partial<Draft>) => void
}) {
  const idUrl = useId()
  const idHeaderName = useId()
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
      <div className={styles.fieldItem}>
        <Typography.Body className={styles.fieldLabel}>Type</Typography.Body>
        <ChoiceTabs
          choices={[
            { key: 'openapi', label: 'REST API (OpenAPI)' },
            { key: 'mcp', label: 'MCP server' },
          ]}
          selected={draft.surfaceType}
          onSelect={(key) =>
            update({
              surfaceType: key,
              // Custom-header auth only applies to OpenAPI surfaces.
              auth: key === 'mcp' && draft.auth === 'header' ? 'bearer' : draft.auth,
            })
          }
        />
      </div>
      <div className={styles.fieldItem}>
        <Typography.Body as="label" htmlFor={idUrl} className={styles.fieldLabel}>
          {draft.surfaceType === 'openapi' ? 'OpenAPI descriptor URL' : 'MCP server URL'}
        </Typography.Body>
        <TextInput
          id={idUrl}
          value={draft.url}
          onChange={(value) => update({ url: value })}
          placeholder={
            draft.surfaceType === 'openapi'
              ? 'https://example.com/openapi.yaml'
              : 'https://example.com/mcp'
          }
        />
        <Typography.BodySmall variant="tertiary">
          {draft.surfaceType === 'openapi'
            ? 'HTTPS link to the OpenAPI (Swagger) document describing the API.'
            : 'HTTPS URL of a streamable-HTTP MCP server.'}
        </Typography.BodySmall>
      </div>
      <div className={styles.fieldItem}>
        <Typography.Body className={styles.fieldLabel}>Authentication</Typography.Body>
        <ChoiceTabs
          choices={authChoices}
          selected={draft.auth}
          onSelect={(key) => update({ auth: key })}
        />
      </div>
      {draft.auth === 'header' ? (
        <div className={styles.fieldItem}>
          <Typography.Body as="label" htmlFor={idHeaderName} className={styles.fieldLabel}>
            Header name
          </Typography.Body>
          <TextInput
            id={idHeaderName}
            value={draft.headerName}
            onChange={(value) => update({ headerName: value })}
            placeholder="X-Api-Key"
          />
        </div>
      ) : null}
    </div>
  )
}

function CredentialsStep({
  draft,
  update,
  disabled,
}: {
  draft: Draft
  update: (patch: Partial<Draft>) => void
  disabled: boolean
}) {
  const idToken = useId()
  return (
    <div className={styles.fieldGroup}>
      {draft.auth === 'none' ? (
        <Typography.BodySmall variant="tertiary">
          No credentials needed — click Create source to install.
        </Typography.BodySmall>
      ) : (
        <div className={styles.fieldItem}>
          <Typography.Body as="label" htmlFor={idToken} className={styles.fieldLabel}>
            {draft.auth === 'bearer' ? 'Bearer token' : `${draft.headerName.trim()} value`}
          </Typography.Body>
          <TextInput
            id={idToken}
            type="password"
            value={draft.token}
            onChange={(value) => update({ token: value })}
            placeholder="Paste token"
            disabled={disabled}
          />
        </div>
      )}
      <div className={styles.summaryBox}>
        <SummaryRow label="Name" value={draft.name.trim()} />
        <SummaryRow
          label="Type"
          value={draft.surfaceType === 'openapi' ? 'REST API (OpenAPI)' : 'MCP server'}
        />
        <SummaryRow label="URL" value={draft.url.trim()} />
        <SummaryRow label="Auth" value={authSummary(draft)} />
      </div>
    </div>
  )
}

function SummaryRow({ label, value }: { label: string; value: string }) {
  return (
    <div className={styles.summaryRow}>
      <Typography.BodySmall className={styles.summaryKey}>{label}</Typography.BodySmall>
      <span className={styles.summaryValue}>{value}</span>
    </div>
  )
}

function authSummary(draft: Draft): string {
  if (draft.auth === 'none') return 'None'
  if (draft.auth === 'bearer') return 'Bearer token'
  return `Header ${draft.headerName.trim()}`
}

function ChoiceTabs<K extends string>({
  choices,
  selected,
  onSelect,
}: {
  choices: { key: K; label: string }[]
  selected: K
  onSelect: (key: K) => void
}) {
  return (
    <div className={styles.choiceTabs}>
      {choices.map((choice) => (
        <button
          key={choice.key}
          type="button"
          className={styles.choiceTab}
          data-active={choice.key === selected ? 'true' : 'false'}
          onClick={() => onSelect(choice.key)}
        >
          {choice.label}
        </button>
      ))}
    </div>
  )
}

/** Quote a scalar as a JSON string, which YAML parses as a flow scalar. */
const s = (value: string) => JSON.stringify(value)

/** Build a DSL v4 source manifest from the wizard fields. */
function buildManifestYaml(draft: Draft): string {
  const name = draft.name.trim()
  const url = draft.url.trim()
  const lines: string[] = [`name: ${s(name)}`, 'dsl_version: 4']
  if (draft.description.trim()) lines.push(`description: ${s(draft.description.trim())}`)
  lines.push('surfaces:')

  const inputLines =
    draft.auth === 'none'
      ? []
      : [
          '    inputs:',
          `      ${SECRET_KEY}:`,
          '        kind: secret',
          `        hint: ${s(`API token for ${name}`)}`,
        ]

  if (draft.surfaceType === 'openapi') {
    lines.push('  - id: api', '    type: openapi', `    url: ${s(url)}`, ...inputLines)
    if (draft.auth !== 'none') {
      const headerName = draft.auth === 'bearer' ? 'Authorization' : draft.headerName.trim()
      const template =
        draft.auth === 'bearer' ? `Bearer {{input.${SECRET_KEY}}}` : `{{input.${SECRET_KEY}}}`
      lines.push(
        '    auth:',
        '      type: HeaderAuth',
        '      headers:',
        `        - name: ${s(headerName)}`,
        '          from: template',
        `          template: ${s(template)}`,
      )
    }
  } else {
    lines.push(
      '  - id: mcp',
      '    type: mcp',
      ...inputLines,
      '    server:',
      '      transport: streamable_http',
      `      url: ${s(url)}`,
    )
    if (draft.auth !== 'none') {
      lines.push(
        '      auth:',
        '        type: bearer',
        '        from: input',
        `        key: ${SECRET_KEY}`,
      )
    }
  }
  return lines.join('\n') + '\n'
}
