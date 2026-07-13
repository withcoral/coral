import classNames from 'classnames'
import { useMemo, useState } from 'react'
import { Form } from 'react-router'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { SpinningButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Dialog } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Typography } from '@/wax/components/typography'

import { Markdown } from '@/components/markdown'
import type {
  CatalogEntry,
  CatalogOAuthCredentialMethod,
  CatalogSourceCredentialMethod,
  CatalogSourceInputSpec,
} from '@/lib/sources'
import { toSentenceCase } from '@/utils/to-sentence-case'

import { ProviderLogo } from './provider-logo'
import * as styles from './source-install.css'

function formatFieldName(key: string): string {
  return toSentenceCase(key.replace(/_/g, ' '))
}

export function SourceInstallDialog({
  actionError,
  entry,
  open,
  onOpenChange,
  submitting,
}: {
  actionError?: string | null
  entry: CatalogEntry | null
  open: boolean
  onOpenChange: (open: boolean) => void
  submitting?: boolean
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="l">
          {entry ? (
            <SourceInstallDialogContent
              actionError={actionError}
              entry={entry}
              onCancel={() => onOpenChange(false)}
              submitting={submitting ?? false}
            />
          ) : null}
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function SourceInstallDialogContent({
  actionError,
  entry,
  onCancel,
  submitting,
}: {
  actionError?: string | null
  entry: CatalogEntry
  onCancel: () => void
  submitting: boolean
}) {
  const [values, setValues] = useState<Record<string, string>>({})
  const [methodChoices, setMethodChoices] = useState<Record<string, number>>({})

  const inputSpecs = entry.inputSpecs
  const inputs: CatalogSourceInputSpec[] = inputSpecs ?? []

  const effectiveChoice = (input: CatalogSourceInputSpec): number => methodChoices[input.key] ?? 0

  const canSubmit = useMemo(() => {
    if (!inputSpecs) return false
    return inputSpecs.every((input) => {
      if (!input.required) return true
      const choice = methodChoices[input.key] ?? 0
      if (input.input.case === 'variable') {
        const def = input.input.value.defaultValue
        return (values[input.key] ?? def).trim().length > 0
      }
      if (input.input.case === 'secret') {
        const method = input.input.value.credential?.methods[choice]
        if (!method || method.method.case === 'sourceConfig') {
          return (values[input.key] ?? '').trim().length > 0
        }
        if (method.method.case === 'oauth') {
          return oauthMethodReady(method.method.value, values)
        }
      }
      return true
    })
  }, [inputSpecs, values, methodChoices])

  return (
    <Form method="post">
      <input type="hidden" name="_intent" value="install" />
      <input type="hidden" name="name" value={entry.name} />

      <div className={styles.header}>
        <ProviderLogo name={entry.name} size="large" />
        <div className={styles.headerText}>
          <Dialog.Title className={styles.headerTitleRow}>
            <Typography.HeadingMedium as="span" className={styles.headerTitle}>
              {entry.name}
            </Typography.HeadingMedium>
            <span className={styles.headerPill}>Core</span>
          </Dialog.Title>
          <Dialog.Description render={<div />}>
            <Markdown>{entry.description}</Markdown>
          </Dialog.Description>
        </div>
      </div>

      {!inputSpecs ? (
        <div className={classNames(styles.alertBox, styles.alertError)}>
          <Icon color="inherit" name="CircleAlert" size="14" />
          <Typography.BodySmall>Source metadata is unavailable.</Typography.BodySmall>
        </div>
      ) : inputs.length === 0 ? (
        <Typography.BodySmall variant="tertiary">
          No configuration needed — click Add source to install.
        </Typography.BodySmall>
      ) : (
        <div className={styles.fieldGroup}>
          {inputs.map((input) => (
            <InputRow
              key={input.key}
              input={input}
              methodIndex={effectiveChoice(input)}
              values={values}
              disabled={submitting}
              onValueChange={(key, value) => setValues((p) => ({ ...p, [key]: value }))}
              onMethodChange={(key, index) => setMethodChoices((p) => ({ ...p, [key]: index }))}
            />
          ))}
        </div>
      )}

      {actionError ? (
        <div className={classNames(styles.alertBox, styles.alertError)}>
          <Icon color="inherit" name="CircleAlert" size="14" />
          <Typography.BodySmall>{actionError}</Typography.BodySmall>
        </div>
      ) : null}

      <Dialog.Actions>
        <ButtonContainer disabled={submitting} onClick={onCancel} size="32" variant="bare">
          <ButtonText>Cancel</ButtonText>
        </ButtonContainer>
        <ButtonContainer
          disabled={submitting || !canSubmit}
          size="32"
          type="submit"
          variant="primary"
        >
          {submitting ? <SpinningButtonIcon name="Loader" /> : null}
          <ButtonText>{submitting ? 'Adding…' : 'Add source'}</ButtonText>
        </ButtonContainer>
      </Dialog.Actions>
    </Form>
  )
}

function InputRow({
  input,
  methodIndex,
  values,
  disabled,
  onValueChange,
  onMethodChange,
}: {
  input: CatalogSourceInputSpec
  methodIndex: number
  values: Record<string, string>
  disabled: boolean
  onValueChange: (key: string, value: string) => void
  onMethodChange: (key: string, index: number) => void
}) {
  if (input.input.case === 'variable') {
    const def = input.input.value.defaultValue
    return (
      <Field input={input}>
        <TextInput
          name={`var:${input.key}`}
          value={values[input.key] ?? def}
          onChange={(value) => onValueChange(input.key, value)}
          placeholder={def || formatFieldName(input.key)}
          disabled={disabled}
        />
      </Field>
    )
  }

  if (input.input.case !== 'secret') return null

  const credential = input.input.value.credential
  const methods = credential?.methods ?? []
  const selected = methods[methodIndex]

  return (
    <Field input={input} fullWidth={methods.length > 1 || isOAuth(selected)}>
      {methods.length > 0 ? (
        <input type="hidden" name={`method:${input.key}`} value={methodIndex} />
      ) : null}

      {methods.length > 1 ? (
        <div className={styles.methodTabs}>
          {methods.map((m, i) => (
            <button
              key={i}
              type="button"
              className={styles.methodTab}
              data-active={i === methodIndex ? 'true' : 'false'}
              disabled={disabled}
              onClick={() => onMethodChange(input.key, i)}
            >
              {methodLabel(m, i)}
            </button>
          ))}
        </div>
      ) : null}

      {!selected || selected.method.case === 'sourceConfig' ? (
        <TextInput
          name={`sec:${input.key}`}
          type="password"
          value={values[input.key] ?? ''}
          onChange={(value) => onValueChange(input.key, value)}
          placeholder={formatFieldName(input.key)}
          disabled={disabled}
        />
      ) : selected.method.case === 'oauth' ? (
        <OAuthFields
          oauth={selected.method.value}
          values={values}
          disabled={disabled}
          onValueChange={onValueChange}
        />
      ) : null}
    </Field>
  )
}

function Field({
  input,
  children,
  fullWidth,
}: {
  input: CatalogSourceInputSpec
  children: React.ReactNode
  fullWidth?: boolean
}) {
  return (
    <div className={classNames(styles.fieldItem, fullWidth ? styles.fieldItemFull : null)}>
      <Typography.Body className={styles.fieldLabel}>{formatFieldName(input.key)}</Typography.Body>
      {children}
      {input.hint ? <Markdown>{input.hint}</Markdown> : null}
    </div>
  )
}

function OAuthFields({
  oauth,
  values,
  disabled,
  onValueChange,
}: {
  oauth: CatalogOAuthCredentialMethod
  values: Record<string, string>
  disabled: boolean
  onValueChange: (key: string, value: string) => void
}) {
  const fields = oauthInputs(oauth)
  if (fields.length === 0) {
    return (
      <Typography.BodySmall variant="secondary">
        OAuth installation will be handled by the route action.
      </Typography.BodySmall>
    )
  }
  return (
    <div className={styles.oauthFields}>
      {fields.map(({ key, secret, defaultValue }) => (
        <div key={key} className={styles.fieldItem}>
          <Typography.Body className={styles.fieldLabel}>{formatFieldName(key)}</Typography.Body>
          <TextInput
            type={secret ? 'password' : 'text'}
            value={values[key] ?? ''}
            onChange={(value) => onValueChange(key, value)}
            placeholder={defaultValue || formatFieldName(key)}
            disabled={disabled}
          />
        </div>
      ))}
    </div>
  )
}

function methodLabel(method: CatalogSourceCredentialMethod, index: number): string {
  if (method.label) return method.label
  if (method.method.case === 'sourceConfig') return 'Paste token'
  if (method.method.case === 'oauth') return 'OAuth'
  return `Method ${index + 1}`
}

function isOAuth(method: CatalogSourceCredentialMethod | undefined): boolean {
  return method?.method.case === 'oauth'
}

interface OAuthInput {
  key: string
  secret: boolean
  defaultValue?: string
  required: boolean
}

function oauthInputs(oauth: CatalogOAuthCredentialMethod): OAuthInput[] {
  const out: OAuthInput[] = []
  const id = oauth.client?.id
  if (id?.input) {
    out.push({
      key: id.input,
      secret: false,
      defaultValue: id.defaultValue,
      required: !id.defaultValue,
    })
  }
  const secret = oauth.client?.secret
  if (secret?.input) {
    out.push({ key: secret.input, secret: true, required: true })
  }
  return out
}

function oauthMethodReady(
  oauth: CatalogOAuthCredentialMethod,
  values: Record<string, string>,
): boolean {
  return oauthInputs(oauth).every((input) => {
    if (!input.required) return true
    return (values[input.key] ?? input.defaultValue ?? '').trim().length > 0
  })
}
