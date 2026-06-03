import { create } from '@bufbuild/protobuf'
import classNames from 'classnames'
import { useEffect, useMemo, useState } from 'react'

import {
  OAuthCredentialRetrievalSchema,
  type OAuthCredentialMethod,
  type SourceCredentialMethod,
  type SourceInputSpec,
} from '@/generated/coral/v1/sources_pb'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { Icon as ButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import * as Dialog from '@/wax/components/dialog'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { addToast } from '@/wax/components/toast'
import { Typography } from '@/wax/components/typography'

import { Markdown } from '@/components/markdown'
import { providerIcon } from '@/lib/provider-icons'
import {
  createBundledSource,
  createBundledSourceWithOAuth,
  getSourceInfo,
  type InstallInput,
  type ResolvedSourceInfo,
} from '@/lib/sources'
import { toSentenceCase } from '@/utils/to-sentence-case'

import * as styles from './source-install.css'

type InstallProgress =
  | { kind: 'idle' }
  | { kind: 'busy' }
  | {
      kind: 'awaiting-oauth'
      inputKey: string
      authorizationUrl: string
      userCode: string
      verificationUri: string
      verificationUriComplete: string
    }
  | { kind: 'oauth-completed'; inputKey: string }

function formatFieldName(key: string): string {
  return toSentenceCase(key.replace(/_/g, ' '))
}

export function SourceInstallDialog({
  name,
  open,
  onOpenChange,
  onInstalled,
}: {
  name: string | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onInstalled: (name: string) => void
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="l">
          {name ? (
            <SourceInstallDialogContent
              name={name}
              onCancel={() => onOpenChange(false)}
              onInstalled={onInstalled}
            />
          ) : null}
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function SourceInstallDialogContent({
  name,
  onCancel,
  onInstalled,
}: {
  name: string
  onCancel: () => void
  onInstalled: (name: string) => void
}) {
  const [resolved, setResolved] = useState<ResolvedSourceInfo | null>(null)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [values, setValues] = useState<Record<string, string>>({})
  const [methodChoices, setMethodChoices] = useState<Record<string, number>>({})
  const [progress, setProgress] = useState<InstallProgress>({ kind: 'idle' })
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    getSourceInfo(name)
      .then((info) => !cancelled && setResolved(info))
      .catch((e) => !cancelled && setLoadError(e instanceof Error ? e.message : String(e)))
    return () => {
      cancelled = true
    }
  }, [name])

  const inputs: SourceInputSpec[] = resolved?.info.inputs ?? []
  const icon = providerIcon(name)
  const busy = progress.kind !== 'idle'

  const effectiveChoice = (input: SourceInputSpec): number => methodChoices[input.key] ?? 0

  const canSubmit = useMemo(() => {
    if (!resolved) return false
    return resolved.info.inputs.every((input) => {
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
  }, [resolved, values, methodChoices])

  async function submit() {
    if (!resolved) return
    setError(null)
    setProgress({ kind: 'busy' })

    try {
      const bindings: InstallInput[] = []
      const retrievalProtos = []

      for (const input of inputs) {
        if (input.input.case === 'variable') {
          const value = (values[input.key] ?? input.input.value.defaultValue ?? '').trim()
          if (value.length > 0) bindings.push({ key: input.key, value, secret: false })
          continue
        }
        if (input.input.case !== 'secret') continue

        const method = input.input.value.credential?.methods[effectiveChoice(input)]
        if (!method || method.method.case === 'sourceConfig') {
          const value = (values[input.key] ?? '').trim()
          if (value.length > 0) bindings.push({ key: input.key, value, secret: true })
          continue
        }
        if (method.method.case === 'oauth') {
          retrievalProtos.push(
            create(OAuthCredentialRetrievalSchema, {
              inputKey: input.key,
              methodIndex: effectiveChoice(input),
              credentialInputs: oauthCredentialInputs(method.method.value, values),
            }),
          )
        }
      }

      const callbacks = {
        onAuthorization: (event: {
          inputKey: string
          authorizationUrl: string
          userCode: string
          verificationUri: string
          verificationUriComplete: string
        }) => {
          setProgress({
            kind: 'awaiting-oauth',
            inputKey: event.inputKey,
            authorizationUrl: event.authorizationUrl,
            userCode: event.userCode,
            verificationUri: event.verificationUri,
            verificationUriComplete: event.verificationUriComplete,
          })
          window.open(event.authorizationUrl, '_blank', 'noopener,noreferrer')
        },
        onCompleted: (event: { inputKey: string }) => {
          setProgress({ kind: 'oauth-completed', inputKey: event.inputKey })
        },
      }

      if (retrievalProtos.length === 0) {
        await createBundledSource(name, bindings)
      } else {
        await createBundledSourceWithOAuth(name, bindings, retrievalProtos, callbacks)
      }

      addToast('neutral', {
        title: `Configured ${name}`,
        description: 'Credentials were saved but not verified.',
      })
      onInstalled(name)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setProgress({ kind: 'idle' })
    }
  }

  return (
    <>
      <div className={styles.header}>
        <div className={styles.headerLogo}>
          {icon ? (
            <img alt="" className={styles.headerLogoImg} src={icon} />
          ) : (
            <Icon name="Plug" size="22" color="secondary" />
          )}
        </div>
        <div className={styles.headerText}>
          <Dialog.Title className={styles.headerTitleRow}>
            <Typography.HeadingMedium as="span" className={styles.headerTitle}>
              {name}
            </Typography.HeadingMedium>
            <span className={styles.headerPill}>Core</span>
          </Dialog.Title>
          <Dialog.Description render={<div />}>
            <Markdown>{resolved?.info.description ?? 'Officially supported by Coral.'}</Markdown>
          </Dialog.Description>
        </div>
      </div>

      {loadError ? (
        <div className={classNames(styles.alertBox, styles.alertError)}>
          <Icon color="inherit" name="CircleAlert" size="14" />
          <Typography.BodySmall>{loadError}</Typography.BodySmall>
        </div>
      ) : null}

      {resolved === null && !loadError ? (
        <Typography.BodySmall variant="tertiary">Loading…</Typography.BodySmall>
      ) : !resolved ? null : (
        <>
          {inputs.length === 0 ? (
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
                  disabled={busy}
                  onValueChange={(key, value) => setValues((p) => ({ ...p, [key]: value }))}
                  onMethodChange={(key, index) => setMethodChoices((p) => ({ ...p, [key]: index }))}
                />
              ))}
            </div>
          )}

          {progress.kind === 'awaiting-oauth' ? (
            <OAuthProgress
              authorizationUrl={progress.authorizationUrl}
              inputKey={progress.inputKey}
              userCode={progress.userCode}
              verificationUri={progress.verificationUri}
              verificationUriComplete={progress.verificationUriComplete}
            />
          ) : null}
          {progress.kind === 'oauth-completed' ? (
            <div className={styles.oauthBox}>
              <Icon name="CircleCheck" size="16" color="success" />
              <Typography.BodySmall variant="primary">
                {progress.inputKey} authorized. Finishing install…
              </Typography.BodySmall>
            </div>
          ) : null}

          {error ? (
            <div className={classNames(styles.alertBox, styles.alertError)}>
              <Icon color="inherit" name="CircleAlert" size="14" />
              <Typography.BodySmall>{error}</Typography.BodySmall>
            </div>
          ) : null}

          <Dialog.Actions>
            <ButtonContainer disabled={busy} onClick={onCancel} size="32" variant="bare">
              <ButtonText>Cancel</ButtonText>
            </ButtonContainer>
            <ButtonContainer
              disabled={busy || !canSubmit}
              onClick={() => void submit()}
              size="32"
              variant="primary"
            >
              {busy ? <ButtonIcon name="Loader" /> : null}
              <ButtonText>{busyLabel(progress)}</ButtonText>
            </ButtonContainer>
          </Dialog.Actions>
        </>
      )}
    </>
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
  input: SourceInputSpec
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
  input: SourceInputSpec
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
  oauth: OAuthCredentialMethod
  values: Record<string, string>
  disabled: boolean
  onValueChange: (key: string, value: string) => void
}) {
  const fields = oauthInputs(oauth)
  if (fields.length === 0) {
    return (
      <Typography.BodySmall variant="secondary">
        Click Add source to open your browser and complete sign-in.
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

function OAuthProgress({
  authorizationUrl,
  inputKey,
  userCode,
  verificationUri,
  verificationUriComplete,
}: {
  authorizationUrl: string
  inputKey: string
  userCode: string
  verificationUri: string
  verificationUriComplete: string
}) {
  const link = verificationUriComplete || authorizationUrl
  const displayUri = verificationUri || authorizationUrl

  return (
    <div className={styles.oauthBox}>
      <Icon name="Loader" size="16" color="secondary" />
      <div>
        <Typography.BodySmall variant="primary">
          Waiting for {formatFieldName(inputKey)} authorization in your browser…
        </Typography.BodySmall>
        {userCode ? (
          <>
            <Typography.BodySmall variant="secondary">
              Enter code <code className={styles.oauthCode}>{userCode}</code> at{' '}
              <a href={link} target="_blank" rel="noopener noreferrer">
                {displayUri}
              </a>
              .
            </Typography.BodySmall>
            <Typography.BodySmall variant="tertiary">
              If the new tab didn't open, use the link above.
            </Typography.BodySmall>
          </>
        ) : (
          <Typography.BodySmall variant="tertiary">
            If the new tab didn't open,{' '}
            <a href={authorizationUrl} target="_blank" rel="noopener noreferrer">
              click here to open it
            </a>
            .
          </Typography.BodySmall>
        )}
      </div>
    </div>
  )
}

function methodLabel(method: SourceCredentialMethod, index: number): string {
  if (method.label) return method.label
  if (method.method.case === 'sourceConfig') return 'Paste token'
  if (method.method.case === 'oauth') return 'OAuth'
  return `Method ${index + 1}`
}

function isOAuth(method: SourceCredentialMethod | undefined): boolean {
  return method?.method.case === 'oauth'
}

interface OAuthInput {
  key: string
  secret: boolean
  defaultValue?: string
  required: boolean
}

function oauthInputs(oauth: OAuthCredentialMethod): OAuthInput[] {
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

function oauthMethodReady(oauth: OAuthCredentialMethod, values: Record<string, string>): boolean {
  return oauthInputs(oauth)
    .filter((input) => input.required)
    .every(({ key }) => (values[key] ?? '').trim().length > 0)
}

function oauthCredentialInputs(
  oauth: OAuthCredentialMethod,
  values: Record<string, string>,
): { key: string; value: string }[] {
  return oauthInputs(oauth)
    .map(({ key }) => ({ key, value: (values[key] ?? '').trim() }))
    .filter((entry) => entry.value.length > 0)
}

function busyLabel(progress: InstallProgress): string {
  if (progress.kind === 'busy') return 'Adding…'
  if (progress.kind === 'awaiting-oauth') return 'Awaiting OAuth…'
  if (progress.kind === 'oauth-completed') return 'Finishing…'
  return 'Add source'
}
