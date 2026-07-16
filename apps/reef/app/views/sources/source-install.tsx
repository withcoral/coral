import classNames from 'classnames'
import { useEffect, useMemo, useRef, useState } from 'react'
import { Form, useNavigate, useRevalidator } from 'react-router'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { SpinningButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Dialog, Tabs } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Typography } from '@/wax/components/typography'

import { Markdown } from '@/components/markdown'
import { OAuthFields, type OAuthField } from '@/components/sources/install/oauth-fields'
import { OAuthProgress } from '@/components/sources/install/oauth-progress'
import * as oauthStatusStyles from '@/components/sources/install/oauth-status.css'
import { readOAuthInstallStream } from '@/lib/source-oauth-install-stream'
import type {
  CatalogEntry,
  CatalogOAuthCredentialMethod,
  CatalogSourceCredentialMethod,
  CatalogSourceInputSpec,
} from '@/lib/sources'
import { routePath } from '@/routing/routemap'

import * as styles from './source-install.css'
import {
  formatFieldName,
  SourceHeader,
  SourceInputField,
  SourceNoConfiguration,
} from './source-presentation'

type InstallProgress =
  | { kind: 'idle' }
  | { kind: 'busy' }
  | {
      kind: 'awaiting-oauth'
      authorizationUrl: string
      inputKey: string
      userCode: string
      verificationUri: string
      verificationUriComplete: string
    }
  | { kind: 'oauth-callback-received'; inputKey: string }
  | { kind: 'oauth-completed'; inputKey: string }
  | { kind: 'success'; name: string }

export function SourceInstallDialog({
  actionError,
  entry,
  fetchOAuthInstall = fetch,
  onOAuthInstallComplete,
  open,
  openAuthorization = (url) => window.open(url, '_blank', 'noopener,noreferrer'),
  onOpenChange,
  submitting,
  workspaceId,
}: {
  actionError?: string | null
  entry: CatalogEntry | null
  fetchOAuthInstall?: typeof fetch
  onOAuthInstallComplete?: () => Promise<void> | void
  open: boolean
  openAuthorization?: (url: string) => unknown
  onOpenChange: (open: boolean) => void
  submitting?: boolean
  workspaceId: string
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
              fetchOAuthInstall={fetchOAuthInstall}
              onOAuthInstallComplete={onOAuthInstallComplete}
              onCancel={() => onOpenChange(false)}
              openAuthorization={openAuthorization}
              submitting={submitting ?? false}
              workspaceId={workspaceId}
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
  fetchOAuthInstall,
  onOAuthInstallComplete,
  onCancel,
  openAuthorization,
  submitting,
  workspaceId,
}: {
  actionError?: string | null
  entry: CatalogEntry
  fetchOAuthInstall: typeof fetch
  onOAuthInstallComplete?: () => Promise<void> | void
  onCancel: () => void
  openAuthorization: (url: string) => unknown
  submitting: boolean
  workspaceId: string
}) {
  const navigate = useNavigate()
  const revalidator = useRevalidator()
  const abortRef = useRef<AbortController | null>(null)
  const formRef = useRef<HTMLFormElement>(null)
  const [values, setValues] = useState<Record<string, string>>({})
  const [methodChoices, setMethodChoices] = useState<Record<string, number>>({})
  const [progress, setProgress] = useState<InstallProgress>({ kind: 'idle' })
  const [streamError, setStreamError] = useState<string | null>(null)
  const inputSpecs = entry.inputSpecs
  const inputs: CatalogSourceInputSpec[] = inputSpecs ?? []
  const oauthBusy = progress.kind !== 'idle'
  const busy = submitting || oauthBusy

  const effectiveChoice = (input: CatalogSourceInputSpec): number => methodChoices[input.key] ?? 0
  const usesOAuth = inputs.some((input) => {
    if (input.input.case !== 'secret') return false
    return input.input.value.credential?.methods[effectiveChoice(input)]?.method.case === 'oauth'
  })

  const canSubmit = useMemo(() => {
    if (!inputSpecs) return false
    return inputSpecs.every((input) => {
      const choice = methodChoices[input.key] ?? 0
      if (input.input.case === 'variable') {
        if (!input.required) return true
        const def = input.input.value.defaultValue
        return (values[input.key] ?? def).trim().length > 0
      }
      if (input.input.case === 'secret') {
        const method = input.input.value.credential?.methods[choice]
        if (method?.method.case === 'oauth') {
          return oauthMethodReady(method.method.value, values)
        }
        if (!input.required) return true
        return (values[input.key] ?? '').trim().length > 0
      }
      return true
    })
  }, [inputSpecs, values, methodChoices])

  useEffect(() => {
    return () => {
      abortRef.current?.abort()
    }
  }, [])

  function cancel() {
    abortRef.current?.abort()
    onCancel()
  }

  function changeMethod(input: CatalogSourceInputSpec, index: number) {
    const previousIndex = effectiveChoice(input)
    if (index === previousIndex) return

    const keys = credentialMethodValueKeys(input, previousIndex)
    setValues((previous) => clearValues(previous, keys))
    setMethodChoices((previous) => ({ ...previous, [input.key]: index }))
  }

  async function submitOAuthInstall() {
    if (!formRef.current || oauthBusy) return
    setStreamError(null)
    setProgress({ kind: 'busy' })

    const abortController = new AbortController()
    abortRef.current = abortController
    try {
      const response = await fetchOAuthInstall(oauthInstallEndpoint(workspaceId, entry.name), {
        body: new FormData(formRef.current),
        method: 'POST',
        signal: abortController.signal,
      })
      const source = await readOAuthInstallStream(response, {
        onAuthorization: (event) => {
          setProgress({
            kind: 'awaiting-oauth',
            authorizationUrl: event.authorizationUrl,
            inputKey: event.inputKey,
            userCode: event.userCode,
            verificationUri: event.verificationUri,
            verificationUriComplete: event.verificationUriComplete,
          })
          openAuthorization(event.authorizationUrl)
        },
        onCallbackReceived: (event) => {
          setProgress({ kind: 'oauth-callback-received', inputKey: event.inputKey })
        },
        onCompleted: (event) => {
          setProgress({ kind: 'oauth-completed', inputKey: event.inputKey })
        },
        onSource: (event) => {
          setProgress({ kind: 'success', name: event.name })
        },
      })

      if (!abortController.signal.aborted) {
        setProgress({ kind: 'success', name: source.name })
        await revalidator.revalidate()
        if (!abortController.signal.aborted) {
          await (onOAuthInstallComplete
            ? onOAuthInstallComplete()
            : navigate(routePath('workspaceSources', { workspaceId })))
        }
      }
    } catch (error) {
      if (abortController.signal.aborted) return
      setStreamError(error instanceof Error ? error.message : String(error))
      setProgress({ kind: 'idle' })
    } finally {
      if (abortRef.current === abortController) abortRef.current = null
    }
  }

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    if (!usesOAuth) return
    event.preventDefault()
    void submitOAuthInstall()
  }

  return (
    <Form method="post" ref={formRef} onSubmit={handleSubmit}>
      <input type="hidden" name="_intent" value="install" />
      <input type="hidden" name="name" value={entry.name} />

      <SourceHeader
        description={entry.description}
        name={entry.name}
        origin={entry.origin}
        version={entry.version}
      />

      {!inputSpecs ? (
        <div className={classNames(styles.alertBox, styles.alertError)}>
          <Icon color="inherit" name="CircleAlert" size="14" />
          <Typography.BodySmall>Source metadata is unavailable.</Typography.BodySmall>
        </div>
      ) : inputs.length === 0 ? (
        <SourceNoConfiguration />
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
              onMethodChange={(index) => changeMethod(input, index)}
            />
          ))}
        </div>
      )}

      {progress.kind === 'awaiting-oauth' ? (
        <OAuthProgress
          authorizationUrl={progress.authorizationUrl}
          inputLabel={formatFieldName(progress.inputKey)}
          userCode={progress.userCode}
          verificationUri={progress.verificationUri}
          verificationUriComplete={progress.verificationUriComplete}
        />
      ) : null}
      {progress.kind === 'oauth-completed' ? (
        <div className={oauthStatusStyles.box}>
          <Icon name="CircleCheck" size="16" color="success" />
          <Typography.BodySmall variant="primary">
            {formatFieldName(progress.inputKey)} authorized. Finishing install…
          </Typography.BodySmall>
        </div>
      ) : null}
      {progress.kind === 'oauth-callback-received' ? (
        <div className={oauthStatusStyles.box}>
          <Icon name="Loader" size="16" color="secondary" />
          <Typography.BodySmall variant="primary">
            {formatFieldName(progress.inputKey)} authorization received. Exchanging token…
          </Typography.BodySmall>
        </div>
      ) : null}
      {progress.kind === 'success' ? (
        <div className={oauthStatusStyles.box}>
          <Icon name="CircleCheck" size="16" color="success" />
          <Typography.BodySmall variant="primary">
            {formatFieldName(progress.name)} configured.
          </Typography.BodySmall>
        </div>
      ) : null}

      {streamError ? (
        <div className={classNames(styles.alertBox, styles.alertError)}>
          <Icon color="inherit" name="CircleAlert" size="14" />
          <Typography.BodySmall>{streamError}</Typography.BodySmall>
        </div>
      ) : null}

      {actionError ? (
        <div className={classNames(styles.alertBox, styles.alertError)}>
          <Icon color="inherit" name="CircleAlert" size="14" />
          <Typography.BodySmall>{actionError}</Typography.BodySmall>
        </div>
      ) : null}

      <Dialog.Actions>
        <ButtonContainer disabled={submitting} onClick={cancel} size="32" variant="bare">
          <ButtonText>Cancel</ButtonText>
        </ButtonContainer>
        <ButtonContainer
          disabled={busy || !canSubmit}
          onClick={usesOAuth ? () => void submitOAuthInstall() : undefined}
          size="32"
          type={usesOAuth ? 'button' : 'submit'}
          variant="primary"
        >
          {busy ? <SpinningButtonIcon name="Loader" /> : null}
          <ButtonText>{busyLabel(progress, submitting)}</ButtonText>
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
  onMethodChange: (index: number) => void
}) {
  if (input.input.case === 'variable') {
    const def = input.input.value.defaultValue
    return (
      <SourceInputField input={input}>
        <TextInput
          ariaLabel={formatFieldName(input.key)}
          name={`var:${input.key}`}
          value={values[input.key] ?? def}
          onChange={(value) => onValueChange(input.key, value)}
          placeholder={def || formatFieldName(input.key)}
          disabled={disabled}
        />
      </SourceInputField>
    )
  }

  if (input.input.case !== 'secret') return null

  const credential = input.input.value.credential
  const methods = credential?.methods ?? []
  const selected = methods[methodIndex]

  return (
    <SourceInputField input={input} showHint={methods.length <= 1} showLabel={methods.length <= 1}>
      {methods.length > 0 ? (
        <input type="hidden" name={`method:${input.key}`} value={methodIndex} />
      ) : null}

      {methods.length > 1 ? (
        <Tabs.Root
          className={styles.methodTabsRoot}
          onValueChange={(value) => onMethodChange(Number(value))}
          value={methodIndex}
        >
          <Tabs.List
            aria-label={`${formatFieldName(input.key)} setup method`}
            className={styles.methodTabs}
          >
            {methods.map((method, index) => (
              <Tabs.Tab disabled={disabled} key={index} value={index}>
                {methodLabel(method, index)}
              </Tabs.Tab>
            ))}
            <Tabs.Indicator />
          </Tabs.List>
          <div className={styles.methodPanels}>
            {methods.map((method, index) => (
              <div aria-hidden="true" className={styles.methodSizer} inert key={`sizer:${index}`}>
                <CredentialMethodContent
                  disabled
                  hint={input.hint}
                  inputKey={input.key}
                  method={method}
                  onValueChange={onValueChange}
                  values={values}
                />
              </div>
            ))}
            {methods.map((method, index) => (
              <Tabs.Panel className={styles.methodPanel} key={index} value={index}>
                <CredentialMethodContent
                  disabled={disabled || index !== methodIndex}
                  hint={input.hint}
                  inputKey={input.key}
                  method={method}
                  onValueChange={onValueChange}
                  values={values}
                />
              </Tabs.Panel>
            ))}
          </div>
        </Tabs.Root>
      ) : (
        <CredentialMethodFields
          disabled={disabled}
          inputKey={input.key}
          method={selected}
          onValueChange={onValueChange}
          values={values}
        />
      )}
    </SourceInputField>
  )
}

function CredentialMethodContent({
  hint,
  ...fieldProps
}: React.ComponentProps<typeof CredentialMethodFields> & { hint: string }) {
  return (
    <div className={styles.methodPanelContent}>
      <CredentialMethodFields {...fieldProps} />
      {hint ? <Markdown>{hint}</Markdown> : null}
    </div>
  )
}

function CredentialMethodFields({
  disabled,
  inputKey,
  method,
  onValueChange,
  values,
}: {
  disabled: boolean
  inputKey: string
  method: CatalogSourceCredentialMethod | undefined
  onValueChange: (key: string, value: string) => void
  values: Record<string, string>
}) {
  if (!method || method.method.case === 'sourceConfig') {
    return (
      <TextInput
        ariaLabel={formatFieldName(inputKey)}
        disabled={disabled}
        name={`sec:${inputKey}`}
        onChange={(value) => onValueChange(inputKey, value)}
        placeholder={formatFieldName(inputKey)}
        type="password"
        value={values[inputKey] ?? ''}
      />
    )
  }

  if (method.method.case === 'oauth') {
    return (
      <OAuthFields
        disabled={disabled}
        fields={oauthInputs(method.method.value)}
        inputKey={inputKey}
        onValueChange={onValueChange}
        values={values}
      />
    )
  }

  return null
}

function methodLabel(method: CatalogSourceCredentialMethod, index: number): string {
  if (method.label) return method.label
  if (method.method.case === 'sourceConfig') return 'Paste token'
  if (method.method.case === 'oauth') return 'OAuth'
  return `Method ${index + 1}`
}

interface OAuthInput extends OAuthField {
  required: boolean
}

function oauthInputs(oauth: CatalogOAuthCredentialMethod): OAuthInput[] {
  const out: OAuthInput[] = []
  const id = oauth.client?.id
  if (id?.input) {
    out.push({
      key: id.input,
      label: formatFieldName(id.input),
      secret: false,
      defaultValue: id.defaultValue,
      required: !id.defaultValue,
    })
  }
  const secret = oauth.client?.secret
  if (secret?.input) {
    out.push({
      key: secret.input,
      label: formatFieldName(secret.input),
      secret: true,
      required: true,
    })
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

function credentialMethodValueKeys(input: CatalogSourceInputSpec, methodIndex: number): string[] {
  if (input.input.case !== 'secret') return []

  const method = input.input.value.credential?.methods[methodIndex]
  if (!method || method.method.case === 'sourceConfig') return [input.key]
  if (method.method.case === 'oauth') {
    return oauthInputs(method.method.value).map((field) => field.key)
  }
  return []
}

function clearValues(values: Record<string, string>, keys: string[]): Record<string, string> {
  if (!keys.some((key) => key in values)) return values

  const next = { ...values }
  for (const key of keys) delete next[key]
  return next
}

function oauthInstallEndpoint(workspaceId: string, name: string): string {
  return `${routePath('workspaceSource', { sourceName: name, workspaceId })}/oauth-install`
}

function busyLabel(progress: InstallProgress, submitting: boolean): string {
  if (submitting || progress.kind === 'busy') return 'Adding…'
  if (progress.kind === 'awaiting-oauth') return 'Awaiting OAuth…'
  if (progress.kind === 'oauth-callback-received') return 'Exchanging token…'
  if (progress.kind === 'oauth-completed') return 'Finishing…'
  if (progress.kind === 'success') return 'Configured'
  return 'Add source'
}
