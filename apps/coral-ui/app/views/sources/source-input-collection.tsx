import { useMemo, useState } from 'react'

import { Tabs } from '@/wax/components'
import { TextInput } from '@/wax/components/inputs/text'

import { Markdown } from '@/components/markdown'
import { OAuthFields, type OAuthField } from '@/components/sources/install/oauth-fields'
import { oauthClientInputs } from '@/lib/source-install-form'
import type {
  CatalogOAuthCredentialMethod,
  CatalogSourceCredentialMethod,
  CatalogSourceInputSpec,
} from '@/lib/sources'

import * as styles from './source-install.css'
import {
  formatFieldName,
  SourceError,
  SourceInputField,
  SourceNoConfiguration,
} from './source-presentation'

/**
 * Collects values for one source's declared inputs. Bundled installs and imported
 * manifests both describe their inputs the same way, so they share this state and
 * the field names it submits.
 */
export interface SourceInputCollection {
  canSubmit: boolean
  changeMethod: (input: CatalogSourceInputSpec, index: number) => void
  effectiveChoice: (input: CatalogSourceInputSpec) => number
  inputSpecs: CatalogSourceInputSpec[] | null
  inputs: CatalogSourceInputSpec[]
  setValue: (key: string, value: string) => void
  usesOAuth: boolean
  values: Record<string, string>
}

export function useSourceInputCollection(
  inputSpecs: CatalogSourceInputSpec[] | null,
): SourceInputCollection {
  const [values, setValues] = useState<Record<string, string>>({})
  const [methodChoices, setMethodChoices] = useState<Record<string, number>>({})
  const inputs: CatalogSourceInputSpec[] = inputSpecs ?? []

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

  function setValue(key: string, value: string) {
    setValues((previous) => ({ ...previous, [key]: value }))
  }

  function changeMethod(input: CatalogSourceInputSpec, index: number) {
    const previousIndex = effectiveChoice(input)
    if (index === previousIndex) return

    const keys = credentialMethodValueKeys(input, previousIndex)
    setValues((previous) => clearValues(previous, keys))
    setMethodChoices((previous) => ({ ...previous, [input.key]: index }))
  }

  return {
    canSubmit,
    changeMethod,
    effectiveChoice,
    inputSpecs,
    inputs,
    setValue,
    usesOAuth,
    values,
  }
}

export function SourceInputRows({
  collection,
  disabled,
}: {
  collection: SourceInputCollection
  disabled: boolean
}) {
  const { changeMethod, effectiveChoice, inputSpecs, inputs, setValue, values } = collection

  if (!inputSpecs) return <SourceError>Source metadata is unavailable.</SourceError>
  if (inputs.length === 0) return <SourceNoConfiguration />

  return (
    <div className={styles.fieldGroup}>
      {inputs.map((input) => (
        <InputRow
          key={input.key}
          input={input}
          methodIndex={effectiveChoice(input)}
          values={values}
          disabled={disabled}
          onValueChange={setValue}
          onMethodChange={(index) => changeMethod(input, index)}
        />
      ))}
    </div>
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
    <SourceInputField input={input} showHint={methods.length === 0} showLabel={methods.length <= 1}>
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
                  hint={method.hint || method.description || input.hint}
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
                  hint={method.hint || method.description || input.hint}
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
        <CredentialMethodContent
          disabled={disabled}
          hint={selected ? selected.hint || selected.description || input.hint : ''}
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
        fields={oauthFields(method.method.value)}
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

/** Labels the shared client-input rule for display. */
function oauthFields(oauth: CatalogOAuthCredentialMethod): OAuthField[] {
  return oauthClientInputs(oauth).map((input) => ({
    ...input,
    label: formatFieldName(input.key),
  }))
}

function oauthMethodReady(
  oauth: CatalogOAuthCredentialMethod,
  values: Record<string, string>,
): boolean {
  return oauthClientInputs(oauth).every((input) => {
    if (!input.required) return true
    return (values[input.key] ?? input.defaultValue ?? '').trim().length > 0
  })
}

function credentialMethodValueKeys(input: CatalogSourceInputSpec, methodIndex: number): string[] {
  if (input.input.case !== 'secret') return []

  const method = input.input.value.credential?.methods[methodIndex]
  if (!method || method.method.case === 'sourceConfig') return [input.key]
  if (method.method.case === 'oauth') {
    return oauthClientInputs(method.method.value).map((field) => field.key)
  }
  return []
}

function clearValues(values: Record<string, string>, keys: string[]): Record<string, string> {
  if (!keys.some((key) => key in values)) return values

  const next = { ...values }
  for (const key of keys) delete next[key]
  return next
}
