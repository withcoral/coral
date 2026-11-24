import { useId, useState, type ReactNode } from 'react'
import { Form } from 'react-router'
import { CatalogToolbar } from '@/components/catalog'
import { DocumentSubmitButton } from '@/components/document-submit-button'
import { useArrivalToast } from '@/lib/use-arrival-toast'
import { ActionDialog, Fields } from '@/views/management/forms'
import { ManagementLayout, trail } from '@/views/management/layout'
import { personLabel } from '@/views/management/person'
import { SavedAt } from '@/views/management/saved-at'
import type { FormContext } from '@/views/management/types'
import { FieldError, RequiredMark } from '@/views/providers/fields'
import { Banner, Card, Inputs, Radio, Typography } from '@/wax/components'
import { Picker } from '@/wax/components/picker'
import { SettingsRemoval } from '@/wax/components/settings-layout'
import { baseUrlHints, protocolLabels, requestArgNames } from './copy'
import {
  type KeyChange,
  type ModelValues,
  type Protocol,
  PROTOCOLS,
  type RefusableField,
  type RefusedSave,
  type SubAgentModelView,
} from './types'
import * as styles from './sub-agent.css'

const ARRIVALS = ['saved', 'removed'] as const
const arrivalToast = (arrival: (typeof ARRIVALS)[number]) => ({
  title: arrival === 'saved' ? 'Model saved' : 'Model removed',
})

/** The model that the organization's sub-agents use. Only an organization owner opens it. */
export function SubAgentPage({
  model,
  form,
  refused,
}: {
  /** Null while the organization has none. */
  model: SubAgentModelView | null
  form: FormContext
  /** The save that opened this page, when Coral refused it. */
  refused?: RefusedSave
}) {
  useArrivalToast(ARRIVALS, arrivalToast)
  return (
    <ManagementLayout breadcrumbs={trail([], 'Sub-agent')} titled compact>
      <CatalogToolbar title="Sub-agent" />
      {refused && !refused.field && (
        <Banner variant="error" title="Coral did not save the model">
          {refused.message}
        </Banner>
      )}
      <Card.Card
        title="Model"
        description={
          model
            ? 'The sub-agents in this organization use this model.'
            : 'Set the model that the sub-agents in this organization use.'
        }
      >
        {model && (
          <Typography.BodySmall as="p" variant="tertiary">
            Last changed by {model.changedBy ? personLabel(model.changedBy) : 'a former member'} on{' '}
            <SavedAt at={model.changedAt} />.
          </Typography.BodySmall>
        )}
        {/* A removed model leaves an empty form. */}
        <ModelForm key={model ? 'set' : 'unset'} model={model} form={form} refused={refused} />
      </Card.Card>
      {model && (
        <SettingsRemoval
          title="Remove model"
          description="Remove the model and its API key."
          action={
            <ActionDialog
              title="Remove model"
              description="This removes the model and its API key. The sub-agent stops in every workspace. You cannot undo this."
              form={form}
              intent="remove"
              etag={model.etag}
              destructive
            />
          }
        />
      )}
    </ManagementLayout>
  )
}

const NEW_MODEL: ModelValues = {
  protocol: 'chat_completions',
  baseUrl: '',
  model: '',
  contextWindow: '',
  requestArgs: '',
}

/**
 * The destination that a base URL names. A slash at the end and space around the URL make no
 * difference, as in Coral. Coral also ignores other spellings, such as the case of the host.
 * There, the form asks for a key that Coral would have kept, and never the reverse.
 */
function destination(baseUrl: string) {
  return baseUrl.trim().replace(/\/+$/, '')
}

/**
 * Every setting in one document form, so that the API key never enters router state. A refused
 * save reloads the page with what the person typed, except the key, which no response carries.
 */
function ModelForm({
  model,
  form,
  refused,
}: {
  model: SubAgentModelView | null
  form: FormContext
  refused?: RefusedSave
}) {
  // A removal that fails reloads the page's data. The form stays on the model it opened with,
  // so its next save cannot replace a change that the person did not see.
  const [saved] = useState(model)
  const [values, setValues] = useState(refused?.values ?? saved?.values ?? NEW_MODEL)
  const [keyChoice, setKeyChoice] = useState<KeyChange | null>(refused?.keyChange ?? null)
  const id = useId()
  const update = (patch: Partial<ModelValues>) => setValues((v) => ({ ...v, ...patch }))

  const moved = saved !== null && destination(values.baseUrl) !== destination(saved.values.baseUrl)
  // Coral sends a saved key only to the base URL it was saved with.
  const keepable = saved !== null && saved.hasApiKey && !moved
  const keyLabels: Partial<Record<KeyChange, string>> = keepable
    ? {
        keep: 'Keep the saved key',
        replace: 'Replace the saved key',
        clear: 'Clear the saved key',
      }
    : { replace: 'Enter a key', clear: 'Use no key' }
  const keyDefault = keepable ? 'keep' : saved?.hasApiKey === false ? 'clear' : 'replace'
  const keyChange = keyChoice && keyChoice in keyLabels ? keyChoice : keyDefault

  /** One field's ids and error, for its control and for the text around it. */
  const control = (name: RefusableField) => {
    const error = refused?.field === name ? refused.message : undefined
    const fieldId = `${id}-${name}`
    return {
      field: { id: fieldId, error },
      input: {
        id: fieldId,
        name,
        invalid: Boolean(error),
        // The page of a refused save opens at the field to correct.
        autoFocus: Boolean(error),
        ariaDescribedBy: error ? `${fieldId}-error ${fieldId}-hint` : `${fieldId}-hint`,
      },
    }
  }
  /** What is saved now, where it differs from what the person typed before the model changed. */
  const savedNow = (name: keyof ModelValues) =>
    refused?.conflict && saved && saved.values[name] !== values[name]
      ? name === 'protocol'
        ? protocolLabels[saved.values.protocol]
        : saved.values[name]
      : undefined

  const baseUrl = control('baseUrl')
  const modelName = control('model')
  const apiKey = control('apiKey')
  const contextWindow = control('contextWindow')
  const requestArgs = control('requestArgs')
  return (
    // The path without its query, so that a refused save does not arrive as a saved one.
    <Form method="post" action="." reloadDocument className={styles.form}>
      <Fields form={form} intent="save" etag={saved?.etag} />
      <div className={styles.field}>
        <Typography.BodySmallStrong as="span">Protocol</Typography.BodySmallStrong>
        <input type="hidden" name="protocol" value={values.protocol} />
        <div className={styles.picker}>
          <Picker
            ariaLabel={`Protocol: ${protocolLabels[values.protocol]}`}
            options={PROTOCOLS.map((value) => ({ value, label: protocolLabels[value] }))}
            value={values.protocol}
            onChange={(protocol) => update({ protocol: protocol as Protocol })}
          />
        </div>
        <SavedNow value={savedNow('protocol')} />
      </div>
      <Field
        {...baseUrl.field}
        label="Base URL"
        required
        hint={baseUrlHints[values.protocol]}
        savedNow={savedNow('baseUrl')}
      >
        <Inputs.TextInput
          {...baseUrl.input}
          required
          maxLength={2048}
          spellCheck={false}
          value={values.baseUrl}
          onChange={(next) => update({ baseUrl: next })}
        />
      </Field>
      {moved && (
        <Banner variant="warning">
          Saving a new base URL turns the sub-agent off in every workspace.
        </Banner>
      )}
      <Field
        {...modelName.field}
        label="Model name"
        required
        hint="Enter the name that the endpoint uses."
        savedNow={savedNow('model')}
      >
        <Inputs.TextInput
          {...modelName.input}
          required
          maxLength={200}
          autoComplete="off"
          spellCheck={false}
          value={values.model}
          onChange={(next) => update({ model: next })}
        />
      </Field>
      <div className={styles.field}>
        <Typography.BodySmallStrong as="span">API key</Typography.BodySmallStrong>
        {saved && (
          <Typography.BodySmall as="p">
            {!saved.hasApiKey
              ? 'No API key is set.'
              : moved
                ? 'The saved key is for the saved base URL only. Enter the key again.'
                : 'An API key is set.'}
          </Typography.BodySmall>
        )}
        <input type="hidden" name="keyChange" value={keyChange} />
        <Radio.Group
          aria-label="API key"
          layout="stack"
          value={keyChange}
          onValueChange={(next) => setKeyChoice(next)}
        >
          {Object.entries(keyLabels).map(([value, label]) => (
            <Radio.Item key={value} value={value as KeyChange}>
              {label}
            </Radio.Item>
          ))}
        </Radio.Group>
      </div>
      {keyChange === 'replace' && (
        <Field
          {...apiKey.field}
          label="Key to save"
          required
          hint={
            // A refused save leaves the key behind, so a person who typed one types it again.
            refused?.keyChange === 'replace' && !apiKey.field.error
              ? 'Enter the key again.'
              : 'You cannot see the key after you save it.'
          }
        >
          <Inputs.CredentialInput {...apiKey.input} />
        </Field>
      )}
      <Field
        {...contextWindow.field}
        label="Context window (tokens)"
        hint="Enter 48,000 or more. Leave this field empty to use 128,000."
        savedNow={savedNow('contextWindow')}
      >
        <Inputs.TextInput
          {...contextWindow.input}
          autoComplete="off"
          value={values.contextWindow}
          onChange={(next) => update({ contextWindow: next })}
        />
      </Field>
      <Field
        {...requestArgs.field}
        label="Arguments (JSON)"
        hint={`Enter a JSON object to add to each model request. Use only these names: ${requestArgNames.join(', ')}.`}
        savedNow={savedNow('requestArgs')}
      >
        <Inputs.TextArea
          {...requestArgs.input}
          monospace
          rows={5}
          value={values.requestArgs}
          onChange={(next) => update({ requestArgs: next })}
        />
      </Field>
      <div>
        <DocumentSubmitButton>Save</DocumentSubmitButton>
      </div>
    </Form>
  )
}

/** A control with its label above it, and its error and guidance below. */
function Field({
  id,
  label,
  required = false,
  error,
  hint,
  savedNow,
  children,
}: {
  id: string
  label: string
  required?: boolean
  error?: string
  hint: string
  savedNow?: string
  children: ReactNode
}) {
  return (
    <div className={styles.field}>
      <Typography.BodySmallStrong as="label" htmlFor={id}>
        {label}
        {required && <RequiredMark />}
      </Typography.BodySmallStrong>
      {children}
      <FieldError id={`${id}-error`} message={error} />
      <Typography.BodySmall as="p" id={`${id}-hint`} variant="tertiary">
        {hint}
      </Typography.BodySmall>
      <SavedNow value={savedNow} />
    </div>
  )
}

function SavedNow({ value }: { value?: string }) {
  if (value === undefined) return null
  return (
    <Typography.BodySmall as="p" className={styles.savedNow}>
      Saved value: {value || 'none'}
    </Typography.BodySmall>
  )
}
