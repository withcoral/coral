import { useId, useRef, useState, type ReactNode } from 'react'
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
import { Banner, Button, Card, Inputs, Radio, Typography } from '@/wax/components'
import { Disclosure } from '@/wax/components/disclosure'
import { Picker } from '@/wax/components/picker'
import { SettingsRemoval } from '@/wax/components/settings-layout'
import {
  baseUrlHints,
  protocolLabels,
  serviceLabels,
  servicePresets,
  requestArgNames,
} from './copy'
import { ModelTestOutcome, testRefusal, useModelTest } from './model-test'
import {
  type KeyChange,
  type ModelValues,
  type Protocol,
  PROTOCOLS,
  type Service,
  SERVICES,
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
            ? 'A Coral agent that your agents can query in natural language. Workspace owners turn it on from the Tools page.'
            : 'A Coral agent that your agents can query in natural language. Set the LLM that it uses.'
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
      {model && <RemoveModel model={model} form={form} />}
    </ManagementLayout>
  )
}

function RemoveModel({ model, form }: { model: SubAgentModelView; form: FormContext }) {
  // As the form does, it stays on the model that the page opened with.
  const [saved] = useState(model)
  return (
    <SettingsRemoval
      title="Remove model"
      description="Remove the model and its API key."
      action={
        <ActionDialog
          title="Remove model"
          description="This removes the model and its API key. The sub-agent stops in every workspace. You cannot undo this."
          form={form}
          intent="remove"
          etag={saved.etag}
          destructive
        />
      }
    />
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

/** The service whose protocol and base URL these are. */
function serviceOf(values: ModelValues): Service {
  const known = SERVICES.find(
    (service) =>
      service !== 'other' &&
      servicePresets[service].protocol === values.protocol &&
      servicePresets[service].baseUrl === destination(values.baseUrl),
  )
  return known ?? 'other'
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
  // A refused URL, and a conflict, are about fields that only `other` shows.
  const [service, setService] = useState<Service>(
    refused?.conflict || refused?.field === 'baseUrl' ? 'other' : serviceOf(values),
  )
  const id = useId()
  const test = useModelTest()
  const refusal = test.tried ? testRefusal(test.result) : refused
  const refusedAdvanced = refusal?.field === 'contextWindow' || refusal?.field === 'requestArgs'
  // Closed unless it holds something to correct or compare. Kept, so that it does not close over a
  // person who corrects it.
  const [advanced, setAdvanced] = useState({
    open: Boolean(refused?.conflict || refusedAdvanced),
    test: test.number,
  })
  // A test that refuses a setting in it opens it anew, though the person closed it.
  if (refusedAdvanced && advanced.test !== test.number)
    setAdvanced({ open: true, test: test.number })
  const testButton = useRef<HTMLButtonElement>(null)
  // Not every control raises an input event, which the form clears a test on.
  const update = (patch: Partial<ModelValues>) => {
    setValues((v) => ({ ...v, ...patch }))
    test.clear()
  }

  const moved = saved !== null && destination(values.baseUrl) !== destination(saved.values.baseUrl)
  // Coral sends a saved key only to the base URL it was saved with.
  const keepable = saved !== null && saved.hasApiKey && !moved
  // A known service always takes a key. Another server may not.
  const keyChoices: KeyChange[] = keepable
    ? ['keep', 'replace', 'clear']
    : service === 'other'
      ? ['optional', 'replace', 'clear']
      : ['replace']
  const keyDefault = keepable
    ? 'keep'
    : service !== 'other' || saved?.hasApiKey
      ? 'replace'
      : 'optional'
  const keyChange = keyChoice && keyChoices.includes(keyChoice) ? keyChoice : keyDefault
  const chooseKey = (next: KeyChange) => {
    setKeyChoice(next)
    test.clear()
  }
  const chooseService = (next: Service) => {
    setService(next)
    if (next !== 'other') update(servicePresets[next])
    // No host is entered for you: it is where the data goes.
    else update({ protocol: NEW_MODEL.protocol, baseUrl: NEW_MODEL.baseUrl })
  }

  /** One field's ids and error, for its control and for the text around it. */
  const control = (name: RefusableField) => {
    const error = refusal?.field === name ? refusal.message : undefined
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
    <Form
      method="post"
      action="."
      reloadDocument
      className={styles.form}
      onInput={test.clear}
      onSubmit={({ nativeEvent }) => {
        if ((nativeEvent as SubmitEvent).submitter === testButton.current) test.start()
      }}
    >
      <Fields form={form} intent="save" etag={saved?.etag} />
      <div className={styles.field}>
        <Typography.BodySmallStrong as="span">Service</Typography.BodySmallStrong>
        <Radio.Group
          aria-label="Service"
          layout="row"
          value={service}
          onValueChange={(next) => chooseService(next)}
        >
          {SERVICES.map((value) => (
            <Radio.Item key={value} value={value}>
              {serviceLabels[value]}
            </Radio.Item>
          ))}
        </Radio.Group>
      </div>
      <input type="hidden" name="protocol" value={values.protocol} />
      {service === 'other' ? (
        <>
          <div className={styles.field}>
            <Typography.BodySmallStrong as="span">Protocol</Typography.BodySmallStrong>
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
        </>
      ) : (
        <div className={styles.field}>
          <Typography.BodySmallStrong as="span">Base URL</Typography.BodySmallStrong>
          <Typography.BodySmall as="p">{values.baseUrl}</Typography.BodySmall>
          <FieldError id={`${id}-baseUrl-error`} message={baseUrl.field.error} />
          <input type="hidden" name="baseUrl" value={values.baseUrl} />
        </div>
      )}
      {moved && (
        <Banner variant="warning">
          Saving a new base URL blocks the sub-agent in every workspace.
        </Banner>
      )}
      <Field
        {...modelName.field}
        label="Model name"
        required
        hint="Enter the name that the service uses."
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
      <input type="hidden" name="keyChange" value={keyChange} />
      {keyChange === 'replace' || keyChange === 'optional' ? (
        <Field
          {...apiKey.field}
          label="API key"
          required={keyChange === 'replace'}
          hint={
            // A refused save leaves the key behind, so a person who typed one types it again.
            refused?.keyChange === 'replace' && !apiKey.field.error
              ? 'Enter the key again.'
              : saved?.hasApiKey && moved
                ? 'The saved key is for the saved base URL only. Enter the key again.'
                : keyChange === 'optional'
                  ? 'Leave this field empty if the server takes no key. You cannot see the key after you save it.'
                  : 'You cannot see the key after you save it.'
          }
        >
          <Inputs.CredentialInput {...apiKey.input} required={keyChange === 'replace'} />
          {keyChange === 'replace' && keyChoices.length > 1 && (
            <KeyButton onClick={() => chooseKey(keepable ? 'keep' : 'clear')}>
              {keepable ? 'Keep the saved key' : 'Use no key'}
            </KeyButton>
          )}
        </Field>
      ) : (
        <div className={styles.field} role="group" aria-label="API key">
          <Typography.BodySmallStrong as="span">API key</Typography.BodySmallStrong>
          <div className={styles.saved}>
            <Typography.BodySmall as="p">
              {keyChange === 'keep'
                ? 'A key is saved.'
                : keepable
                  ? 'Save to remove the key.'
                  : 'No key.'}
            </Typography.BodySmall>
            {keyChange === 'keep' ? (
              <>
                <KeyButton onClick={() => chooseKey('replace')}>Replace</KeyButton>
                <KeyButton onClick={() => chooseKey('clear')}>Remove</KeyButton>
              </>
            ) : (
              <KeyButton onClick={() => chooseKey(keepable ? 'keep' : 'replace')}>
                {keepable ? 'Keep the saved key' : 'Enter a key'}
              </KeyButton>
            )}
          </div>
        </div>
      )}
      <Disclosure key={`advanced-${advanced.test}`} title="Advanced" open={advanced.open}>
        <div className={styles.advanced}>
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
        </div>
      </Disclosure>
      <div className={styles.actions}>
        <DocumentSubmitButton>Save</DocumentSubmitButton>
        <Button.TextButton
          ref={testButton}
          type="submit"
          variant="secondary"
          busy={test.testing}
          {...test.button}
        >
          Test
        </Button.TextButton>
        {test.testing && <Typography.BodySmall as="span">Testing the model…</Typography.BodySmall>}
      </div>
      {test.result && <ModelTestOutcome result={test.result} />}
      {test.frame}
    </Form>
  )
}

/** Changes what a save does to the API key. */
function KeyButton({ onClick, children }: { onClick: () => void; children: ReactNode }) {
  return (
    <Button.TextButton type="button" variant="secondary" size="22" onClick={onClick}>
      {children}
    </Button.TextButton>
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
