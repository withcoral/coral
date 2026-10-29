import { RequestHeaders } from './request-headers'
import { useEffect, useId, useRef, useState, type ReactNode } from 'react'
import { Form } from 'react-router'
import { Banner, Button, Card, Inputs, Table, Typography } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { CodeBlock } from '@/wax/components/code-block'
import { Picker } from '@/wax/components/picker'
import { Tooltip } from '@/wax/components/tooltip'
import { Fields } from '@/views/management/forms'
import {
  defaultAnswers,
  filledRecipe,
  refilled,
  serverInputs,
  withRecipe,
  type RecipeAnswers,
} from '@/lib/provider-recipes/fill-inputs'
import type {
  CredentialBindingValue,
  CredentialSourceValue,
  ConfigurationValues,
  FormContext,
  RecipeConfigurationView,
  SetupView,
  StepperResult,
} from './types'
import { bindingEditor, credentialLabel, type CredentialBindings } from './credential-bindings'
import {
  BASIC_PARTS,
  basicCredentialEditor,
  partLabel,
  type BasicCredentials as BasicCredentialsValue,
} from './basic-credentials'
import { OAuthApplicationBlock } from './oauth-application'
import {
  FieldError,
  NamespaceField,
  NAMESPACE,
  RequiredMark,
  suggestedNamespace,
  TextField,
} from './fields'
import {
  authenticationLabels,
  authenticationOptions,
  awaitingDiscovery,
  toolCount,
  serverLabel,
} from './configuration-details'
import { ScopesField } from './scopes-field'
import { ServerField } from './server-field'
import { serverProblem, type ServerGroup } from './server-choices'
import { StepFrame } from './step-frame'
import { ConfigurationChanges } from './configuration-changes'
import { DocumentSubmitButton } from '@/components/document-submit-button'
import * as styles from './providers.css'

/** Names that differ only in case or surrounding space read as the same provider. */
// The host's locale would let the server and the browser disagree, so case folds the same everywhere.
const nameKey = (name: string) => name.trim().toLowerCase()

/** A value the source fixed: shaped like the fields beside it, never editable. */
function ReadField({ label, value }: { label: string; value: string }) {
  const id = useId()
  return (
    <div className={styles.field} role="group" aria-label={label}>
      <Typography.BodySmallStrong as="label" htmlFor={id}>
        {label}
      </Typography.BodySmallStrong>
      <Inputs.TextInput id={id} value={value} disabled />
    </div>
  )
}

/**
 * A saved server the source no longer declares. Confirming cannot apply it, so
 * the field shows why and Remove clears it.
 */
function StaleServer({
  id,
  selection,
  onRemove,
}: {
  id: string
  selection: ConfigurationValues['servers'][number]
  onRemove: () => void
}) {
  const inputId = useId()
  const label = serverLabel(selection.declaration)
  return (
    <div className={styles.field} role="group" aria-label={label}>
      <Typography.BodySmallStrong as="label" htmlFor={inputId}>
        {label}
      </Typography.BodySmallStrong>
      <div className={styles.fieldWithAction}>
        <div className={styles.grow}>
          <Inputs.TextInput
            id={inputId}
            value={selection.url}
            readOnly
            invalid
            ariaDescribedBy={`${id}-error`}
          />
        </div>
        <Button.TextButton
          id={id}
          type="button"
          variant="secondary"
          ariaLabel={`Remove ${label}`}
          onClick={onRemove}
        >
          Remove
        </Button.TextButton>
      </div>
      <FieldError
        id={`${id}-error`}
        message="The source does not have this saved server. Remove it to continue."
      />
    </div>
  )
}

/** Why Next cannot confirm yet, and the field that fixes it. */
interface Blocker {
  field: string
  message: string
}

/**
 * Configuration confirms name, destinations and authentication. Application
 * secrets stay in uncontrolled document fields, never in the configuration draft.
 * The document's declared values are suggestions, not authority.
 */
export function ConfigurationStep({
  setup,
  form,
  pending,
  result,
  callbackUrl = '',
  namespace,
  onNamespace,
  defaultNamespace,
  takenNamespaces = [],
  takenDisplayNames = [],
  suggestedName,
  recipe = null,
  title = 'Setup',
  actions,
  confirmLabel = 'Next',
  compare = false,
  unchanged = false,
  retry,
  onDirtyChange,
}: {
  setup: SetupView
  form: FormContext
  pending: boolean
  result?: StepperResult
  callbackUrl?: string
  /** The workspace namespace programs call it by, confirmed here and posted with the configuration; absent for Update. */
  namespace?: string
  onNamespace?: (namespace: string) => void
  /** The namespace offered before anyone typed one, such as the recipe's service. */
  defaultNamespace?: string
  /** Namespaces other providers in the workspace have; one of them cannot be confirmed. */
  takenNamespaces?: readonly string[]
  /** Display names other providers in the workspace have; one of them cannot be confirmed. Absent for Update. */
  takenDisplayNames?: readonly string[]
  /** An entered or recipe-suggested name; undefined keeps the document's title. */
  suggestedName?: string
  /** What the chosen recipe fills in; it applies only before the first confirmation. */
  recipe?: RecipeConfigurationView | null
  title?: string
  /** Actions before the confirm button, such as Cancel. */
  actions?: ReactNode
  /** Save when confirming also publishes; Next when another step follows. */
  confirmLabel?: 'Next' | 'Save'
  /** Shows where a new read of the source differs from the saved values. */
  compare?: boolean
  /** Nothing to save yet, so the confirm button stays off. */
  unchanged?: boolean
  /** Reads the source again after Coral cannot read it. */
  retry?: ReactNode
  /** Reports whether the posted values differ from what the step first showed. */
  onDirtyChange?: (dirty: boolean) => void
}) {
  const formId = useId()
  const formRef = useRef<HTMLFormElement>(null)
  const baseline = useRef<string | null>(null)
  // Every field, controlled or not, posts through the form, so the posted
  // values are the one thing to compare.
  const checkDirty = () => {
    if (!onDirtyChange || !formRef.current) return
    const current = JSON.stringify(
      [...new FormData(formRef.current)].filter(
        ([name]) => !['csrf', 'requestId', 'intent'].includes(name),
      ),
    )
    baseline.current ??= current
    onDirtyChange(current !== baseline.current)
  }
  useEffect(checkDirty)
  const clientSecretId = useId()
  const fieldId = useId()
  const ids = {
    displayName: `${fieldId}-display-name`,
    namespace: `${fieldId}-namespace`,
    signIn: `${fieldId}-sign-in`,
    input: (name: string) => `${fieldId}-input-${name}`,
    server: (declaration: string) => `${fieldId}-server-${declaration}`,
  }
  const review = setup.configuration!
  const errors = result?.configurationErrors ?? review.errors
  const mcp = setup.kind === 'mcp'
  // A recipe fills in only a first configuration, never a confirmed one.
  const applied = review.confirmed ? null : recipe
  const [answers, setAnswers] = useState<RecipeAnswers>(
    () => result?.configurationDraft?.recipeInputs ?? (applied ? defaultAnswers(applied) : {}),
  )
  const filled = applied ? filledRecipe(applied, answers) : null
  // What was saved, or read from the source and filled in by the recipe.
  const origin = filled ? withRecipe(review.values, filled.values) : review.values
  const initial = result?.configurationDraft?.values ?? {
    ...origin,
    displayName: review.confirmed
      ? review.values.displayName
      : (suggestedName ?? review.values.displayName),
  }
  const [value, setValue] = useState<ConfigurationValues>(initial)
  // The namespace follows the display name until the person types their own.
  // A namespace that matches neither the offered one nor the name is their own.
  const [namespaceFollows, setNamespaceFollows] = useState(
    () =>
      !namespace ||
      namespace === defaultNamespace ||
      namespace === suggestedNamespace(initial.displayName),
  )
  const authorizationUrl = mcp
    ? value.authentication === 'device'
      ? (review.deviceAuthorizationUrl ?? '')
      : review.values.authorizationUrl
    : value.authorizationUrl
  const submission = { ...value, authorizationUrl }
  const update = (patch: Partial<ConfigurationValues>) =>
    setValue((current) => ({ ...current, ...patch }))
  const setServer = (declaration: string, selection: ConfigurationValues['servers'][number]) => {
    update({
      servers: [...value.servers.filter((server) => server.declaration !== declaration), selection],
    })
  }
  const changeAnswer = (name: string, answer: string) => {
    if (!applied || !filled) return
    const next = { ...answers, [name]: answer }
    const after = filledRecipe(applied, next)
    setAnswers(next)
    setValue((current) => refilled(current, review.values, filled.values, after.values))
  }
  const clearServer = (declaration: string) =>
    update({
      servers: value.servers.filter((server) => server.declaration !== declaration),
    })
  // A recipe's methods come first, as suggested; the rest follow as other.
  const suggested = applied?.suggestedMethods ?? []
  const options = authenticationOptions(review)
    .map((option) => ({
      value: option.value,
      label: option.label,
      ...(suggested.length > 0 && {
        group: suggested.includes(option.value) ? 'Suggested' : 'Other',
      }),
    }))
    .toSorted((a, b) => Number(a.group === 'Other') - Number(b.group === 'Other'))
  const source = mcp ? 'server' : 'document'
  const device = value.authentication === 'device'
  const oauth =
    (value.authentication === 'oauth' || device) &&
    review.authenticationMethods.includes(value.authentication as 'oauth' | 'device')
  const tools = toolCount(setup)
  const serverGroups = mcp ? [] : review.servers
  const recipeServer = (group: ServerGroup) =>
    applied?.values.servers?.find((server) => server.declaration === group.declaration)
  // A recipe server's URL stays empty until each input it uses is answered.
  const serverInputNames = new Set(
    serverGroups.flatMap((group) => {
      const server = recipeServer(group)
      return server ? serverInputs(server) : []
    }),
  )
  const staleServers = value.servers.filter(
    (selection) => !review.servers.some((group) => group.declaration === selection.declaration),
  )
  // A saved method the source no longer offers is an error on its field.
  const signInError =
    errors.authentication ??
    (value.authentication &&
    !review.authenticationMethods.some((method) => method === value.authentication)
      ? `This ${source} does not support ${authenticationLabels[value.authentication]}. Choose a supported method.`
      : undefined)
  const readFrom = `Read from ${setup.sourceHost || 'the document'}`
  const namespaceTaken = namespace !== undefined && takenNamespaces.includes(namespace)
  const displayNameTaken = takenDisplayNames.some(
    (name) => nameKey(name) === nameKey(value.displayName),
  )
  // Each reason Next cannot confirm, in the order the fields appear. Next names
  // the first one and moves focus to its field.
  const blockers: Blocker[] = []
  if (!value.displayName.trim())
    blockers.push({ field: ids.displayName, message: 'Enter a display name.' })
  else if (displayNameTaken)
    blockers.push({ field: ids.displayName, message: 'Enter a different display name.' })
  if (namespace !== undefined && !NAMESPACE.test(namespace))
    blockers.push({
      field: ids.namespace,
      message: namespace ? 'Correct the namespace.' : 'Enter a namespace.',
    })
  if (namespaceTaken)
    blockers.push({ field: ids.namespace, message: 'Enter a different namespace.' })
  // Confirming cannot fix a server the person has not answered, so the step
  // names the problem in the field instead of posting for the same answer.
  const serverProblems = serverGroups.flatMap((group) => {
    const problem = serverProblem(
      group,
      value.servers.find((server) => server.declaration === group.declaration),
    )
    if (!problem) return []
    // A server waiting for a recipe answer is fixed in that answer's field.
    const server = recipeServer(group)
    const missing = server ? serverInputs(server).filter((name) => !answers[name]?.trim()) : []
    return [{ group, problem, missing }]
  })
  const missingInputs = new Set(serverProblems.flatMap(({ missing }) => missing))
  // An input is required while a server waits for it, and stays marked once
  // answered. A blank one that no server waits for, because the person typed
  // their own URL or the server is optional, blocks nothing, so the browser
  // must not block the form for it either.
  const requiredInputs = new Set(
    [...serverInputNames].filter((name) => missingInputs.has(name) || answers[name]?.trim()),
  )
  const waitingFor = (group: ServerGroup) => {
    const missing = serverProblems.find((entry) => entry.group === group)?.missing ?? []
    return (applied?.inputs ?? [])
      .filter((input) => missing.includes(input.name))
      .map((input) => input.label)
  }
  for (const input of applied?.inputs ?? [])
    if (missingInputs.has(input.name))
      blockers.push({
        field: ids.input(input.name),
        message: `${input.choices.length > 0 ? 'Choose' : 'Enter'} a value for ${input.label}.`,
      })
  if (!review.authenticationMethods.some((method) => method === value.authentication))
    blockers.push({ field: ids.signIn, message: 'Choose a sign-in method.' })
  for (const selection of staleServers)
    blockers.push({
      field: ids.server(selection.declaration),
      message: 'Remove the saved server that the source does not have.',
    })
  for (const { group, problem, missing } of serverProblems)
    if (missing.length === 0)
      blockers.push({ field: ids.server(group.declaration), message: problem })
  const blocked = blockers.at(0)
  const serverFieldsShowErrors =
    staleServers.length > 0 ||
    serverProblems.length > 0 ||
    review.servers.some((group) => errors[group.declaration])
  const nextButton = (
    <DocumentSubmitButton
      type="submit"
      form={formId}
      disabled={pending || unchanged || blocked !== undefined}
      focusableWhenDisabled
      onClick={() => {
        if (pending || !blocked) return
        document.getElementById(blocked.field)?.focus()
      }}
    >
      {confirmLabel}
    </DocumentSubmitButton>
  )
  const confirm =
    blocked && !pending ? <Tooltip content={blocked.message}>{nextButton}</Tooltip> : nextButton
  // Nothing on the form can fix a source Coral cannot read.
  if (!review.readable)
    return (
      <StepFrame title={title} actions={actions}>
        <Banner variant="error" title={`Cannot read this ${source}`} action={retry}>
          Fix the {source} before you try again. Error code: {review.diagnostic}
        </Banner>
      </StepFrame>
    )
  return (
    <StepFrame
      title={title}
      actions={
        <>
          {actions}
          {confirm}
        </>
      }
    >
      <Form
        ref={formRef}
        id={formId}
        method="post"
        reloadDocument
        className={styles.stack}
        aria-busy={pending}
        onInput={checkDirty}
      >
        {compare && <ConfigurationChanges setup={setup} />}
        <Fields form={form} intent="configure" />
        <input type="hidden" name="setup" value={setup.id} />
        <input type="hidden" name="configurationRevision" value={review.revision} />
        <input type="hidden" name="configuration" value={JSON.stringify(submission)} />
        <div className={styles.configureSplit}>
          <div className={styles.stack}>
            <TextField
              id={ids.displayName}
              label="Display name"
              required
              value={value.displayName}
              onChange={(displayName) => {
                update({ displayName })
                if (namespaceFollows) onNamespace?.(suggestedNamespace(displayName))
              }}
              error={
                errors.display_name ??
                (displayNameTaken
                  ? 'Another provider uses this name. Enter a different display name.'
                  : undefined)
              }
              maxLength={256}
            />
            {namespace !== undefined && (
              <NamespaceField
                id={ids.namespace}
                value={namespace}
                onChange={(next) => {
                  setNamespaceFollows(false)
                  onNamespace?.(next)
                }}
                error={
                  namespaceTaken
                    ? 'Another provider uses this namespace. Enter a different namespace.'
                    : undefined
                }
              />
            )}
            {applied && applied.inputs.length > 0 && (
              <>
                {/* The recipe's own questions are fields like any other: each
                    carries its label and its guidance. */}
                <input type="hidden" name="recipeInputs" value={JSON.stringify(answers)} />
                {applied.inputs.map((input) =>
                  input.choices.length > 0 ? (
                    <div key={input.name} className={styles.field}>
                      <Typography.BodySmallStrong as="span">
                        {input.label}
                        {requiredInputs.has(input.name) && <RequiredMark />}
                      </Typography.BodySmallStrong>
                      <div className={styles.inlineControl}>
                        <Picker
                          id={ids.input(input.name)}
                          label={input.label}
                          value={answers[input.name] ?? ''}
                          options={input.choices.map((choice) => ({
                            label: choice,
                            value: choice,
                          }))}
                          onChange={(answer) => changeAnswer(input.name, answer)}
                        />
                      </div>
                      {input.description && (
                        <Typography.BodySmall as="p" variant="tertiary">
                          {input.description}
                        </Typography.BodySmall>
                      )}
                    </div>
                  ) : (
                    <TextField
                      key={input.name}
                      id={ids.input(input.name)}
                      required={requiredInputs.has(input.name)}
                      label={input.label}
                      description={input.description}
                      value={answers[input.name] ?? ''}
                      onChange={(answer) => changeAnswer(input.name, answer)}
                    />
                  ),
                )}
              </>
            )}
          </div>
          <Card.Container as="aside" className={styles.readColumn} aria-label="Source">
            <ReadField label={mcp ? 'MCP server URL' : 'Source'} value={setup.sourceLabel} />
          </Card.Container>
          <div className={styles.groupRule} role="presentation" />
          <div className={styles.fullRow}>
            <div className={styles.field} role="group" aria-label="Sign-in">
              <div className={styles.filterLabel}>
                <Typography.BodySmallStrong as="span">
                  Sign-in
                  <RequiredMark />
                </Typography.BodySmallStrong>
                <Tooltip content="How accounts sign in to this provider.">
                  <span className={styles.helpIcon} tabIndex={0} aria-label="Sign-in help">
                    <Icon name="Info" size="14" />
                  </span>
                </Tooltip>
              </div>
              <div className={styles.inlineControl}>
                <Picker
                  id={ids.signIn}
                  invalid={Boolean(signInError)}
                  ariaDescribedBy={signInError ? `${ids.signIn}-error` : undefined}
                  placeholder="Choose a method"
                  value={value.authentication}
                  options={options}
                  onChange={(next) =>
                    update({
                      authentication: next as ConfigurationValues['authentication'],
                    })
                  }
                />
              </div>
            </div>
            <FieldError id={`${ids.signIn}-error`} message={signInError} />
            {value.authentication === 'header' &&
              review.authenticationMethods.includes('header') && (
                <CredentialBindingsEditor
                  values={{
                    bindings: value.headers,
                    credentialFields: value.credentialFields,
                  }}
                  origin={{
                    bindings: origin.headers,
                    credentialFields: origin.credentialFields,
                  }}
                  update={(patch) =>
                    update({
                      headers: patch.bindings,
                      credentialFields: patch.credentialFields,
                    })
                  }
                  errors={errors}
                  spec={HEADER_BINDINGS}
                />
              )}
            {value.authentication === 'basic' && review.authenticationMethods.includes('basic') && (
              <BasicCredentials values={value} origin={origin} update={update} errors={errors} />
            )}
            {value.authentication === 'query' && review.authenticationMethods.includes('query') && (
              <CredentialBindingsEditor
                values={{
                  bindings: value.parameters,
                  credentialFields: value.credentialFields,
                }}
                origin={{
                  bindings: origin.parameters,
                  credentialFields: origin.credentialFields,
                }}
                update={(patch) =>
                  update({
                    parameters: patch.bindings,
                    credentialFields: patch.credentialFields,
                  })
                }
                errors={errors}
                spec={PARAMETER_BINDINGS}
              />
            )}
            {oauth && (
              <>
                {mcp ? (
                  <>
                    <FieldError message={errors['authentication.authorization_url']} />
                    <FieldError message={errors['authentication.token_url']} />
                  </>
                ) : (
                  <>
                    <Typography.BodySmallStrong as="h4">OAuth settings</Typography.BodySmallStrong>
                    <TextField
                      label={device ? 'Device authorization URL' : 'Authorization URL'}
                      error={
                        errors[
                          device
                            ? 'authentication.device_authorization_url'
                            : 'authentication.authorization_url'
                        ]
                      }
                      value={authorizationUrl}
                      onChange={(nextUrl) => update({ authorizationUrl: nextUrl })}
                    />
                    <TextField
                      label="Token URL"
                      error={errors['authentication.token_url']}
                      value={value.tokenUrl}
                      onChange={(tokenUrl) => update({ tokenUrl })}
                    />
                  </>
                )}
                <ScopesField
                  value={value.scopes}
                  onChange={(scopes) => update({ scopes })}
                  suggestions={[...new Set([...review.values.scopes, ...origin.scopes])]}
                  error={errors['authentication.scopes']}
                  format={value.scopeFormat ?? 'space'}
                  onFormatChange={(scopeFormat) => update({ scopeFormat })}
                  formatError={errors['authentication.scope_format']}
                />
              </>
            )}
          </div>
          <RequestHeaders
            className={styles.leftColumnWidth}
            value={value.requestHeaders ?? []}
            onChange={(requestHeaders) => update({ requestHeaders })}
            errors={errors}
          />
          {/* Only present cells: an empty one would add a grid row gap. */}
          {oauth && (
            <div className={styles.stack}>
              <OAuthApplicationBlock
                device={device}
                setup={setup}
                application={
                  result?.configurationDraft?.application ??
                  filled?.application ??
                  review.application
                }
                callbackUrl={callbackUrl}
                clientConfigHintMarkdown={applied?.clientConfigHintMarkdown}
                configuring
                clientSecretInput={
                  <>
                    <Typography.BodySmallStrong as="label" htmlFor={clientSecretId}>
                      Client secret
                    </Typography.BodySmallStrong>
                    <Inputs.CredentialInput
                      id={clientSecretId}
                      name="clientSecret"
                      required={!setup.clientSecretConfigured}
                    />
                  </>
                }
              />
            </div>
          )}
          {/* The endpoints the server publishes for this flow: shown beside the
              application that uses them, and never editable for an MCP server. */}
          {oauth && mcp && (
            <Card.Container
              as="aside"
              className={styles.readColumn}
              aria-label="OAuth settings from the server"
            >
              <>
                <Typography.BodySmall as="p" variant="tertiary">
                  {readFrom}
                </Typography.BodySmall>
                <ReadField
                  label={device ? 'Device authorization URL' : 'Authorization URL'}
                  value={authorizationUrl}
                />
                <ReadField label="Token URL" value={value.tokenUrl} />
                <ReadField label="Suggested scopes" value={review.values.scopes.join(' ')} />
                {review.issuer && <ReadField label="Issuer" value={review.issuer} />}
                {review.resource && <ReadField label="Resource" value={review.resource} />}
              </>
            </Card.Container>
          )}
          {(staleServers.length > 0 || (!mcp && review.servers.length > 0)) && (
            <div className={styles.fullRow}>
              {staleServers.map((selection) => (
                <StaleServer
                  key={selection.declaration}
                  id={ids.server(selection.declaration)}
                  selection={selection}
                  onRemove={() => clearServer(selection.declaration)}
                />
              ))}
              {!mcp && review.servers.length > 0 && (
                <section className={styles.stack} aria-label="Servers">
                  {/* Each field names the server it sets, so the group needs a
                    boundary rather than a title. */}
                  <div className={styles.groupRule} role="presentation" />
                  {review.servers.map((group) => (
                    <ServerField
                      key={group.declaration}
                      id={ids.server(group.declaration)}
                      waitingFor={waitingFor(group)}
                      group={group}
                      selection={value.servers.find(
                        (server) => server.declaration === group.declaration,
                      )}
                      error={errors[group.declaration]}
                      onChange={(selection) =>
                        selection
                          ? setServer(group.declaration, selection)
                          : clearServer(group.declaration)
                      }
                    />
                  ))}
                </section>
              )}
            </div>
          )}
        </div>
        {result?.error && <Banner variant="error">{result.error}</Banner>}
        {Object.entries(errors)
          .filter(
            ([field]) =>
              ![
                'display_name',
                'authentication',
                'authentication.authorization_url',
                'authentication.device_authorization_url',
                'authentication.token_url',
                'authentication.scopes',
              ].includes(field) &&
              !isCredentialsError(field) &&
              !review.servers.some((group) => group.declaration === field) &&
              // A server field that shows its own error already explains this one.
              !(field === 'servers' && serverFieldsShowErrors),
          )
          .map(([field, message]) => (
            <Banner key={field} variant="error">
              {message}
            </Banner>
          ))}
        {review.diagnostic && <Banner variant="warning">{review.diagnostic}</Banner>}
        {!awaitingDiscovery(setup) && tools === 0 && (
          <Banner title="No tools found">Check the {source}, or change the source.</Banner>
        )}
      </Form>
    </StepFrame>
  )
}

/** Errors a credential editor shows beside the value at fault. */
function isCredentialsError(field: string) {
  return (
    field.startsWith('authentication.headers') ||
    field.startsWith('authentication.parameters') ||
    field.startsWith('authentication.fields') ||
    field === 'authentication.username' ||
    field === 'authentication.password'
  )
}

/** What one credential method calls its bindings, and how it shows them. */
type BindingSpec = {
  /** The heading over the table, such as "Headers". */
  heading: string
  /** What one binding is called, such as "Header" or "Parameter". */
  noun: string
  namePlaceholder: string
  valuePlaceholder: string
  /** The error key the method reports a binding under. */
  errors: string
  /** Text before the value, which only a header sends. */
  prefix: boolean
  /** The line the preview shows for what Coral sends. */
  preview: (sent: { name: string; value: string }[]) => string
}

/**
 * What Coral sends in each credential header or query parameter: a credential
 * each account enters when it connects, or a fixed value. A document or recipe
 * fills this in; an owner can also write it here.
 */
function CredentialBindingsEditor({
  values,
  origin,
  update,
  errors,
  spec,
}: {
  values: CredentialBindings
  /** The bindings and credentials as saved or filled in, whose names never change. */
  origin: CredentialBindings
  update: (patch: CredentialBindings) => void
  errors: Record<string, string>
  spec: BindingSpec
}) {
  const editor = bindingEditor(origin, { prefix: spec.prefix })
  const { bindings } = values
  // Each problem names its binding, so the table keeps one height per row.
  const problems = bindings.flatMap((binding) =>
    [
      errors[`${spec.errors}.${binding.name}`],
      binding.source === 'field' ? errors[`authentication.fields.${binding.field}`] : undefined,
    ].flatMap((message) =>
      message ? [`${binding.name || `New ${spec.noun.toLowerCase()}`}: ${message}`] : [],
    ),
  )
  return (
    <div className={styles.stack}>
      <Typography.BodySmallStrong as="h4">{spec.heading}</Typography.BodySmallStrong>
      <Table.Container
        ariaLabel={spec.heading}
        variant="card"
        className={styles.credentialTable}
        columns={[
          { label: spec.noun, width: 'minmax(160px, 2fr)' },
          { label: 'Type', width: 'content' },
          { label: 'Value', width: 'minmax(160px, 2fr)' },
          ...(spec.prefix ? [{ label: 'Prefix', width: 'minmax(96px, 1fr)' }] : []),
          { ariaLabel: 'Actions', width: 44 },
        ]}
      >
        <Table.Head />
        <Table.Body>
          {bindings.map((binding, index) => (
            // Keyed by place, not name, so typing a name keeps the row and its focus.
            <Table.Row key={index}>
              <Table.Cell control>
                <Inputs.TextInput
                  bare
                  ariaLabel={spec.noun}
                  value={binding.name}
                  maxLength={256}
                  placeholder={spec.namePlaceholder}
                  invalid={Boolean(errors[`${spec.errors}.${binding.name}`])}
                  onChange={(name) =>
                    update(editor.setName(values, index, spec.prefix ? name.toLowerCase() : name))
                  }
                />
              </Table.Cell>
              <Table.Cell control>
                <Picker
                  ariaLabel="Type"
                  value={binding.source}
                  options={[
                    { value: 'field', label: 'Credential' },
                    { value: 'literal', label: 'Fixed value' },
                  ]}
                  onChange={(source) =>
                    update(
                      editor.setSource(values, index, source as CredentialBindingValue['source']),
                    )
                  }
                />
              </Table.Cell>
              <Table.Cell control>
                {binding.source === 'field' ? (
                  <Inputs.TextInput
                    bare
                    ariaLabel="Credential label"
                    value={credentialLabel(values, binding)}
                    maxLength={120}
                    placeholder="API key"
                    invalid={Boolean(errors[`authentication.fields.${binding.field}`])}
                    onChange={(label) => update(editor.setLabel(values, index, label))}
                  />
                ) : (
                  <Inputs.TextInput
                    bare
                    ariaLabel="Value"
                    value={binding.value}
                    maxLength={4096}
                    placeholder={spec.valuePlaceholder}
                    onChange={(value) => update(editor.setValue(values, index, value))}
                  />
                )}
              </Table.Cell>
              {spec.prefix && (
                <Table.Cell control>
                  {binding.source === 'field' && (
                    <Inputs.TextInput
                      bare
                      ariaLabel="Prefix"
                      value={binding.prefix ?? ''}
                      maxLength={256}
                      placeholder="None"
                      onChange={(prefix) => update(editor.setPrefix(values, index, prefix))}
                    />
                  )}
                </Table.Cell>
              )}
              <Table.Cell actions>
                <Button.IconButton
                  name="Trash"
                  variant="bare"
                  tooltipText={`Remove ${binding.name || spec.noun.toLowerCase()}`}
                  onClick={() => update(editor.remove(values, index))}
                />
              </Table.Cell>
            </Table.Row>
          ))}
          <Table.AddRow onClick={() => update(editor.add(values))}>
            Add {spec.noun.toLowerCase()}
          </Table.AddRow>
        </Table.Body>
      </Table.Container>
      <FieldError message={errors[spec.errors]} />
      <FieldError message={errors['authentication.fields']} />
      {problems.map((message, index) => (
        <FieldError key={index} message={message} />
      ))}
      {bindings.length > 0 && (
        <div className={styles.headerPreview}>
          <Typography.BodySmallStrong as="span">Preview</Typography.BodySmallStrong>
          <CodeBlock
            framed
            value={spec.preview(
              bindings.map((binding) => ({
                name: binding.name || '\u2026',
                value:
                  binding.source === 'field'
                    ? `${binding.prefix ?? ''}\u2039${credentialLabel(values, binding) || 'credential'}\u203a`
                    : binding.value,
              })),
            )}
          />
        </div>
      )}
    </div>
  )
}

/** Headers carry a prefix; query parameters join the URL as it leaves. */
const HEADER_BINDINGS: BindingSpec = {
  heading: 'Headers',
  noun: 'Header',
  namePlaceholder: 'x-api-key',
  valuePlaceholder: '2024-01',
  errors: 'authentication.headers',
  prefix: true,
  preview: (sent) => sent.map(({ name, value }) => `${name}: ${value}`).join('\n'),
}

const PARAMETER_BINDINGS: BindingSpec = {
  heading: 'Query parameters',
  noun: 'Parameter',
  namePlaceholder: 'api_key',
  valuePlaceholder: '2024-01',
  errors: 'authentication.parameters',
  prefix: false,
  preview: (sent) => `?${sent.map(({ name, value }) => `${name}=${value}`).join('&')}`,
}

/**
 * The username and password Coral encodes as HTTP Basic. Each one is a
 * credential an account enters, or a fixed value such as Mailgun's username
 * `api`. A document or recipe fills this in; an owner can also write it here.
 */
function BasicCredentials({
  values,
  origin,
  update,
  errors,
}: {
  values: ConfigurationValues
  /** The parts and credentials as saved or filled in, whose names never change. */
  origin: BasicCredentialsValue
  update: (patch: BasicCredentialsValue) => void
  errors: Record<string, string>
}) {
  const editor = basicCredentialEditor(origin)
  return (
    <div className={styles.stack}>
      <Typography.BodySmallStrong as="h4">Username and password</Typography.BodySmallStrong>
      <Table.Container
        ariaLabel="Username and password"
        variant="card"
        className={styles.credentialTable}
        columns={[
          { label: 'Part', width: 'content' },
          { label: 'Type', width: 'content' },
          { label: 'Value', width: 'minmax(160px, 2fr)' },
        ]}
      >
        <Table.Head />
        <Table.Body>
          {BASIC_PARTS.map(([part, label]) => {
            const source = values.basic[part]
            return (
              <Table.Row key={part}>
                <Table.Cell>
                  <Typography.BodySmall as="span">{label}</Typography.BodySmall>
                </Table.Cell>
                <Table.Cell control>
                  <Picker
                    ariaLabel={`${label} type`}
                    value={source.source}
                    options={[
                      { value: 'field', label: 'Credential' },
                      { value: 'literal', label: 'Fixed value' },
                    ]}
                    onChange={(next) =>
                      update(
                        editor.setSource(values, part, next as CredentialSourceValue['source']),
                      )
                    }
                  />
                </Table.Cell>
                <Table.Cell control>
                  {source.source === 'field' ? (
                    <Inputs.TextInput
                      bare
                      ariaLabel={`${label} credential`}
                      value={partLabel(values, part)}
                      maxLength={120}
                      placeholder={label}
                      invalid={Boolean(errors[`authentication.fields.${source.field}`])}
                      onChange={(text) => update(editor.setLabel(values, part, text))}
                    />
                  ) : part === 'password' ? (
                    // A constant password would keep a secret here, so Coral sends none.
                    <Typography.BodySmall as="span">No password</Typography.BodySmall>
                  ) : (
                    <Inputs.TextInput
                      bare
                      ariaLabel={`${label} value`}
                      value={source.value}
                      maxLength={4096}
                      placeholder="api"
                      onChange={(text) => update(editor.setValue(values, part, text))}
                    />
                  )}
                </Table.Cell>
              </Table.Row>
            )
          })}
        </Table.Body>
      </Table.Container>
      <FieldError message={errors['authentication.fields']} />
      {BASIC_PARTS.map(([part]) => (
        <FieldError key={part} message={errors[`authentication.${part}`]} />
      ))}
      <div className={styles.headerPreview}>
        <Typography.BodySmallStrong as="span">Preview</Typography.BodySmallStrong>
        <CodeBlock
          framed
          value={`authorization: Basic base64(${BASIC_PARTS.map(([part]) => {
            const source = values.basic[part]
            return source.source === 'field'
              ? `\u2039${partLabel(values, part) || 'credential'}\u203a`
              : source.value
          }).join(':')})`}
        />
      </div>
    </div>
  )
}
