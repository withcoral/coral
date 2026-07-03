import { useMemo, useState } from 'react'
import type React from 'react'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { Icon as ButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { addToast } from '@/wax/components/toast'
import { Typography } from '@/wax/components/typography'

import { ErrorBanner } from '@/components/error-banner'
import { importSource, validateSource, type InstallInput } from '@/lib/sources'

import * as styles from './source-create.css'

type SourceInputKind = 'variable' | 'secret'
type SurfaceType = 'openapi' | 'mcp'
type OpenApiDescriptorKind = 'url' | 'file'
type OpenApiAuthMode = 'none' | 'bearer' | 'header'
type HeaderValueMode = 'literal' | 'input' | 'template'
type McpTransport = 'streamable_http' | 'stdio'

interface SourceInputDraft {
  id: string
  key: string
  kind: SourceInputKind
  required: boolean
  defaultValue: string
  hint: string
}

interface RequestHeaderDraft {
  id: string
  name: string
  mode: HeaderValueMode
  value: string
}

interface SurfaceDraft {
  id: string
  surfaceId: string
  namespaceSuffix: string
  type: SurfaceType
  openapiDescriptorKind: OpenApiDescriptorKind
  openapiDescriptor: string
  openapiBaseUrl: string
  openapiAuthMode: OpenApiAuthMode
  authInputKey: string
  authHeaderName: string
  requestHeaders: RequestHeaderDraft[]
  mcpTransport: McpTransport
  mcpUrl: string
  mcpCommand: string
  mcpArgs: string
  mcpBearerInputKey: string
}

interface SourceDraft {
  name: string
  description: string
  inputs: SourceInputDraft[]
  surfaces: SurfaceDraft[]
  testQueries: string
}

type ImportState =
  | { kind: 'idle' }
  | { kind: 'importing' }
  | { kind: 'validating'; sourceName: string }
  | { kind: 'imported'; sourceName: string }
  | { kind: 'validated'; sourceName: string; response: Awaited<ReturnType<typeof validateSource>> }

let draftId = 0

function nextDraftId(prefix: string): string {
  draftId += 1
  return `${prefix}-${draftId}`
}

function defaultSurface(): SurfaceDraft {
  return {
    id: nextDraftId('surface'),
    surfaceId: 'rest',
    namespaceSuffix: '',
    type: 'openapi',
    openapiDescriptorKind: 'url',
    openapiDescriptor:
      'https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.yaml',
    openapiBaseUrl: 'https://api.github.com',
    openapiAuthMode: 'none',
    authInputKey: '',
    authHeaderName: 'Authorization',
    requestHeaders: [
      {
        id: nextDraftId('header'),
        name: 'User-Agent',
        mode: 'literal',
        value: 'coral-dsl-v4',
      },
    ],
    mcpTransport: 'streamable_http',
    mcpUrl: 'https://api.githubcopilot.com/mcp/x/all/readonly',
    mcpCommand: '',
    mcpArgs: '',
    mcpBearerInputKey: '',
  }
}

function defaultDraft(): SourceDraft {
  return {
    name: 'github_openapi_v4',
    description: 'Query a generated OpenAPI surface through Coral DSL v4.',
    inputs: [],
    surfaces: [defaultSurface()],
    testQueries: '',
  }
}

export function SourceCreate() {
  const [draft, setDraft] = useState<SourceDraft>(() => defaultDraft())
  const generatedManifest = useMemo(() => buildManifestYaml(draft), [draft])
  const [editedManifest, setEditedManifest] = useState<string | null>(null)
  const [inputValues, setInputValues] = useState<Record<string, string>>({})
  const [importState, setImportState] = useState<ImportState>({ kind: 'idle' })
  const [error, setError] = useState<string | null>(null)

  const manifestYaml = editedManifest ?? generatedManifest
  const validationErrors = useMemo(() => validateDraft(draft), [draft])
  const installErrors = useMemo(
    () => validateInstallInputs(draft.inputs, inputValues),
    [draft.inputs, inputValues],
  )
  const canImport =
    validationErrors.length === 0 &&
    installErrors.length === 0 &&
    manifestYaml.trim().length > 0 &&
    (importState.kind === 'idle' ||
      importState.kind === 'imported' ||
      importState.kind === 'validated')

  async function submit(validateAfterImport: boolean) {
    if (!canImport) return

    setError(null)
    setImportState({ kind: 'importing' })

    try {
      const source = await importSource(manifestYaml, installInputs(draft.inputs, inputValues))
      addToast('neutral', {
        title: `Imported ${source.name}`,
        description: 'The DSL v4 source manifest was materialized and installed.',
      })

      if (!validateAfterImport) {
        setImportState({ kind: 'imported', sourceName: source.name })
        return
      }

      setImportState({ kind: 'validating', sourceName: source.name })
      const response = await validateSource(source.name)
      setImportState({ kind: 'validated', sourceName: source.name, response })
      addToast('neutral', {
        title: `Validated ${source.name}`,
        description: validationSummary(response),
      })
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setImportState({ kind: 'idle' })
    }
  }

  function updateDraft(update: (previous: SourceDraft) => SourceDraft) {
    setDraft(update)
    if (editedManifest === null) return
    setEditedManifest(null)
  }

  return (
    <div className={styles.root}>
      <div className={styles.container}>
        <div className={styles.header}>
          <div className={styles.headerText}>
            <Typography.HeadingLarge as="h1">Create DSL v4 Source</Typography.HeadingLarge>
            <Typography.Body variant="secondary">
              Compose an imported source manifest from OpenAPI and MCP surfaces.
            </Typography.Body>
          </div>
          <div className={styles.headerActions}>
            <ButtonContainer
              onClick={() => {
                setDraft(defaultDraft())
                setEditedManifest(null)
                setInputValues({})
                setImportState({ kind: 'idle' })
                setError(null)
              }}
              size="32"
              variant="secondary"
            >
              <ButtonIcon name="RefreshCw" />
              <ButtonText>Reset</ButtonText>
            </ButtonContainer>
            <ButtonContainer
              disabled={!canImport}
              onClick={() => void submit(false)}
              size="32"
              variant="secondary"
            >
              <ButtonIcon name={importState.kind === 'importing' ? 'Loader' : 'Check'} />
              <ButtonText>{importState.kind === 'importing' ? 'Importing…' : 'Import'}</ButtonText>
            </ButtonContainer>
            <ButtonContainer
              disabled={!canImport}
              onClick={() => void submit(true)}
              size="32"
              variant="primary"
            >
              <ButtonIcon
                name={
                  importState.kind === 'importing' || importState.kind === 'validating'
                    ? 'Loader'
                    : 'Check'
                }
              />
              <ButtonText>{primaryActionLabel(importState)}</ButtonText>
            </ButtonContainer>
          </div>
        </div>

        {validationErrors.length > 0 ? (
          <InlineIssue title="Manifest fields need attention" messages={validationErrors} />
        ) : installErrors.length > 0 ? (
          <InlineIssue title="Installation values need attention" messages={installErrors} />
        ) : null}

        {error ? <ErrorBanner title="Source import failed" message={error} /> : null}

        <div className={styles.layout}>
          <div className={styles.formColumn}>
            <SourceSection draft={draft} updateDraft={updateDraft} />
            <InputsSection
              inputs={draft.inputs}
              updateDraft={updateDraft}
              inputValues={inputValues}
              setInputValues={setInputValues}
            />
            <SurfacesSection draft={draft} updateDraft={updateDraft} />
            <TestQueriesSection draft={draft} updateDraft={updateDraft} />
          </div>

          <aside className={styles.previewColumn}>
            <div className={styles.panel}>
              <div className={styles.panelHead}>
                <div>
                  <Typography.HeadingSmall as="h2">Manifest</Typography.HeadingSmall>
                  {editedManifest ? (
                    <Typography.BodySmall variant="tertiary">
                      Edited YAML is being imported.
                    </Typography.BodySmall>
                  ) : null}
                </div>
                {editedManifest ? (
                  <ButtonContainer onClick={() => setEditedManifest(null)} size="22" variant="bare">
                    <ButtonText>Reset YAML</ButtonText>
                  </ButtonContainer>
                ) : null}
              </div>
              <textarea
                aria-label="Manifest YAML"
                className={styles.manifestTextarea}
                spellCheck={false}
                value={manifestYaml}
                onChange={(event) => setEditedManifest(event.target.value)}
              />
            </div>

            <InstallValuesSection
              inputs={draft.inputs}
              inputValues={inputValues}
              setInputValues={setInputValues}
            />
          </aside>
        </div>

        {importState.kind === 'imported' ? (
          <ResultPanel
            title={`Imported ${importState.sourceName}`}
            body="Run validation to inspect the tables and functions exposed by this source."
            action={
              <ButtonContainer
                onClick={async () => {
                  setError(null)
                  setImportState({ kind: 'validating', sourceName: importState.sourceName })
                  try {
                    const response = await validateSource(importState.sourceName)
                    setImportState({
                      kind: 'validated',
                      sourceName: importState.sourceName,
                      response,
                    })
                  } catch (e) {
                    setError(e instanceof Error ? e.message : String(e))
                    setImportState({ kind: 'imported', sourceName: importState.sourceName })
                  }
                }}
                size="32"
                variant="secondary"
              >
                <ButtonIcon name="Check" />
                <ButtonText>Validate</ButtonText>
              </ButtonContainer>
            }
          />
        ) : null}

        {importState.kind === 'validated' ? <ValidationResult state={importState} /> : null}
      </div>
    </div>
  )
}

function SourceSection({
  draft,
  updateDraft,
}: {
  draft: SourceDraft
  updateDraft: (update: (previous: SourceDraft) => SourceDraft) => void
}) {
  return (
    <section className={styles.panel}>
      <Typography.HeadingSmall as="h2">Source</Typography.HeadingSmall>
      <div className={styles.fieldGrid}>
        <Field label="Name">
          <TextInput
            value={draft.name}
            onChange={(name) => updateDraft((previous) => ({ ...previous, name }))}
            placeholder="github_v4"
          />
        </Field>
        <Field label="Description">
          <TextInput
            value={draft.description}
            onChange={(description) => updateDraft((previous) => ({ ...previous, description }))}
            placeholder="Query data from a provider API"
          />
        </Field>
      </div>
    </section>
  )
}

function InputsSection({
  inputs,
  updateDraft,
  inputValues,
  setInputValues,
}: {
  inputs: SourceInputDraft[]
  updateDraft: (update: (previous: SourceDraft) => SourceDraft) => void
  inputValues: Record<string, string>
  setInputValues: React.Dispatch<React.SetStateAction<Record<string, string>>>
}) {
  return (
    <section className={styles.panel}>
      <div className={styles.panelHead}>
        <Typography.HeadingSmall as="h2">Inputs</Typography.HeadingSmall>
        <ButtonContainer
          onClick={() =>
            updateDraft((previous) => ({
              ...previous,
              inputs: [
                ...previous.inputs,
                {
                  id: nextDraftId('input'),
                  key: `API_TOKEN_${previous.inputs.length + 1}`,
                  kind: 'secret',
                  required: true,
                  defaultValue: '',
                  hint: '',
                },
              ],
            }))
          }
          size="32"
          variant="secondary"
        >
          <ButtonIcon name="Plus" />
          <ButtonText>Add input</ButtonText>
        </ButtonContainer>
      </div>

      {inputs.length === 0 ? (
        <Typography.BodySmall variant="tertiary">
          No source inputs. Add one for API tokens, tenants, base URLs, or MCP auth.
        </Typography.BodySmall>
      ) : (
        <div className={styles.stack}>
          {inputs.map((input) => (
            <InputEditor
              key={input.id}
              input={input}
              inputValue={inputValues[input.id] ?? ''}
              onInputValueChange={(value) =>
                setInputValues((previous) => ({ ...previous, [input.id]: value }))
              }
              updateDraft={updateDraft}
            />
          ))}
        </div>
      )}
    </section>
  )
}

function InputEditor({
  input,
  inputValue,
  onInputValueChange,
  updateDraft,
}: {
  input: SourceInputDraft
  inputValue: string
  onInputValueChange: (value: string) => void
  updateDraft: (update: (previous: SourceDraft) => SourceDraft) => void
}) {
  const updateInput = (patch: Partial<SourceInputDraft>) => {
    updateDraft((previous) => ({
      ...previous,
      inputs: previous.inputs.map((item) => (item.id === input.id ? { ...item, ...patch } : item)),
    }))
  }

  return (
    <div className={styles.itemPanel}>
      <div className={styles.itemHeader}>
        <Typography.BodyLargeStrong>{input.key || 'New input'}</Typography.BodyLargeStrong>
        <ButtonContainer
          onClick={() =>
            updateDraft((previous) => ({
              ...previous,
              inputs: previous.inputs.filter((item) => item.id !== input.id),
              surfaces: previous.surfaces.map((surface) => ({
                ...surface,
                authInputKey: surface.authInputKey === input.key ? '' : surface.authInputKey,
                mcpBearerInputKey:
                  surface.mcpBearerInputKey === input.key ? '' : surface.mcpBearerInputKey,
              })),
            }))
          }
          size="22"
          variant="bare"
        >
          <ButtonIcon name="X" />
          <ButtonText>Remove</ButtonText>
        </ButtonContainer>
      </div>
      <div className={styles.fieldGrid}>
        <Field label="Key">
          <TextInput value={input.key} onChange={(key) => updateInput({ key })} />
        </Field>
        <Field label="Kind">
          <select
            className={styles.select}
            value={input.kind}
            onChange={(event) =>
              updateInput({
                kind: event.target.value as SourceInputKind,
                defaultValue: event.target.value === 'secret' ? '' : input.defaultValue,
              })
            }
          >
            <option value="secret">Secret</option>
            <option value="variable">Variable</option>
          </select>
        </Field>
        <Field label="Install value">
          <TextInput
            type={input.kind === 'secret' ? 'password' : 'text'}
            value={inputValue}
            onChange={onInputValueChange}
            placeholder={input.defaultValue || input.key}
          />
        </Field>
        {input.kind === 'variable' ? (
          <Field label="Default">
            <TextInput
              value={input.defaultValue}
              onChange={(defaultValue) => updateInput({ defaultValue })}
              placeholder="https://api.example.com"
            />
          </Field>
        ) : null}
      </div>
      <div className={styles.fieldGrid}>
        <label className={styles.checkRow}>
          <input
            type="checkbox"
            checked={input.required}
            onChange={(event) => updateInput({ required: event.target.checked })}
          />
          <Typography.BodySmall>Required</Typography.BodySmall>
        </label>
        <Field label="Hint">
          <TextInput
            value={input.hint}
            onChange={(hint) => updateInput({ hint })}
            placeholder="Where to find this value"
          />
        </Field>
      </div>
    </div>
  )
}

function SurfacesSection({
  draft,
  updateDraft,
}: {
  draft: SourceDraft
  updateDraft: (update: (previous: SourceDraft) => SourceDraft) => void
}) {
  return (
    <section className={styles.panel}>
      <div className={styles.panelHead}>
        <Typography.HeadingSmall as="h2">Surfaces</Typography.HeadingSmall>
        <ButtonContainer
          onClick={() =>
            updateDraft((previous) => {
              const surface = defaultSurface()
              surface.surfaceId = previous.surfaces.some((item) => item.surfaceId === 'mcp')
                ? `surface_${previous.surfaces.length + 1}`
                : 'mcp'
              surface.namespaceSuffix = surface.surfaceId
              surface.type = 'mcp'
              return { ...previous, surfaces: [...previous.surfaces, surface] }
            })
          }
          size="32"
          variant="secondary"
        >
          <ButtonIcon name="Plus" />
          <ButtonText>Add surface</ButtonText>
        </ButtonContainer>
      </div>
      <div className={styles.stack}>
        {draft.surfaces.map((surface) => (
          <SurfaceEditor
            key={surface.id}
            draft={draft}
            surface={surface}
            updateDraft={updateDraft}
          />
        ))}
      </div>
    </section>
  )
}

function SurfaceEditor({
  draft,
  surface,
  updateDraft,
}: {
  draft: SourceDraft
  surface: SurfaceDraft
  updateDraft: (update: (previous: SourceDraft) => SourceDraft) => void
}) {
  const secretInputs = draft.inputs.filter((input) => input.kind === 'secret')
  const updateSurface = (patch: Partial<SurfaceDraft>) => {
    updateDraft((previous) => ({
      ...previous,
      surfaces: previous.surfaces.map((item) =>
        item.id === surface.id ? { ...item, ...patch } : item,
      ),
    }))
  }

  return (
    <div className={styles.itemPanel}>
      <div className={styles.itemHeader}>
        <div>
          <Typography.BodyLargeStrong>
            {surface.surfaceId || 'New surface'}
          </Typography.BodyLargeStrong>
          <Typography.BodySmall variant="tertiary">
            {relationName(draft.name, surface)}
          </Typography.BodySmall>
        </div>
        <ButtonContainer
          disabled={draft.surfaces.length === 1}
          onClick={() =>
            updateDraft((previous) => ({
              ...previous,
              surfaces: previous.surfaces.filter((item) => item.id !== surface.id),
            }))
          }
          size="22"
          variant="bare"
        >
          <ButtonIcon name="X" />
          <ButtonText>Remove</ButtonText>
        </ButtonContainer>
      </div>

      <div className={styles.fieldGrid}>
        <Field label="Surface ID">
          <TextInput
            value={surface.surfaceId}
            onChange={(surfaceId) => updateSurface({ surfaceId })}
          />
        </Field>
        <Field label="Namespace suffix">
          <TextInput
            value={surface.namespaceSuffix}
            onChange={(namespaceSuffix) => updateSurface({ namespaceSuffix })}
            placeholder="default namespace"
          />
        </Field>
        <Field label="Type">
          <div className={styles.segmented}>
            <button
              type="button"
              className={styles.segment}
              data-active={surface.type === 'openapi' ? 'true' : 'false'}
              onClick={() => updateSurface({ type: 'openapi' })}
            >
              OpenAPI
            </button>
            <button
              type="button"
              className={styles.segment}
              data-active={surface.type === 'mcp' ? 'true' : 'false'}
              onClick={() => updateSurface({ type: 'mcp' })}
            >
              MCP
            </button>
          </div>
        </Field>
      </div>

      {surface.type === 'openapi' ? (
        <OpenApiSurfaceEditor
          inputs={draft.inputs}
          surface={surface}
          secretInputs={secretInputs}
          updateSurface={updateSurface}
        />
      ) : (
        <McpSurfaceEditor
          surface={surface}
          secretInputs={secretInputs}
          updateSurface={updateSurface}
        />
      )}
    </div>
  )
}

function OpenApiSurfaceEditor({
  inputs,
  surface,
  secretInputs,
  updateSurface,
}: {
  inputs: SourceInputDraft[]
  surface: SurfaceDraft
  secretInputs: SourceInputDraft[]
  updateSurface: (patch: Partial<SurfaceDraft>) => void
}) {
  return (
    <>
      <div className={styles.fieldGrid}>
        <Field label="Descriptor">
          <select
            className={styles.select}
            value={surface.openapiDescriptorKind}
            onChange={(event) =>
              updateSurface({ openapiDescriptorKind: event.target.value as OpenApiDescriptorKind })
            }
          >
            <option value="url">HTTPS URL</option>
            <option value="file">Absolute file path</option>
          </select>
        </Field>
        <Field label={surface.openapiDescriptorKind === 'url' ? 'OpenAPI URL' : 'OpenAPI file'}>
          <TextInput
            type={surface.openapiDescriptorKind === 'url' ? 'url' : 'text'}
            value={surface.openapiDescriptor}
            onChange={(openapiDescriptor) => updateSurface({ openapiDescriptor })}
            placeholder={
              surface.openapiDescriptorKind === 'url'
                ? 'https://example.com/openapi.yaml'
                : '/absolute/path/openapi.yaml'
            }
          />
        </Field>
        <Field label="Base URL">
          <TextInput
            value={surface.openapiBaseUrl}
            onChange={(openapiBaseUrl) => updateSurface({ openapiBaseUrl })}
            placeholder="https://api.example.com"
          />
        </Field>
      </div>

      <div className={styles.fieldGrid}>
        <Field label="Auth">
          <select
            className={styles.select}
            value={surface.openapiAuthMode}
            onChange={(event) =>
              updateSurface({ openapiAuthMode: event.target.value as OpenApiAuthMode })
            }
          >
            <option value="none">None</option>
            <option value="bearer">Bearer token</option>
            <option value="header">Header from secret</option>
          </select>
        </Field>
        {surface.openapiAuthMode !== 'none' ? (
          <Field label="Secret input">
            <InputKeySelect
              inputs={secretInputs}
              value={surface.authInputKey}
              onChange={(authInputKey) => updateSurface({ authInputKey })}
            />
          </Field>
        ) : null}
        {surface.openapiAuthMode === 'header' ? (
          <Field label="Header name">
            <TextInput
              value={surface.authHeaderName}
              onChange={(authHeaderName) => updateSurface({ authHeaderName })}
              placeholder="X-API-Key"
            />
          </Field>
        ) : null}
      </div>

      <div className={styles.subsectionHead}>
        <Typography.BodyStrong>Request headers</Typography.BodyStrong>
        <ButtonContainer
          onClick={() =>
            updateSurface({
              requestHeaders: [
                ...surface.requestHeaders,
                { id: nextDraftId('header'), name: '', mode: 'literal', value: '' },
              ],
            })
          }
          size="22"
          variant="bare"
        >
          <ButtonIcon name="Plus" />
          <ButtonText>Add header</ButtonText>
        </ButtonContainer>
      </div>
      {surface.requestHeaders.length > 0 ? (
        <div className={styles.stackSmall}>
          {surface.requestHeaders.map((header) => (
            <RequestHeaderEditor
              key={header.id}
              header={header}
              inputs={inputs}
              updateHeader={(patch) =>
                updateSurface({
                  requestHeaders: surface.requestHeaders.map((item) =>
                    item.id === header.id ? { ...item, ...patch } : item,
                  ),
                })
              }
              removeHeader={() =>
                updateSurface({
                  requestHeaders: surface.requestHeaders.filter((item) => item.id !== header.id),
                })
              }
            />
          ))}
        </div>
      ) : null}
    </>
  )
}

function McpSurfaceEditor({
  surface,
  secretInputs,
  updateSurface,
}: {
  surface: SurfaceDraft
  secretInputs: SourceInputDraft[]
  updateSurface: (patch: Partial<SurfaceDraft>) => void
}) {
  return (
    <>
      <div className={styles.fieldGrid}>
        <Field label="Transport">
          <select
            className={styles.select}
            value={surface.mcpTransport}
            onChange={(event) =>
              updateSurface({ mcpTransport: event.target.value as McpTransport })
            }
          >
            <option value="streamable_http">Streamable HTTP</option>
            <option value="stdio">Stdio</option>
          </select>
        </Field>
        {surface.mcpTransport === 'streamable_http' ? (
          <Field label="MCP URL">
            <TextInput
              type="url"
              value={surface.mcpUrl}
              onChange={(mcpUrl) => updateSurface({ mcpUrl })}
              placeholder="https://example.com/mcp"
            />
          </Field>
        ) : (
          <Field label="Command">
            <TextInput
              value={surface.mcpCommand}
              onChange={(mcpCommand) => updateSurface({ mcpCommand })}
              placeholder="npx"
            />
          </Field>
        )}
        {surface.mcpTransport === 'stdio' ? (
          <Field label="Arguments">
            <TextInput
              value={surface.mcpArgs}
              onChange={(mcpArgs) => updateSurface({ mcpArgs })}
              placeholder="-y @modelcontextprotocol/server-example"
            />
          </Field>
        ) : null}
      </div>
      {surface.mcpTransport === 'streamable_http' ? (
        <div className={styles.fieldGrid}>
          <Field label="Bearer input">
            <InputKeySelect
              allowEmpty
              inputs={secretInputs}
              value={surface.mcpBearerInputKey}
              onChange={(mcpBearerInputKey) => updateSurface({ mcpBearerInputKey })}
            />
          </Field>
        </div>
      ) : null}
    </>
  )
}

function RequestHeaderEditor({
  header,
  inputs,
  updateHeader,
  removeHeader,
}: {
  header: RequestHeaderDraft
  inputs: SourceInputDraft[]
  updateHeader: (patch: Partial<RequestHeaderDraft>) => void
  removeHeader: () => void
}) {
  return (
    <div className={styles.headerRow}>
      <TextInput
        value={header.name}
        onChange={(name) => updateHeader({ name })}
        placeholder="Header"
      />
      <select
        className={styles.select}
        value={header.mode}
        onChange={(event) => updateHeader({ mode: event.target.value as HeaderValueMode })}
      >
        <option value="literal">Literal</option>
        <option value="input">Input</option>
        <option value="template">Template</option>
      </select>
      {header.mode === 'input' ? (
        <InputKeySelect
          inputs={inputs}
          value={header.value}
          onChange={(value) => updateHeader({ value })}
        />
      ) : (
        <TextInput value={header.value} onChange={(value) => updateHeader({ value })} />
      )}
      <ButtonContainer ariaLabel="Remove header" onClick={removeHeader} size="32" variant="bare">
        <ButtonIcon name="X" />
      </ButtonContainer>
    </div>
  )
}

function TestQueriesSection({
  draft,
  updateDraft,
}: {
  draft: SourceDraft
  updateDraft: (update: (previous: SourceDraft) => SourceDraft) => void
}) {
  return (
    <section className={styles.panel}>
      <Typography.HeadingSmall as="h2">Test Queries</Typography.HeadingSmall>
      <textarea
        aria-label="Test queries"
        className={styles.textarea}
        value={draft.testQueries}
        onChange={(event) =>
          updateDraft((previous) => ({ ...previous, testQueries: event.target.value }))
        }
        placeholder="SELECT * FROM github_openapi_v4.search_issues_and_pull_requests(q => 'repo:octocat/Hello-World is:issue') LIMIT 5"
      />
    </section>
  )
}

function InstallValuesSection({
  inputs,
  inputValues,
  setInputValues,
}: {
  inputs: SourceInputDraft[]
  inputValues: Record<string, string>
  setInputValues: React.Dispatch<React.SetStateAction<Record<string, string>>>
}) {
  return (
    <div className={styles.panel}>
      <Typography.HeadingSmall as="h2">Install Values</Typography.HeadingSmall>
      {inputs.length === 0 ? (
        <Typography.BodySmall variant="tertiary">
          No values needed for this manifest.
        </Typography.BodySmall>
      ) : (
        <div className={styles.stackSmall}>
          {inputs.map((input) => (
            <Field key={input.id} label={input.key || 'Input'}>
              <TextInput
                type={input.kind === 'secret' ? 'password' : 'text'}
                value={inputValues[input.id] ?? ''}
                onChange={(value) =>
                  setInputValues((previous) => ({ ...previous, [input.id]: value }))
                }
                placeholder={input.defaultValue || input.key}
              />
            </Field>
          ))}
        </div>
      )}
    </div>
  )
}

function ValidationResult({ state }: { state: Extract<ImportState, { kind: 'validated' }> }) {
  const { response } = state
  return (
    <section className={styles.panel}>
      <div className={styles.resultHeader}>
        <div>
          <Typography.HeadingSmall as="h2">Validated {state.sourceName}</Typography.HeadingSmall>
          <Typography.BodySmall variant="tertiary">
            {validationSummary(response)}
          </Typography.BodySmall>
        </div>
        <Icon name="CircleCheck" size="24" color="success" />
      </div>

      {response.tables.length > 0 ? (
        <div className={styles.resultSection}>
          <Typography.BodyStrong>Tables</Typography.BodyStrong>
          <div className={styles.resultGrid}>
            {response.tables.slice(0, 8).map((table) => (
              <div className={styles.resultCard} key={`${table.schemaName}.${table.name}`}>
                <Typography.BodySmallStrong>
                  {table.schemaName}.{table.name}
                </Typography.BodySmallStrong>
                <Typography.BodySmall variant="tertiary">
                  {table.columns.length} columns
                  {table.requiredFilters.length
                    ? `, ${table.requiredFilters.length} required filters`
                    : ''}
                </Typography.BodySmall>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {response.tableFunctions.length > 0 ? (
        <div className={styles.resultSection}>
          <Typography.BodyStrong>Table Functions</Typography.BodyStrong>
          <div className={styles.resultGrid}>
            {response.tableFunctions.slice(0, 8).map((fn) => (
              <div className={styles.resultCard} key={`${fn.schemaName}.${fn.name}`}>
                <Typography.BodySmallStrong>
                  {fn.schemaName}.{fn.name}
                </Typography.BodySmallStrong>
                <Typography.BodySmall variant="tertiary">
                  {fn.arguments.length} args, {fn.resultColumns.length} columns
                </Typography.BodySmall>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {response.queryTests.length > 0 ? (
        <div className={styles.resultSection}>
          <Typography.BodyStrong>Query Tests</Typography.BodyStrong>
          <div className={styles.stackSmall}>
            {response.queryTests.map((test) => (
              <div className={styles.queryResult} key={test.sql}>
                <Typography.CodeSmallInline>{test.sql}</Typography.CodeSmallInline>
                <Typography.BodySmall
                  variant={test.outcome.case === 'failure' ? 'tertiary' : 'secondary'}
                >
                  {test.outcome.case === 'success'
                    ? `${test.outcome.value.rowCount.toString()} rows`
                    : test.outcome.case === 'failure'
                      ? test.outcome.value.errorMessage
                      : 'No result'}
                </Typography.BodySmall>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </section>
  )
}

function ResultPanel({
  title,
  body,
  action,
}: {
  title: string
  body: string
  action: React.ReactNode
}) {
  return (
    <section className={styles.panel}>
      <div className={styles.resultHeader}>
        <div>
          <Typography.HeadingSmall as="h2">{title}</Typography.HeadingSmall>
          <Typography.BodySmall variant="tertiary">{body}</Typography.BodySmall>
        </div>
        {action}
      </div>
    </section>
  )
}

function InlineIssue({ title, messages }: { title: string; messages: string[] }) {
  return (
    <div className={styles.issueBox}>
      <Icon name="CircleAlert" size="16" color="warning" />
      <div>
        <Typography.BodySmallStrong>{title}</Typography.BodySmallStrong>
        <ul className={styles.issueList}>
          {messages.slice(0, 4).map((message) => (
            <li key={message}>
              <Typography.BodySmall variant="secondary">{message}</Typography.BodySmall>
            </li>
          ))}
        </ul>
      </div>
    </div>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className={styles.field}>
      <Typography.BodySmallStrong>{label}</Typography.BodySmallStrong>
      {children}
    </label>
  )
}

function InputKeySelect({
  inputs,
  value,
  onChange,
  allowEmpty,
}: {
  inputs: SourceInputDraft[]
  value: string
  onChange: (value: string) => void
  allowEmpty?: boolean
}) {
  return (
    <select
      className={styles.select}
      value={value}
      onChange={(event) => onChange(event.target.value)}
    >
      {allowEmpty ? <option value="">None</option> : null}
      {!allowEmpty && value === '' ? <option value="">Select input</option> : null}
      {inputs.map((input) => (
        <option key={input.id} value={input.key}>
          {input.key}
        </option>
      ))}
    </select>
  )
}

function buildManifestYaml(draft: SourceDraft): string {
  const lines: string[] = []
  lines.push(`name: ${yamlScalar(draft.name)}`)
  lines.push('dsl_version: 4')
  if (draft.description.trim()) emitStringField(lines, 0, 'description', draft.description.trim())
  lines.push('surfaces:')

  for (const surface of draft.surfaces) {
    lines.push(`  - id: ${yamlScalar(surface.surfaceId)}`)
    if (surface.namespaceSuffix.trim()) {
      lines.push(`    namespace_suffix: ${yamlScalar(surface.namespaceSuffix.trim())}`)
    }
    lines.push(`    type: ${surface.type}`)

    if (surface.type === 'openapi') {
      const descriptorKey = surface.openapiDescriptorKind === 'url' ? 'url' : 'file'
      lines.push(`    ${descriptorKey}: ${yamlScalar(surface.openapiDescriptor.trim())}`)
      emitInputs(lines, 4, draft.inputs)
      if (surface.openapiBaseUrl.trim()) {
        lines.push(`    base_url: ${yamlScalar(surface.openapiBaseUrl.trim())}`)
      }
      emitOpenApiAuth(lines, surface)
      emitRequestHeaders(lines, surface.requestHeaders)
    } else {
      emitInputs(lines, 4, draft.inputs)
      lines.push('    server:')
      if (surface.mcpTransport === 'streamable_http') {
        lines.push('      transport: streamable_http')
        lines.push(`      url: ${yamlScalar(surface.mcpUrl.trim())}`)
        if (surface.mcpBearerInputKey) {
          lines.push('      auth:')
          lines.push('        type: bearer')
          lines.push('        from: input')
          lines.push(`        key: ${yamlScalar(surface.mcpBearerInputKey)}`)
        }
      } else {
        lines.push('      transport: stdio')
        lines.push(`      command: ${yamlScalar(surface.mcpCommand.trim())}`)
        const args = splitArgs(surface.mcpArgs)
        if (args.length > 0) {
          lines.push('      args:')
          for (const arg of args) lines.push(`        - ${yamlScalar(arg)}`)
        }
      }
    }
  }

  const queries = draft.testQueries
    .split('\n')
    .map((query) => query.trim())
    .filter(Boolean)
  if (queries.length > 0) {
    lines.push('test_queries:')
    for (const query of queries) lines.push(`  - ${yamlScalar(query)}`)
  }

  return `${lines.join('\n')}\n`
}

function emitInputs(lines: string[], indent: number, inputs: SourceInputDraft[]) {
  if (inputs.length === 0) return
  const pad = ' '.repeat(indent)
  lines.push(`${pad}inputs:`)
  for (const input of inputs) {
    lines.push(`${pad}  ${input.key || 'INPUT'}:`)
    lines.push(`${pad}    kind: ${input.kind}`)
    if (input.kind === 'variable' && input.defaultValue.trim()) {
      lines.push(`${pad}    default: ${yamlScalar(input.defaultValue.trim())}`)
    }
    const requiredDefault = input.kind === 'variable' && input.defaultValue.trim().length > 0
    if (input.required === false || requiredDefault) {
      lines.push(`${pad}    required: ${input.required ? 'true' : 'false'}`)
    }
    if (input.hint.trim()) emitStringField(lines, indent + 4, 'hint', input.hint.trim())
  }
}

function emitOpenApiAuth(lines: string[], surface: SurfaceDraft) {
  if (surface.openapiAuthMode === 'none') return
  lines.push('    auth:')
  lines.push('      type: HeaderAuth')
  lines.push('      headers:')
  if (surface.openapiAuthMode === 'bearer') {
    lines.push('        - name: Authorization')
    lines.push('          from: template')
    lines.push(`          template: ${yamlScalar(`Bearer {{input.${surface.authInputKey}}}`)}`)
    return
  }
  lines.push(`        - name: ${yamlScalar(surface.authHeaderName.trim())}`)
  lines.push('          from: input')
  lines.push(`          key: ${yamlScalar(surface.authInputKey)}`)
}

function emitRequestHeaders(lines: string[], headers: RequestHeaderDraft[]) {
  const active = headers.filter((header) => header.name.trim() && header.value.trim())
  if (active.length === 0) return
  lines.push('    request_headers:')
  for (const header of active) {
    lines.push(`      - name: ${yamlScalar(header.name.trim())}`)
    if (header.mode === 'input') {
      lines.push('        from: input')
      lines.push(`        key: ${yamlScalar(header.value.trim())}`)
    } else if (header.mode === 'template') {
      lines.push('        from: template')
      lines.push(`        template: ${yamlScalar(header.value.trim())}`)
    } else {
      lines.push('        from: literal')
      lines.push(`        value: ${yamlScalar(header.value.trim())}`)
    }
  }
}

function emitStringField(lines: string[], indent: number, key: string, value: string) {
  const pad = ' '.repeat(indent)
  if (value.includes('\n')) {
    lines.push(`${pad}${key}: |`)
    for (const line of value.split('\n')) lines.push(`${pad}  ${line}`)
  } else {
    lines.push(`${pad}${key}: ${yamlScalar(value)}`)
  }
}

function yamlScalar(value: string): string {
  return JSON.stringify(value)
}

function splitArgs(raw: string): string[] {
  return raw
    .split(/\s+/)
    .map((arg) => arg.trim())
    .filter(Boolean)
}

function validateDraft(draft: SourceDraft): string[] {
  const errors: string[] = []
  if (!/^[a-z][a-z0-9_]*$/.test(draft.name)) {
    errors.push('Source name must match [a-z][a-z0-9_]*.')
  }
  if (['coral', 'coral_admin', 'public'].includes(draft.name)) {
    errors.push(`Source name '${draft.name}' is reserved.`)
  }
  validateInputs(draft.inputs, errors)
  validateSurfaces(draft, errors)
  return errors
}

function validateInputs(inputs: SourceInputDraft[], errors: string[]) {
  const seen = new Set<string>()
  for (const input of inputs) {
    if (!input.key.trim()) errors.push('Input keys must not be empty.')
    if (/[=/\\\n\r]/.test(input.key) || input.key.startsWith('#')) {
      errors.push(`Input '${input.key}' contains characters the source spec rejects.`)
    }
    if (seen.has(input.key)) errors.push(`Input '${input.key}' is declared more than once.`)
    seen.add(input.key)
    if (input.kind === 'secret' && input.defaultValue) {
      errors.push(`Secret input '${input.key}' cannot declare a default.`)
    }
    if (input.kind === 'variable' && credentialLike(input.key)) {
      errors.push(`Credential-looking input '${input.key}' must be a secret.`)
    }
  }
}

function validateSurfaces(draft: SourceDraft, errors: string[]) {
  const surfaceIds = new Set<string>()
  const namespaces = new Set<string>()
  let defaultNamespaceSurface: string | null = null
  const inputKeys = new Set(draft.inputs.map((input) => input.key))
  const secretKeys = new Set(
    draft.inputs.filter((input) => input.kind === 'secret').map((input) => input.key),
  )

  for (const surface of draft.surfaces) {
    if (!/^[a-z][a-z0-9_]*$/.test(surface.surfaceId)) {
      errors.push(`Surface '${surface.surfaceId}' must match [a-z][a-z0-9_]*.`)
    }
    if (surfaceIds.has(surface.surfaceId)) {
      errors.push(`Surface '${surface.surfaceId}' is declared more than once.`)
    }
    surfaceIds.add(surface.surfaceId)

    if (surface.namespaceSuffix.trim()) {
      if (!/^[a-z][a-z0-9_]*$/.test(surface.namespaceSuffix)) {
        errors.push(`Surface '${surface.surfaceId}' namespace suffix must match [a-z][a-z0-9_]*.`)
      }
    } else if (draft.surfaces.length > 1) {
      if (defaultNamespaceSurface) {
        errors.push(
          `Surfaces '${defaultNamespaceSurface}' and '${surface.surfaceId}' both use the default namespace.`,
        )
      }
      defaultNamespaceSurface = surface.surfaceId
    }

    const namespace = relationName(draft.name, surface)
    if (namespaces.has(namespace)) errors.push(`Relation namespace '${namespace}' is duplicated.`)
    namespaces.add(namespace)

    if (surface.type === 'openapi') {
      if (!surface.openapiDescriptor.trim()) {
        errors.push(`Surface '${surface.surfaceId}' needs a descriptor.`)
      }
      if (
        surface.openapiDescriptorKind === 'url' &&
        !surface.openapiDescriptor.trim().startsWith('https://')
      ) {
        errors.push(`Surface '${surface.surfaceId}' OpenAPI URL must use https.`)
      }
      if (
        surface.openapiDescriptorKind === 'file' &&
        !surface.openapiDescriptor.trim().startsWith('/')
      ) {
        errors.push(`Surface '${surface.surfaceId}' file descriptor must be absolute.`)
      }
      if (surface.openapiAuthMode !== 'none' && !secretKeys.has(surface.authInputKey)) {
        errors.push(`Surface '${surface.surfaceId}' auth must reference a secret input.`)
      }
      if (surface.openapiAuthMode === 'header' && !surface.authHeaderName.trim()) {
        errors.push(`Surface '${surface.surfaceId}' auth header name is required.`)
      }
      for (const header of surface.requestHeaders) {
        if (
          (header.name.trim() && !header.value.trim()) ||
          (!header.name.trim() && header.value.trim())
        ) {
          errors.push(`Surface '${surface.surfaceId}' has an incomplete request header.`)
        }
        if (header.mode === 'input' && header.value.trim() && !inputKeys.has(header.value)) {
          errors.push(`Surface '${surface.surfaceId}' request header references an unknown input.`)
        }
      }
    } else if (surface.mcpTransport === 'streamable_http') {
      if (!surface.mcpUrl.trim()) {
        errors.push(`Surface '${surface.surfaceId}' needs an MCP URL.`)
      }
      if (surface.mcpBearerInputKey && !secretKeys.has(surface.mcpBearerInputKey)) {
        errors.push(`Surface '${surface.surfaceId}' MCP auth must reference a secret input.`)
      }
    } else if (!surface.mcpCommand.trim()) {
      errors.push(`Surface '${surface.surfaceId}' needs a stdio command.`)
    }
  }
}

function credentialLike(key: string): boolean {
  return [
    'API_KEY',
    'APPLICATION_KEY',
    'ACCESS_KEY',
    'ACCESS_TOKEN',
    'ADMIN_KEY',
    'AUTHORIZATION',
    'BEARER_TOKEN',
    'CLIENT_SECRET',
    'PASSWORD',
    'PRIVATE_KEY',
    'READ_KEY',
    'SECRET',
    'TOKEN',
  ].some((marker) => key.toUpperCase().includes(marker))
}

function validateInstallInputs(
  inputs: SourceInputDraft[],
  inputValues: Record<string, string>,
): string[] {
  return inputs
    .filter((input) => input.required)
    .filter((input) => !(inputValues[input.id] ?? '').trim())
    .map((input) => `Required input '${input.key}' needs an install value.`)
}

function installInputs(inputs: SourceInputDraft[], values: Record<string, string>): InstallInput[] {
  return inputs.flatMap((input) => {
    const value = (values[input.id] ?? '').trim()
    if (!value) return []
    return [{ key: input.key, value, secret: input.kind === 'secret' }]
  })
}

function relationName(sourceName: string, surface: SurfaceDraft): string {
  const suffix = surface.namespaceSuffix.trim()
  return suffix ? `${sourceName}_${suffix}` : sourceName
}

function primaryActionLabel(state: ImportState): string {
  if (state.kind === 'importing') return 'Importing…'
  if (state.kind === 'validating') return 'Validating…'
  return 'Import & validate'
}

function validationSummary(response: Awaited<ReturnType<typeof validateSource>>): string {
  return `${response.tables.length} tables, ${response.tableFunctions.length} table functions, ${response.queryTests.length} query tests.`
}
