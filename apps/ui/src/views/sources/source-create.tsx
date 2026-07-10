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

type SourceKind = 'openapi' | 'mcp'
type AuthMode = 'none' | 'bearer' | 'header'
type McpTransport = 'streamable_http' | 'stdio'

interface SourceDraft {
  kind: SourceKind
  name: string
  description: string
  openapiUrl: string
  openapiBaseUrl: string
  mcpTransport: McpTransport
  mcpUrl: string
  mcpCommand: string
  mcpArgs: string
  authMode: AuthMode
  headerName: string
  tokenKey: string
  tokenValue: string
}

type ImportState =
  | { kind: 'idle' }
  | { kind: 'importing' }
  | { kind: 'validating'; sourceName: string }
  | { kind: 'validated'; sourceName: string; response: Awaited<ReturnType<typeof validateSource>> }

const DEFAULT_OPENAPI_URL =
  'https://raw.githubusercontent.com/github/rest-api-description/main/descriptions/api.github.com/api.github.com.yaml'
const DEFAULT_MCP_URL = 'https://api.githubcopilot.com/mcp/x/all/readonly'

function defaultDraft(kind: SourceKind = 'openapi'): SourceDraft {
  return {
    kind,
    name: defaultName(kind),
    description: defaultDescription(kind),
    openapiUrl: DEFAULT_OPENAPI_URL,
    openapiBaseUrl: '',
    mcpTransport: 'streamable_http',
    mcpUrl: DEFAULT_MCP_URL,
    mcpCommand: '',
    mcpArgs: '',
    authMode: 'bearer',
    headerName: 'Authorization',
    tokenKey: 'GITHUB_TOKEN',
    tokenValue: '',
  }
}

function defaultName(kind: SourceKind): string {
  return kind === 'openapi' ? 'github_openapi_v4' : 'github_mcp_v4'
}

function defaultDescription(kind: SourceKind): string {
  return kind === 'openapi'
    ? 'Generated from a remote OpenAPI descriptor.'
    : 'Generated from an MCP server tool catalog.'
}

export function SourceCreate() {
  const [draft, setDraft] = useState<SourceDraft>(() => defaultDraft())
  const generatedManifest = useMemo(() => buildManifestYaml(draft), [draft])
  const [editedManifest, setEditedManifest] = useState<string | null>(null)
  const [importState, setImportState] = useState<ImportState>({ kind: 'idle' })
  const [error, setError] = useState<string | null>(null)

  const manifestYaml = editedManifest ?? generatedManifest
  const validationErrors = useMemo(() => validateDraft(draft), [draft])
  const canImport =
    validationErrors.length === 0 &&
    manifestYaml.trim().length > 0 &&
    (importState.kind === 'idle' || importState.kind === 'validated')

  function updateDraft(update: (previous: SourceDraft) => SourceDraft) {
    setDraft(update)
    setEditedManifest(null)
    setImportState((previous) => (previous.kind === 'validated' ? { kind: 'idle' } : previous))
    setError(null)
  }

  async function submit() {
    if (!canImport) return

    setError(null)
    setImportState({ kind: 'importing' })

    try {
      const source = await importSource(manifestYaml, installInputs(draft))
      addToast('neutral', {
        title: `Imported ${source.name}`,
        description: 'Coral materialized the DSL v4 source and generated projections.',
      })

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

  return (
    <div className={styles.root}>
      <div className={styles.container}>
        <div className={styles.header}>
          <div className={styles.headerText}>
            <Typography.HeadingLarge as="h1">Create DSL v4 Source</Typography.HeadingLarge>
            <Typography.Body variant="secondary">
              Create one generated OpenAPI or MCP source.
            </Typography.Body>
          </div>
          <ButtonContainer
            onClick={() => {
              setDraft(defaultDraft())
              setEditedManifest(null)
              setImportState({ kind: 'idle' })
              setError(null)
            }}
            size="32"
            variant="secondary"
          >
            <ButtonIcon name="RefreshCw" />
            <ButtonText>Reset</ButtonText>
          </ButtonContainer>
        </div>

        <div className={styles.layout}>
          <div className={styles.formColumn}>
            {error ? <ErrorBanner message={error} /> : null}

            <KindSection draft={draft} updateDraft={updateDraft} />
            <ConnectionSection draft={draft} updateDraft={updateDraft} />
            <AuthSection draft={draft} updateDraft={updateDraft} />

            {validationErrors.length > 0 ? (
              <InlineIssue title="Complete the source setup" messages={validationErrors} />
            ) : null}

            <GenerateSection canImport={canImport} importState={importState} submit={submit} />

            {importState.kind === 'validated' ? <ValidationResult state={importState} /> : null}
          </div>

          <aside className={styles.previewColumn}>
            <section className={styles.panel}>
              <div className={styles.panelHead}>
                <div>
                  <Typography.HeadingSmall as="h2">Manifest YAML</Typography.HeadingSmall>
                  <Typography.BodySmall variant="tertiary">
                    Review or edit the manifest sent to ImportSource.
                  </Typography.BodySmall>
                </div>
                {editedManifest !== null ? (
                  <ButtonContainer
                    onClick={() => setEditedManifest(null)}
                    size="32"
                    variant="secondary"
                  >
                    <ButtonIcon name="RefreshCw" />
                    <ButtonText>Regenerate</ButtonText>
                  </ButtonContainer>
                ) : null}
              </div>
              <textarea
                aria-label="Manifest YAML"
                className={styles.manifestTextarea}
                value={manifestYaml}
                onChange={(event) => setEditedManifest(event.target.value)}
                spellCheck={false}
              />
            </section>
          </aside>
        </div>
      </div>
    </div>
  )
}

function KindSection({
  draft,
  updateDraft,
}: {
  draft: SourceDraft
  updateDraft: (update: (previous: SourceDraft) => SourceDraft) => void
}) {
  function setKind(kind: SourceKind) {
    updateDraft((previous) => {
      const previousDefaultName = defaultName(previous.kind)
      const previousDefaultDescription = defaultDescription(previous.kind)

      return {
        ...previous,
        kind,
        name: previous.name === previousDefaultName ? defaultName(kind) : previous.name,
        description:
          previous.description === previousDefaultDescription
            ? defaultDescription(kind)
            : previous.description,
        authMode:
          kind === 'mcp' && previous.authMode === 'header'
            ? 'bearer'
            : previous.mcpTransport === 'stdio'
              ? 'none'
              : previous.authMode,
      }
    })
  }

  return (
    <section className={styles.panel}>
      <Typography.HeadingSmall as="h2">1. Source type</Typography.HeadingSmall>
      <div className={styles.choiceGrid}>
        <ChoiceButton
          active={draft.kind === 'openapi'}
          icon="Activity"
          label="OpenAPI spec"
          meta="Remote HTTPS descriptor"
          onClick={() => setKind('openapi')}
        />
        <ChoiceButton
          active={draft.kind === 'mcp'}
          icon="Plug"
          label="MCP server"
          meta="Streamable HTTP or stdio"
          onClick={() => setKind('mcp')}
        />
      </div>
    </section>
  )
}

function ConnectionSection({
  draft,
  updateDraft,
}: {
  draft: SourceDraft
  updateDraft: (update: (previous: SourceDraft) => SourceDraft) => void
}) {
  return (
    <section className={styles.panel}>
      <Typography.HeadingSmall as="h2">2. Connection</Typography.HeadingSmall>
      <div className={styles.fieldGrid}>
        <Field label="Source name">
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
            placeholder="Generated source"
          />
        </Field>
      </div>

      {draft.kind === 'openapi' ? (
        <div className={styles.stack}>
          <Field label="OpenAPI spec URL">
            <TextInput
              type="url"
              value={draft.openapiUrl}
              onChange={(openapiUrl) => updateDraft((previous) => ({ ...previous, openapiUrl }))}
              placeholder="https://example.com/openapi.yaml"
            />
          </Field>
          <Field label="Base URL override">
            <TextInput
              type="url"
              value={draft.openapiBaseUrl}
              onChange={(openapiBaseUrl) =>
                updateDraft((previous) => ({ ...previous, openapiBaseUrl }))
              }
              placeholder="Use OpenAPI servers entry"
            />
          </Field>
        </div>
      ) : (
        <div className={styles.stack}>
          <Field label="Transport">
            <select
              className={styles.select}
              value={draft.mcpTransport}
              onChange={(event) => {
                const mcpTransport = event.target.value as McpTransport
                updateDraft((previous) => ({
                  ...previous,
                  mcpTransport,
                  authMode:
                    mcpTransport === 'stdio'
                      ? 'none'
                      : previous.authMode === 'header'
                        ? 'bearer'
                        : previous.authMode,
                }))
              }}
            >
              <option value="streamable_http">Streamable HTTP</option>
              <option value="stdio">Stdio command</option>
            </select>
          </Field>

          {draft.mcpTransport === 'streamable_http' ? (
            <Field label="MCP server URL">
              <TextInput
                type="url"
                value={draft.mcpUrl}
                onChange={(mcpUrl) => updateDraft((previous) => ({ ...previous, mcpUrl }))}
                placeholder="https://example.com/mcp"
              />
            </Field>
          ) : (
            <div className={styles.fieldGrid}>
              <Field label="Command">
                <TextInput
                  value={draft.mcpCommand}
                  onChange={(mcpCommand) =>
                    updateDraft((previous) => ({ ...previous, mcpCommand }))
                  }
                  placeholder="demo-mcp-server"
                />
              </Field>
              <Field label="Arguments">
                <TextInput
                  value={draft.mcpArgs}
                  onChange={(mcpArgs) => updateDraft((previous) => ({ ...previous, mcpArgs }))}
                  placeholder="--readonly"
                />
              </Field>
            </div>
          )}
        </div>
      )}
    </section>
  )
}

function AuthSection({
  draft,
  updateDraft,
}: {
  draft: SourceDraft
  updateDraft: (update: (previous: SourceDraft) => SourceDraft) => void
}) {
  const authModes = allowedAuthModes(draft)
  const authMode = authModes.includes(draft.authMode) ? draft.authMode : 'none'

  return (
    <section className={styles.panel}>
      <Typography.HeadingSmall as="h2">3. Auth</Typography.HeadingSmall>
      <div className={styles.fieldGrid}>
        <Field label="Auth mode">
          <select
            className={styles.select}
            value={authMode}
            onChange={(event) => {
              const nextAuthMode = event.target.value as AuthMode
              updateDraft((previous) => ({ ...previous, authMode: nextAuthMode }))
            }}
          >
            <option value="none">None</option>
            {authModes.includes('bearer') ? <option value="bearer">Bearer token</option> : null}
            {authModes.includes('header') ? <option value="header">Header token</option> : null}
          </select>
        </Field>
        {draft.kind === 'openapi' && authMode === 'header' ? (
          <Field label="Header name">
            <TextInput
              value={draft.headerName}
              onChange={(headerName) => updateDraft((previous) => ({ ...previous, headerName }))}
              placeholder="Authorization"
            />
          </Field>
        ) : null}
      </div>

      {needsSecret(draft) ? (
        <div className={styles.fieldGrid}>
          <Field label="Secret input key">
            <TextInput
              value={draft.tokenKey}
              onChange={(tokenKey) => updateDraft((previous) => ({ ...previous, tokenKey }))}
              placeholder="API_TOKEN"
            />
          </Field>
          <Field label="Token">
            <TextInput
              type="password"
              value={draft.tokenValue}
              onChange={(tokenValue) => updateDraft((previous) => ({ ...previous, tokenValue }))}
              placeholder="Paste token"
            />
          </Field>
        </div>
      ) : null}
    </section>
  )
}

function GenerateSection({
  canImport,
  importState,
  submit,
}: {
  canImport: boolean
  importState: ImportState
  submit: () => Promise<void>
}) {
  const busy = importState.kind === 'importing' || importState.kind === 'validating'
  const buttonText =
    importState.kind === 'importing'
      ? 'Importing...'
      : importState.kind === 'validating'
        ? 'Validating...'
        : 'Import and generate projections'

  return (
    <section className={styles.panel}>
      <div className={styles.generateRow}>
        <div>
          <Typography.HeadingSmall as="h2">4. Generate</Typography.HeadingSmall>
          <Typography.BodySmall variant="tertiary">
            ImportSource materializes the source, then validation lists generated projections.
          </Typography.BodySmall>
        </div>
        <ButtonContainer disabled={!canImport || busy} onClick={() => void submit()} size="32">
          <ButtonIcon name={busy ? 'Loader' : 'Check'} />
          <ButtonText>{buttonText}</ButtonText>
        </ButtonContainer>
      </div>
    </section>
  )
}

function ChoiceButton({
  active,
  icon,
  label,
  meta,
  onClick,
}: {
  active: boolean
  icon: 'Activity' | 'Plug'
  label: string
  meta: string
  onClick: () => void
}) {
  return (
    <button
      aria-pressed={active}
      className={styles.choiceButton({ active })}
      onClick={onClick}
      type="button"
    >
      <Icon name={icon} size="20" color={active ? 'primary' : 'secondary'} />
      <span className={styles.choiceText}>
        <Typography.BodySmallStrong>{label}</Typography.BodySmallStrong>
        <Typography.BodySmall variant="tertiary">{meta}</Typography.BodySmall>
      </span>
    </button>
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

function InlineIssue({ title, messages }: { title: string; messages: string[] }) {
  return (
    <div className={styles.issueBox}>
      <Icon name="CircleAlert" size="16" color="warning" />
      <div>
        <Typography.BodySmallStrong>{title}</Typography.BodySmallStrong>
        <ul className={styles.issueList}>
          {messages.slice(0, 5).map((message) => (
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

function allowedAuthModes(draft: SourceDraft): AuthMode[] {
  if (draft.kind === 'openapi') return ['none', 'bearer', 'header']
  if (draft.mcpTransport === 'streamable_http') return ['none', 'bearer']
  return ['none']
}

function needsSecret(draft: SourceDraft): boolean {
  if (draft.kind === 'openapi') return draft.authMode === 'bearer' || draft.authMode === 'header'
  return draft.mcpTransport === 'streamable_http' && draft.authMode === 'bearer'
}

function installInputs(draft: SourceDraft): InstallInput[] {
  if (!needsSecret(draft)) return []
  return [{ key: draft.tokenKey.trim(), value: draft.tokenValue, secret: true }]
}

function buildManifestYaml(draft: SourceDraft): string {
  const lines = [`name: ${yamlString(draft.name.trim())}`, 'dsl_version: 4']
  if (draft.description.trim()) lines.push(`description: ${yamlString(draft.description.trim())}`)
  lines.push('surfaces:')

  if (draft.kind === 'openapi') {
    lines.push('  - id: rest')
    lines.push('    type: openapi')
    lines.push(`    url: ${yamlString(draft.openapiUrl.trim())}`)
    if (needsSecret(draft)) pushSecretInput(lines, '    ', draft.tokenKey.trim())
    if (draft.openapiBaseUrl.trim()) {
      lines.push(`    base_url: ${yamlString(draft.openapiBaseUrl.trim())}`)
    }
    if (draft.authMode === 'bearer') {
      lines.push('    auth:')
      lines.push('      type: HeaderAuth')
      lines.push('      headers:')
      lines.push(`        - name: ${yamlString(draft.headerName.trim() || 'Authorization')}`)
      lines.push('          from: template')
      lines.push(`          template: ${yamlString(`Bearer {{input.${draft.tokenKey.trim()}}}`)}`)
    } else if (draft.authMode === 'header') {
      lines.push('    auth:')
      lines.push('      type: HeaderAuth')
      lines.push('      headers:')
      lines.push(`        - name: ${yamlString(draft.headerName.trim())}`)
      lines.push('          from: template')
      lines.push(`          template: ${yamlString(`{{input.${draft.tokenKey.trim()}}}`)}`)
    }
    lines.push('    request_headers:')
    lines.push('      - name: User-Agent')
    lines.push('        from: literal')
    lines.push('        value: coral-dsl-v4')
  } else {
    lines.push('  - id: mcp')
    lines.push('    type: mcp')
    if (needsSecret(draft)) pushSecretInput(lines, '    ', draft.tokenKey.trim())
    lines.push('    server:')
    if (draft.mcpTransport === 'streamable_http') {
      lines.push('      transport: streamable_http')
      lines.push(`      url: ${yamlString(draft.mcpUrl.trim())}`)
      if (draft.authMode === 'bearer') {
        lines.push('      auth:')
        lines.push('        type: bearer')
        lines.push('        from: input')
        lines.push(`        key: ${draft.tokenKey.trim()}`)
      }
    } else {
      lines.push('      transport: stdio')
      lines.push(`      command: ${yamlString(draft.mcpCommand.trim())}`)
      const args = splitArgs(draft.mcpArgs)
      if (args.length > 0) {
        lines.push('      args:')
        for (const arg of args) lines.push(`        - ${yamlString(arg)}`)
      }
    }
  }

  return `${lines.join('\n')}\n`
}

function pushSecretInput(lines: string[], indent: string, key: string) {
  lines.push(`${indent}inputs:`)
  lines.push(`${indent}  ${key}:`)
  lines.push(`${indent}    kind: secret`)
}

function validateDraft(draft: SourceDraft): string[] {
  const errors: string[] = []
  const name = draft.name.trim()
  if (!name) {
    errors.push('Source name is required.')
  } else if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    errors.push(
      'Source name must start with a letter or underscore and use only letters, numbers, and underscores.',
    )
  }

  if (draft.kind === 'openapi') {
    if (!isHttpsUrl(draft.openapiUrl.trim())) {
      errors.push('OpenAPI spec URL must be HTTPS.')
    }
    if (draft.openapiBaseUrl.trim() && !isHttpUrl(draft.openapiBaseUrl.trim())) {
      errors.push('Base URL override must be a valid HTTP or HTTPS URL.')
    }
    if ((draft.authMode === 'bearer' || draft.authMode === 'header') && !draft.headerName.trim()) {
      errors.push('Header name is required for OpenAPI auth.')
    }
  } else if (draft.mcpTransport === 'streamable_http') {
    if (!isAllowedMcpHttpUrl(draft.mcpUrl.trim())) {
      errors.push('MCP Streamable HTTP URL must be HTTPS, or HTTP on localhost.')
    }
  } else if (!draft.mcpCommand.trim()) {
    errors.push('MCP stdio command is required.')
  }

  if (needsSecret(draft)) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(draft.tokenKey.trim())) {
      errors.push('Secret input key must use letters, numbers, and underscores.')
    }
    if (!draft.tokenValue.trim()) {
      errors.push(
        'Token is required before import because Coral validates declared required secrets before materialization.',
      )
    }
  }

  return errors
}

function yamlString(value: string): string {
  return JSON.stringify(value)
}

function splitArgs(value: string): string[] {
  const matches = value.matchAll(/"([^"]*)"|'([^']*)'|[^\s]+/g)
  return Array.from(matches, (match) => match[1] ?? match[2] ?? match[0]).filter(Boolean)
}

function isHttpsUrl(value: string): boolean {
  try {
    return new URL(value).protocol === 'https:'
  } catch {
    return false
  }
}

function isHttpUrl(value: string): boolean {
  try {
    const protocol = new URL(value).protocol
    return protocol === 'http:' || protocol === 'https:'
  } catch {
    return false
  }
}

function isAllowedMcpHttpUrl(value: string): boolean {
  try {
    const url = new URL(value)
    if (url.protocol === 'https:') return true
    if (url.protocol !== 'http:') return false
    return ['localhost', '127.0.0.1', '::1'].includes(url.hostname)
  } catch {
    return false
  }
}

function validationSummary(response: Awaited<ReturnType<typeof validateSource>>): string {
  return `${response.tables.length} tables, ${response.tableFunctions.length} table functions, ${response.queryTests.length} query tests.`
}
