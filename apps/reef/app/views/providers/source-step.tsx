import { RequestHeaders } from './request-headers'
import { useEffect, useId, useState, type KeyboardEvent, type ReactNode } from 'react'
import { flushSync } from 'react-dom'
import { useFetcher } from 'react-router'
import { Button, Card, Inputs, Tabs, Typography } from '@/wax/components'
import { Picker } from '@/wax/components/picker'
import { TextField } from './fields'
import { Logo } from '@/wax/components/logo'
import { Fields } from '@/views/management/forms'
import {
  OPENAPI_DOCUMENT_MAX_BYTES,
  OPENAPI_DOCUMENT_SIZE_LABEL,
} from '@/lib/provider-input-limits'
import {
  filledSourceUrl,
  recipeSource,
  recipeNamespace,
  type SourceChoice,
} from '@/lib/provider-recipes/choice'
import type { FormContext, StepperResult } from './types'
import * as styles from './providers.css'

type Kind = 'mcp' | 'openapi'
export type SourceTab = 'catalog' | 'custom'

/** What the action does, in the words each kind uses for it. */
const SOURCE_VERB = { mcp: 'Connect', openapi: 'Read' } as const
const DOCUMENT_FILE_TYPES =
  '.json,.yaml,.yml,.gz,application/json,application/yaml,application/gzip'

/** What each kind calls its address, and what that address points to. */
const SOURCE_URL = {
  mcp: {
    label: 'MCP server URL',
    placeholder: 'https://example.com/mcp',
    help: 'A streamable HTTP endpoint.',
  },
  openapi: {
    label: 'OpenAPI URL',
    placeholder: 'https://example.com/openapi.json',
    help: 'A JSON or YAML document.',
  },
} as const

/** A recipe's bundled document, shown where its OpenAPI URL would be. */
const BUNDLED_DOCUMENT = {
  label: 'OpenAPI document',
  placeholder: '',
  help: 'A document that comes with this provider.',
} as const

function validUrl(value: string) {
  try {
    const parsed = new URL(value.trim())
    return parsed.protocol === 'https:' || parsed.protocol === 'http:'
  } catch {
    return false
  }
}

/** Enter reads the address it was pressed in, not the form's first action. */
function enterReads(event: KeyboardEvent<HTMLDivElement>) {
  if (event.key !== 'Enter' || !(event.target instanceof HTMLInputElement)) return
  event.preventDefault()
  event.currentTarget.querySelector<HTMLButtonElement>('button[type="submit"]')?.click()
}

/**
 * Where the provider is: an MCP server URL, or an OpenAPI document by URL or
 * file. The person states which of the two it is, because nothing else can. A
 * path rule would route `api.githubcopilot.com/mcp/x/all/readonly` to the
 * document side, and a refusal settles nothing either: `mcp.linear.app/mcp`
 * answers a handshake with 401 until it holds a token, exactly as a protected
 * document host answers a fetch. Reading never publishes a provider. The
 * namespace comes later, in Configuration, so this form is the same in a
 * launcher dialog and on the page.
 */
export function SourceStep({
  form,
  pending,
  result,
  hidden,
  action,
  recipe = null,
  formId,
  catalog,
  fill = false,
  tab,
  onTabChange,
  onReading,
}: {
  form: FormContext
  pending: boolean
  result?: StepperResult
  hidden?: ReactNode
  /** Where the read posts; the current URL when absent. */
  action?: string
  /** A recipe already chosen, shown read-only with the header values it asks for. */
  recipe?: SourceChoice | null
  /** Names the form so a button outside it, such as in a dialog footer, can submit it. */
  formId?: string
  /** The catalog, shown as the first tab and chosen by default. */
  catalog?: ReactNode
  /** The caller bounds the height, so the catalog tab scrolls in what is left. */
  fill?: boolean
  /** The open tab, when the caller needs to change it, such as from the catalog. */
  tab?: SourceTab
  onTabChange?: (tab: SourceTab) => void
  /** Told when this form's read starts and ends, so a dialog can stay open for it. */
  onReading?: (reading: boolean) => void
}) {
  const [ownTab, setOwnTab] = useState<SourceTab>(catalog && !recipe ? 'catalog' : 'custom')
  const shownTab = tab ?? ownTab
  // Both custom kinds show at once; the one last typed in, or whose action was
  // pressed, is the one the form reads.
  const [kind, setKind] = useState<Kind>(recipe?.kind ?? 'mcp')
  const [requestHeaders, setRequestHeaders] = useState(recipe?.requestHeaders ?? [])
  const [urls, setUrls] = useState<Record<Kind, string>>(() => ({
    mcp: '',
    openapi: '',
    ...(recipe && 'url' in recipe ? { [recipe.kind]: recipe.url } : {}),
  }))
  const [sourceAnswers, setSourceAnswers] = useState<Record<string, string>>(() =>
    Object.fromEntries(
      (recipe?.sourceInputs ?? []).map((input) => [input.name, input.defaultValue]),
    ),
  )
  const url = recipe && 'url' in recipe ? filledSourceUrl(recipe, sourceAnswers) : urls[kind]
  const [file, setFile] = useState<File | null>(null)
  const [problem, setProblem] = useState('')
  const [dismissedResult, setDismissedResult] = useState<StepperResult>()
  const id = useId()
  // Reading with a fetcher keeps this form mounted: Coral redirects on success,
  // and a refusal comes back here with the address still typed.
  const fetcher = useFetcher<StepperResult>()
  const failure = fetcher.data?.error ?? (result === dismissedResult ? '' : result?.error) ?? ''
  const fetching = fetcher.state !== 'idle'
  const reading = pending || fetching
  useEffect(() => {
    onReading?.(fetching)
    // A form that goes away mid-read must not leave its dialog held open.
    return () => onReading?.(false)
  }, [fetching, onReading])
  const clearFailure = () => {
    fetcher.reset()
    setDismissedResult(result)
  }
  // A recipe states its own kind and is taken at its word; otherwise the chosen
  // tab decides, and only the document side can hold a file.
  const source = recipe ? recipeSource(recipe) : kind === 'mcp' ? 'mcp' : file ? 'file' : 'url'
  // The picker reports drops, choices, and native form resets alike, so a
  // cleared selection has to clear the problem it raised too.
  const acceptFile = (next: File | null) => {
    clearFailure()
    if (next) setKind('openapi')
    setFile(next)
    setProblem(
      next && next.size > OPENAPI_DOCUMENT_MAX_BYTES
        ? `Choose a file up to ${OPENAPI_DOCUMENT_SIZE_LABEL}.`
        : '',
    )
  }
  const choose = (next: SourceTab) => {
    clearFailure()
    setOwnTab(next)
    onTabChange?.(next)
    setProblem('')
  }
  // The read goes to the kind whose action was pressed. Committing that before
  // the submit lets the form carry the right source and address.
  const readAs = (of: Kind) => {
    if (of === kind) return
    clearFailure()
    setProblem('')
    flushSync(() => setKind(of))
  }
  const validate = () => {
    if (source === 'recipe') return ''
    if (recipe?.sourceInputs?.length && !url) return 'Enter the source details.'
    if (source === 'mcp') return validUrl(url) ? '' : 'Enter the MCP server URL.'
    if (source === 'file')
      return file && file.size > OPENAPI_DOCUMENT_MAX_BYTES
        ? `Choose a file up to ${OPENAPI_DOCUMENT_SIZE_LABEL}.`
        : ''
    if (!url.trim()) return 'Enter the OpenAPI URL, or choose a file.'
    return validUrl(url) ? '' : 'Enter an OpenAPI URL that starts with http:// or https://.'
  }
  // One line under the field carries either what the field takes or what went
  // wrong with it, so a refusal never adds a banner above the fold and never
  // changes the block's height.
  const message = problem || failure
  const urlField = (of: Kind) => {
    const active = of === kind
    const failed = active && Boolean(message)
    const bundled = recipe && 'document' in recipe ? recipe.document : null
    const text = bundled ? BUNDLED_DOCUMENT : SOURCE_URL[of]
    return (
      <div className={styles.sourceField} onKeyDown={enterReads}>
        <Typography.BodySmallStrong as="label" htmlFor={`${id}-${of}-url`}>
          {text.label}
        </Typography.BodySmallStrong>
        <div className={styles.fieldWithAction}>
          <div className={styles.grow}>
            <Inputs.TextInput
              id={`${id}-${of}-url`}
              name={active && !bundled ? 'sourceUrl' : undefined}
              type={bundled ? 'text' : 'url'}
              readOnly={Boolean(recipe)}
              value={bundled ?? (recipe ? url : urls[of])}
              invalid={active && Boolean(message) && source !== 'file'}
              ariaDescribedBy={`${id}-${of}-help`}
              placeholder={text.placeholder}
              onChange={(next) => {
                clearFailure()
                setKind(of)
                setUrls((current) => ({ ...current, [of]: next }))
                if (of === 'openapi') setFile(null)
                setProblem('')
              }}
            />
          </div>
          {/* The action belongs with the address it reads. */}
          <Button.TextButton
            type="submit"
            form={formId}
            disabled={reading}
            busy={reading && active}
            className={styles.readAction}
            onClick={() => readAs(of)}
          >
            {SOURCE_VERB[of]}
          </Button.TextButton>
        </div>
        <Typography.BodySmall
          as="p"
          id={`${id}-${of}-help`}
          variant={failed ? 'error' : 'tertiary'}
          role={failed ? 'alert' : undefined}
        >
          {failed ? message : text.help}
        </Typography.BodySmall>
      </div>
    )
  }
  // Only a recipe can ask for header values before the read. A custom server
  // takes its headers in Configuration.
  const headers = requestHeaders.length > 0 && (
    <RequestHeaders value={requestHeaders} onChange={setRequestHeaders} />
  )
  // An MCP server first, as most providers are; an OpenAPI document under it,
  // by address or as a file. Each kind is its own card, so what belongs to which
  // is plain at a glance.
  const custom = (
    <div className={styles.stack}>
      <Card.Container as="section" aria-label="MCP server" className={styles.sourceKind}>
        {urlField('mcp')}
      </Card.Container>
      <div className={styles.orDivider}>
        <Typography.BodySmall variant="tertiary">or</Typography.BodySmall>
      </div>
      <Card.Container as="section" aria-label="OpenAPI document" className={styles.sourceKind}>
        {urlField('openapi')}
        {/* A typed URL drops the chosen file, so the picker is remounted to drop
          the native file with it. The file stays chosen while the MCP server is
          read, but only a file read sends it. */}
        <Inputs.FileInput
          key={urls.openapi}
          id={`${id}-file`}
          ariaLabel="OpenAPI file"
          name={source === 'file' ? 'document' : undefined}
          accept={DOCUMENT_FILE_TYPES}
          dropLabel={`Drop an OpenAPI document here, up to ${OPENAPI_DOCUMENT_SIZE_LABEL}.`}
          disabled={reading}
          invalid={Boolean(problem) && source === 'file'}
          onChange={acceptFile}
        />
      </Card.Container>
    </div>
  )
  return (
    <fetcher.Form
      id={formId}
      method="post"
      action={action}
      encType="multipart/form-data"
      noValidate
      className={fill ? styles.sourceFill : styles.stack}
      aria-busy={reading}
      aria-label="New source"
      onSubmit={(event) => {
        const found = validate()
        setProblem(found)
        if (found || reading) event.preventDefault()
      }}
    >
      <Fields form={form} intent="prepare" />
      {hidden}
      <input type="hidden" name="source" value={source} />
      {recipe?.id && <input type="hidden" name="namespace" value={recipeNamespace(recipe)} />}
      {recipe?.id && <input type="hidden" name="recipe" value={recipe.id} />}
      {recipe ? (
        <>
          <div className={styles.chosenRecipe}>
            <Logo name={recipe.displayName} icons={recipe.icons} />
            <Typography.BodyStrong as="h3" variant="primary">
              {recipe.displayName}
            </Typography.BodyStrong>
          </div>
          {recipe.sourceInputs?.map((input) => (
            <div key={input.name} className={styles.field}>
              {input.choices.length > 0 ? (
                <>
                  <Typography.BodySmallStrong as="span">{input.label}</Typography.BodySmallStrong>
                  <Picker
                    ariaLabel={input.label}
                    label={input.label}
                    value={sourceAnswers[input.name] ?? ''}
                    options={input.choices.map((choice) => ({ label: choice, value: choice }))}
                    onChange={(answer) => {
                      clearFailure()
                      setProblem('')
                      setSourceAnswers((current) => ({ ...current, [input.name]: answer }))
                    }}
                  />
                  {input.description && (
                    <Typography.BodySmall as="p" variant="tertiary">
                      {input.description}
                    </Typography.BodySmall>
                  )}
                </>
              ) : (
                <TextField
                  label={input.label}
                  description={input.description}
                  value={sourceAnswers[input.name] ?? ''}
                  onChange={(answer) => {
                    clearFailure()
                    setProblem('')
                    setSourceAnswers((current) => ({ ...current, [input.name]: answer }))
                  }}
                />
              )}
            </div>
          ))}
          {urlField(recipe.kind)}
          {recipe.kind === 'mcp' && headers}
        </>
      ) : catalog ? (
        <Tabs.Root
          className={fill ? styles.sourceTabsFill : undefined}
          value={shownTab}
          onValueChange={(next) => choose(next as SourceTab)}
        >
          <Tabs.List aria-label="Source" inset="none" className={styles.sourceTabs}>
            <Tabs.Tab value="catalog" disabled={reading}>
              Catalog
            </Tabs.Tab>
            <Tabs.Tab value="custom" disabled={reading}>
              Add custom
            </Tabs.Tab>
            <Tabs.Indicator />
          </Tabs.List>
          {/* Both panels stay mounted so each keeps what was typed, and each
              is only as tall as its own content. */}
          <div className={fill ? styles.sourceSlotFill : styles.sourceSlot}>
            <Tabs.Panel
              keepMounted
              value="catalog"
              className={styles.sourcePanel}
              data-hidden={shownTab === 'catalog' ? undefined : ''}
            >
              {catalog}
            </Tabs.Panel>
            <Tabs.Panel
              keepMounted
              value="custom"
              className={styles.sourcePanel}
              data-hidden={shownTab === 'custom' ? undefined : ''}
            >
              {custom}
            </Tabs.Panel>
          </div>
        </Tabs.Root>
      ) : (
        custom
      )}
      {source === 'mcp' && (
        <input type="hidden" name="requestHeaders" value={JSON.stringify(requestHeaders)} />
      )}
    </fetcher.Form>
  )
}
