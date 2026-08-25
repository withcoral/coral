import { type DragEvent, type RefObject, useEffect, useId, useRef, useState } from 'react'
import { useFetcher } from 'react-router'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { Icon as ButtonIcon, SpinningButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Dialog } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { addToast } from '@/wax/components/toast'
import { Typography } from '@/wax/components/typography'

import type { SourceDescribeData } from '@/lib/source-describe'
import type { CatalogEntry } from '@/lib/sources'
import type { SourcesActionData } from '@/routes/sources-action'
import type { SourceDiscoveryData } from '@/routes/source-discovery'

import * as styles from './source-add.css'
import { SourceCreateFlow } from './source-create'
import { SourceImportConfigureForm } from './source-import'
import { SourceError, SourceField, SourceHeader } from './source-presentation'

const MANIFEST_FILE_TYPES = '.yaml,.yml'
/**
 * Generated DSL v4 manifests are large: the bundled `github` manifest is 4.6 MB
 * and `stripe` is 3 MB. Coral reads the manifest rather than showing it, so size
 * costs a read and a request, and this cap only rejects a pick that cannot be a
 * manifest at all. The server decodes at most
 * `SOURCE_REQUEST_MAX_MESSAGE_SIZE`, so keep the two in step.
 */
const MAX_MANIFEST_BYTES = 64 * 1024 * 1024
/**
 * Import errors carry parse positions and Rust type paths, so they need longer
 * than the default toast to read. Hovering holds the toast open past this.
 */
export const IMPORT_ERROR_TOAST_MS = 10_000

type DiscoveredSource = Extract<SourceDiscoveryData, { status: 'success' }>

/**
 * What a branch stands to lose when the dialog closes. The first step has only
 * a URL, so a branch that holds more than that registers its own answer and the
 * way to throw it away.
 */
export interface DiscardGuard {
  discard: () => void
  isDirty: () => boolean
}

/**
 * A source arrives either as a URL for Coral to inspect or as a manifest the
 * user already holds, so the first step offers both, and the way it is given
 * decides the steps that follow.
 */
export function SourceAddDialog({
  actionData,
  describePath,
  discoveryPath,
  fetchOAuthImport = fetch,
  oauthImportPath = discoveryPath.replace(/\/discover$/, '/oauth-import'),
  onOAuthImportComplete,
  open,
  openAuthorization = (url) => window.open(url, '_blank', 'noopener,noreferrer'),
  onOpenChange,
}: {
  actionData: SourcesActionData
  describePath: string
  discoveryPath: string
  fetchOAuthImport?: typeof fetch
  oauthImportPath?: string
  onOAuthImportComplete?: (name: string, signal: AbortSignal) => Promise<void> | void
  open: boolean
  openAuthorization?: (url: string) => unknown
  onOpenChange: (open: boolean) => void
}) {
  const requestCancelRef = useRef<() => void>(() => onOpenChange(false))

  return (
    <Dialog.Root
      open={open}
      onOpenChange={(nextOpen) => {
        if (!nextOpen) requestCancelRef.current()
      }}
    >
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="xl">
          {open ? (
            <SourceAddDialogContent
              actionData={actionData}
              describePath={describePath}
              discoveryPath={discoveryPath}
              fetchOAuthImport={fetchOAuthImport}
              oauthImportPath={oauthImportPath}
              onCancel={() => onOpenChange(false)}
              onOAuthImportComplete={onOAuthImportComplete}
              openAuthorization={openAuthorization}
              requestCancelRef={requestCancelRef}
            />
          ) : null}
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function SourceAddDialogContent({
  actionData,
  describePath,
  discoveryPath,
  fetchOAuthImport,
  oauthImportPath,
  onCancel,
  onOAuthImportComplete,
  openAuthorization,
  requestCancelRef,
}: {
  actionData: SourcesActionData
  describePath: string
  discoveryPath: string
  fetchOAuthImport: typeof fetch
  oauthImportPath: string
  onCancel: () => void
  onOAuthImportComplete?: (name: string, signal: AbortSignal) => Promise<void> | void
  openAuthorization: (url: string) => unknown
  requestCancelRef: RefObject<() => void>
}) {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const idUrl = useId()
  const [url, setUrl] = useState('')
  const [dropping, setDropping] = useState(false)
  const [confirmingCancel, setConfirmingCancel] = useState(false)
  // Names the file being read, so the wait says which manifest it is about.
  const [fileName, setFileName] = useState('')
  const [manifestYaml, setManifestYaml] = useState('')
  // Counts each branch rather than naming one, so the step after it remounts for
  // every URL and every manifest. Two of either can describe the same source
  // name, and a remount is what keeps one's credentials out of the next one's
  // form.
  const [discoveryCount, setDiscoveryCount] = useState(0)
  const [describeCount, setDescribeCount] = useState(0)
  const [branch, setBranch] = useState<'none' | 'url' | 'manifest'>('none')
  const discovery = useFetcher<SourceDiscoveryData>()
  const describe = useFetcher<SourceDescribeData>()

  const discovering = discovery.state !== 'idle'
  const describing = describe.state !== 'idle'
  const busy = discovering || describing
  const trimmedUrl = url.trim()
  const discoveryError =
    discovery.data?.status === 'error' && discovery.data.url === trimmedUrl
      ? discovery.data.message
      : null
  const discovered: DiscoveredSource | null =
    discovery.data?.status === 'success' && discovery.data.url === trimmedUrl
      ? discovery.data
      : null
  const entry: CatalogEntry | null =
    describe.data?.status === 'success' && !describing ? describe.data.entry : null

  // The create branch holds a draft the user can lose, so while it is mounted it
  // answers for what closing costs. Without it, a typed URL is all that is at
  // stake.
  const discardRef = useRef<DiscardGuard | null>(null)
  const requestCancel = () => {
    if (discardRef.current?.isDirty() ?? trimmedUrl.length > 0) {
      setConfirmingCancel(true)
      return
    }
    onCancel()
  }
  useEffect(() => {
    requestCancelRef.current = requestCancel
  })

  // A discovered URL resolves as fetcher state rather than as an event, so the
  // branch opens from here. The ref keys on the result object, which is new per
  // load, so inspecting the same URL twice opens the branch twice.
  const appliedDiscovery = useRef<SourceDiscoveryData | undefined>(undefined)
  useEffect(() => {
    const result = discovery.data
    if (!result || result === appliedDiscovery.current || result.status !== 'success') return
    appliedDiscovery.current = result
    if (branch !== 'none' || result.url !== url.trim()) return
    setDiscoveryCount((previous) => previous + 1)
    setBranch('url')
  }, [branch, discovery.data, url])

  // A described manifest settles the same way, and both of its outcomes land
  // here: an unreadable manifest is a toast, a readable one is the branch.
  const settledDescribe = useRef<SourceDescribeData | null>(null)
  useEffect(() => {
    const result = describe.data
    if (!result || settledDescribe.current === result) return
    settledDescribe.current = result
    if (result.status === 'error') {
      addToast('error', {
        description: result.message,
        durationMs: IMPORT_ERROR_TOAST_MS,
        title: 'Coral could not read that manifest',
      })
      return
    }
    setBranch('manifest')
  }, [describe.data])

  function inspectUrl() {
    if (!urlIsInspectable(trimmedUrl) || busy) return
    discovery.load(`${discoveryPath}?url=${encodeURIComponent(trimmedUrl)}`)
  }

  // Coral reads the manifest rather than showing it, so the text goes straight
  // to the server and the described name, version, and inputs are all the user
  // sees of it. That keeps a 4.6 MB generated manifest as cheap as a
  // hand-written one.
  async function readManifestFile(file: File | undefined) {
    if (!file) return

    if (file.size > MAX_MANIFEST_BYTES) {
      reportFileError(`${file.name} is too large to be a source manifest.`)
      return
    }
    let text: string
    try {
      text = await file.text()
    } catch (cause) {
      reportFileError(cause instanceof Error ? cause.message : `Could not read ${file.name}.`)
      return
    }
    setManifestYaml(text)
    setFileName(file.name)
    setDescribeCount((previous) => previous + 1)
    describe.submit({ manifest_yaml: text }, { action: describePath, method: 'post' })
  }

  function pickManifestFile(input: HTMLInputElement) {
    const file = input.files?.[0]
    // Reset first so picking the same file twice still fires a change event.
    input.value = ''
    void readManifestFile(file)
  }

  function dropManifestFile(event: DragEvent) {
    event.preventDefault()
    setDropping(false)
    if (busy) return
    void readManifestFile(event.dataTransfer.files[0])
  }

  return (
    <div className={styles.dialogContent}>
      <SourceHeader
        className={styles.header}
        title={<Typography.HeadingMedium as="span">Add source</Typography.HeadingMedium>}
      />

      <div className={styles.fieldGroup}>
        <SourceField
          className={styles.fieldItem}
          hint={
            <Typography.BodySmall variant="tertiary">
              Enter an OpenAPI document or streamable HTTP MCP endpoint.
            </Typography.BodySmall>
          }
          htmlFor={idUrl}
          label="Source URL"
        >
          <TextInput
            id={idUrl}
            onChange={setUrl}
            placeholder="https://example.com/openapi.yaml"
            value={url}
          />
        </SourceField>
      </div>

      <div className={styles.orDivider}>
        <Typography.BodySmall variant="tertiary">or</Typography.BodySmall>
      </div>

      <input
        accept={MANIFEST_FILE_TYPES}
        className={styles.hiddenFileInput}
        data-testid="manifest-file"
        onChange={(event) => pickManifestFile(event.currentTarget)}
        ref={fileInputRef}
        tabIndex={-1}
        type="file"
      />
      <div
        className={styles.manifestDropZone}
        data-dropping={dropping || undefined}
        onDragEnter={() => setDropping(true)}
        onDragLeave={() => setDropping(false)}
        onDragOver={(event) => event.preventDefault()}
        onDrop={dropManifestFile}
      >
        {describing ? (
          <>
            <SpinningButtonIcon name="Loader" />
            <Typography.Body>Reading {fileName}…</Typography.Body>
          </>
        ) : (
          <>
            <Icon color="secondary" name="FileCode" size="30" />
            <Typography.Body>Drop a manifest file here</Typography.Body>
            <ButtonContainer
              disabled={busy}
              onClick={() => fileInputRef.current?.click()}
              size="32"
              variant="secondary"
            >
              <ButtonIcon name="Upload" />
              <ButtonText>Choose a file</ButtonText>
            </ButtonContainer>
          </>
        )}
      </div>

      {discoveryError ? <SourceError>{discoveryError}</SourceError> : null}

      <Dialog.Actions>
        <ButtonContainer disabled={busy} onClick={requestCancel} size="32" variant="bare">
          <ButtonText>Cancel</ButtonText>
        </ButtonContainer>
        <ButtonContainer
          disabled={!urlIsInspectable(trimmedUrl) || busy}
          onClick={inspectUrl}
          size="32"
          variant="primary"
        >
          {discovering ? <SpinningButtonIcon name="Loader" /> : null}
          <ButtonText>{discovering ? 'Inspecting…' : 'Next'}</ButtonText>
        </ButtonContainer>
      </Dialog.Actions>

      {branch === 'url' && discovered ? (
        <SourceCreateFlow
          actionData={actionData}
          discardRef={discardRef}
          discovery={discovered}
          fetchOAuthImport={fetchOAuthImport}
          key={discoveryCount}
          oauthImportPath={oauthImportPath}
          onBack={() => setBranch('none')}
          onCancel={onCancel}
          onOAuthImportComplete={onOAuthImportComplete}
          openAuthorization={openAuthorization}
          requestCancel={requestCancel}
          url={trimmedUrl}
        />
      ) : null}

      <Dialog.Root
        open={branch === 'manifest' && entry !== null}
        onOpenChange={(next) => {
          if (!next) setBranch('none')
        }}
      >
        <Dialog.Portal>
          <Dialog.Popup size="l">
            {entry ? (
              <SourceImportConfigureForm
                entry={entry}
                fetchOAuthImport={fetchOAuthImport}
                key={describeCount}
                manifestYaml={manifestYaml}
                oauthImportPath={oauthImportPath}
                onBack={() => setBranch('none')}
                onCancel={onCancel}
                onOAuthImportComplete={onOAuthImportComplete}
                openAuthorization={openAuthorization}
              />
            ) : null}
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>

      <Dialog.Root open={confirmingCancel} onOpenChange={setConfirmingCancel}>
        <Dialog.Portal>
          <Dialog.Popup size="m">
            <Dialog.Title>Discard source draft?</Dialog.Title>
            <Dialog.Description>The information you entered will be lost.</Dialog.Description>
            <Dialog.Actions>
              <ButtonContainer
                onClick={() => setConfirmingCancel(false)}
                size="32"
                variant="secondary"
              >
                <ButtonText>Keep editing</ButtonText>
              </ButtonContainer>
              <ButtonContainer
                onClick={() => {
                  discardRef.current?.discard()
                  onCancel()
                }}
                size="32"
                variant="destructive"
              >
                <ButtonText>Discard</ButtonText>
              </ButtonContainer>
            </Dialog.Actions>
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
    </div>
  )
}

function urlIsInspectable(value: string): boolean {
  return value.startsWith('https://')
}

function reportFileError(message: string) {
  addToast('error', {
    description: message,
    durationMs: IMPORT_ERROR_TOAST_MS,
    title: 'Could not read that file',
  })
}
