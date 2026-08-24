import { useCallback, useEffect, useRef, useState } from 'react'
import { Form, useFetcher, useNavigation } from 'react-router'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { Icon as ButtonIcon, SpinningButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Dialog } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { KeyboardHint } from '@/wax/components/keyboard-hint'
import { Pill } from '@/wax/components/pill'
import { addToast } from '@/wax/components/toast'
import { Typography } from '@/wax/components/typography'

import { OAuthProgressDialog } from '@/components/sources/install/oauth-progress-dialog'
import type { SourceDescribeData } from '@/lib/source-describe'
import { oauthActionLabel, useOAuthInstallFlow } from '@/lib/source-oauth-install-flow'
import type { CatalogEntry } from '@/lib/sources'

import * as styles from './source-import.css'
import { SourceInputRows, useSourceInputCollection } from './source-input-collection'
import {
  formatFieldName,
  SourceError,
  SourceHeader,
  SourceIdentityHeader,
} from './source-presentation'

const STEP_COUNT = 2
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

export function SourceImportDialog({
  describePath,
  fetchOAuthImport = fetch,
  oauthImportPath,
  onOAuthImportComplete,
  open,
  openAuthorization = (url) => window.open(url, '_blank', 'noopener,noreferrer'),
  onOpenChange,
}: {
  describePath: string
  fetchOAuthImport?: typeof fetch
  oauthImportPath: string
  onOAuthImportComplete?: (name: string, signal: AbortSignal) => Promise<void> | void
  open: boolean
  openAuthorization?: (url: string) => unknown
  onOpenChange: (open: boolean) => void
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="m">
          <SourceImportDialogContent
            describePath={describePath}
            fetchOAuthImport={fetchOAuthImport}
            oauthImportPath={oauthImportPath}
            onCancel={() => onOpenChange(false)}
            onOAuthImportComplete={onOAuthImportComplete}
            openAuthorization={openAuthorization}
          />
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function SourceImportDialogContent({
  describePath,
  fetchOAuthImport,
  oauthImportPath,
  onCancel,
  onOAuthImportComplete,
  openAuthorization,
}: {
  describePath: string
  fetchOAuthImport: typeof fetch
  oauthImportPath: string
  onCancel: () => void
  onOAuthImportComplete?: (name: string, signal: AbortSignal) => Promise<void> | void
  openAuthorization: (url: string) => unknown
}) {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [step, setStep] = useState(0)
  const [manifestYaml, setManifestYaml] = useState('')
  // Counts describes rather than naming one, so the configure step remounts for
  // every manifest. Two manifests can declare the same source name, and a
  // remount is what keeps one manifest's credentials out of the next one's form.
  const [describeCount, setDescribeCount] = useState(0)
  // Names the file being read, so the wait says which manifest it is about. A
  // pasted manifest has no name.
  const [fileName, setFileName] = useState<string | null>(null)
  const [dropping, setDropping] = useState(false)
  const describe = useFetcher<SourceDescribeData>()

  const describing = describe.state !== 'idle'
  const entry: CatalogEntry | null =
    describe.data?.status === 'success' && !describing ? describe.data.entry : null

  // A described manifest resolves as fetcher state rather than as an event, so
  // both outcomes land here. The ref keys on the result object, which is new per
  // submit, so describing the same manifest twice reports it twice.
  const settledRef = useRef<SourceDescribeData | null>(null)
  useEffect(() => {
    const result = describe.data
    if (!result || settledRef.current === result) return
    settledRef.current = result
    if (result.status === 'error') {
      addToast('error', {
        description: result.message,
        durationMs: IMPORT_ERROR_TOAST_MS,
        title: 'Coral could not read that manifest',
      })
      return
    }
    setStep(1)
  }, [describe.data])

  // Nothing in this dialog is editable, so the manifest text goes straight to the
  // server, and the described name, version, and inputs are all the user sees of
  // it. That keeps a 4.6 MB generated manifest as cheap as a hand-written one.
  const describeManifest = useCallback(
    (text: string, from: string | null) => {
      setManifestYaml(text)
      setFileName(from)
      setDescribeCount((previous) => previous + 1)
      describe.submit({ manifest_yaml: text }, { action: describePath, method: 'post' })
    },
    [describe, describePath],
  )

  async function readManifestFile(file: File | undefined) {
    if (!file) return

    if (file.size > MAX_MANIFEST_BYTES) {
      reportFileError(`${file.name} is too large to be a source manifest.`)
      return
    }
    try {
      describeManifest(await file.text(), file.name)
    } catch (cause) {
      reportFileError(cause instanceof Error ? cause.message : `Could not read ${file.name}.`)
    }
  }

  function pickManifestFile(input: HTMLInputElement) {
    const file = input.files?.[0]
    // Reset first so picking the same file twice still fires a change event.
    input.value = ''
    void readManifestFile(file)
  }

  function dropManifestFile(event: React.DragEvent) {
    event.preventDefault()
    setDropping(false)
    if (describing) return
    void readManifestFile(event.dataTransfer.files[0])
  }

  // The first step has nothing to type into, so a paste there means "this text is
  // my manifest", the same way a dropped file means "this file is".
  useEffect(() => {
    if (step !== 0 || describing) return
    function handlePaste(event: ClipboardEvent) {
      const text = event.clipboardData?.getData('text/plain') ?? ''
      if (text.trim().length === 0) return
      event.preventDefault()
      describeManifest(text, null)
    }
    document.addEventListener('paste', handlePaste)
    return () => document.removeEventListener('paste', handlePaste)
  }, [describeManifest, describing, step])

  return (
    <div className={styles.dialogContent}>
      <StepHeader step={0} />

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
            <Typography.Body>Reading {fileName ?? 'the pasted manifest'}…</Typography.Body>
          </>
        ) : (
          <>
            <Icon color="secondary" name="FileCode" size="30" />
            <Typography.Body>Drop a manifest file here</Typography.Body>
            <Typography.BodySmall className={styles.manifestDropHint} variant="secondary">
              or paste one with <KeyboardHint shortcut="mod+v" />
            </Typography.BodySmall>
            <ButtonContainer
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
      <Dialog.Actions>
        <ButtonContainer disabled={describing} onClick={onCancel} size="32" variant="bare">
          <ButtonText>Cancel</ButtonText>
        </ButtonContainer>
      </Dialog.Actions>

      <Dialog.Root
        open={step >= 1 && entry !== null}
        onOpenChange={(next) => {
          if (!next) setStep(0)
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
                onBack={() => setStep(0)}
                onCancel={onCancel}
                onOAuthImportComplete={onOAuthImportComplete}
                openAuthorization={openAuthorization}
              />
            ) : null}
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
    </div>
  )
}

/**
 * The configure step, mounted per described manifest. It owns the collected
 * input values, so remounting it is what clears one manifest's credentials
 * before the next manifest's form reuses the same input keys.
 */
function SourceImportConfigureForm({
  entry,
  fetchOAuthImport,
  manifestYaml,
  oauthImportPath,
  onBack,
  onCancel,
  onOAuthImportComplete,
  openAuthorization,
}: {
  entry: CatalogEntry
  fetchOAuthImport: typeof fetch
  manifestYaml: string
  oauthImportPath: string
  onBack: () => void
  onCancel: () => void
  onOAuthImportComplete?: (name: string, signal: AbortSignal) => Promise<void> | void
  openAuthorization: (url: string) => unknown
}) {
  const formRef = useRef<HTMLFormElement>(null)
  const navigation = useNavigation()
  const oauth = useOAuthInstallFlow({
    fetchOAuthInstall: fetchOAuthImport,
    openAuthorization,
    onComplete: onOAuthImportComplete ?? (() => {}),
  })
  const collection = useSourceInputCollection(entry.inputSpecs ?? null)

  // A successful import answers with a redirect, so navigation goes on to
  // `loading` while this dialog is still mounted. Treating only `submitting` as
  // busy re-enables the button in that gap and lets a second import through.
  const submitting = navigation.state !== 'idle'
  const importing = submitting || oauth.busy
  // Pressing Enter in a field submits the form whatever the button says, so the
  // rule that disables the button has to hold here too.
  const blocked = importing || !collection.canSubmit || entry.installed

  function cancel() {
    oauth.cancel()
    onCancel()
  }

  async function submitOAuthImport() {
    if (!formRef.current || blocked) return
    await oauth.start(oauthImportPath, new FormData(formRef.current))
  }

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    if (blocked) {
      event.preventDefault()
      return
    }
    if (!collection.usesOAuth) return
    event.preventDefault()
    void submitOAuthImport()
  }

  return (
    // The form lives here because the credential inputs render here: a
    // portalled popup sits outside any outer form.
    <Form className={styles.dialogContent} method="post" onSubmit={handleSubmit} ref={formRef}>
      <input type="hidden" name="_intent" value="import" />
      <input type="hidden" name="name" value={entry.name} />
      <input type="hidden" name="manifest_yaml" value={manifestYaml} />

      <StepHeader step={1} />
      <SourceIdentityHeader
        description={entry.description}
        name={entry.name}
        origin={entry.origin}
        version={entry.version}
      />
      {entry.installed ? (
        <SourceError>A source named {entry.name} is already configured.</SourceError>
      ) : null}

      <SourceInputRows collection={collection} disabled={importing} submitLabel="Import source" />

      <OAuthProgressDialog
        error={oauth.error}
        inputLabel={formatFieldName}
        onCancel={oauth.cancel}
        progress={oauth.progress}
      />

      <Dialog.Actions>
        <ButtonContainer disabled={submitting} onClick={cancel} size="32" variant="bare">
          <ButtonText>Cancel</ButtonText>
        </ButtonContainer>
        <ButtonContainer disabled={importing} onClick={onBack} size="32" variant="secondary">
          <ButtonText>Back</ButtonText>
        </ButtonContainer>
        <ButtonContainer
          disabled={blocked}
          onClick={collection.usesOAuth ? () => void submitOAuthImport() : undefined}
          size="32"
          type={collection.usesOAuth ? 'button' : 'submit'}
          variant="primary"
        >
          {importing ? <SpinningButtonIcon name="Loader" /> : null}
          <ButtonText>
            {submitting
              ? 'Importing…'
              : oauthActionLabel(oauth.progress, {
                  busy: 'Importing…',
                  idle: 'Import source',
                })}
          </ButtonText>
        </ButtonContainer>
      </Dialog.Actions>
    </Form>
  )
}

function StepHeader({ step }: { step: number }) {
  return (
    <SourceHeader
      className={styles.header}
      pill={
        <Pill as="span" color="graySubtle">
          Step {step + 1}/{STEP_COUNT}
        </Pill>
      }
      title={<Typography.HeadingMedium as="span">Import source</Typography.HeadingMedium>}
    />
  )
}

function reportFileError(message: string) {
  addToast('error', {
    description: message,
    durationMs: IMPORT_ERROR_TOAST_MS,
    title: 'Could not read that file',
  })
}
