import { type FormEvent, type RefObject, useEffect, useRef } from 'react'
import { Form, useNavigation } from 'react-router'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { SpinningButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Dialog } from '@/wax/components'
import { Pill } from '@/wax/components/pill'
import { Typography } from '@/wax/components/typography'

import { OAuthProgressDialog } from '@/components/sources/install/oauth-progress-dialog'
import { oauthActionLabel, useOAuthInstallFlow } from '@/lib/source-oauth-install-flow'
import type { CatalogEntry } from '@/lib/sources'

import type { DiscardGuard } from './source-add'
import * as styles from './source-import.css'
import { SourceInputRows, useSourceInputCollection } from './source-input-collection'
import {
  formatFieldName,
  SourceError,
  SourceHeader,
  SourceIdentityHeader,
} from './source-presentation'

const STEP_COUNT = 2

/**
 * The configure step, mounted per described manifest. It owns the collected
 * input values, so remounting it is what clears one manifest's credentials
 * before the next manifest's form reuses the same input keys.
 */
export function SourceImportConfigureForm({
  discardRef,
  entry,
  fetchOAuthImport,
  manifestYaml,
  oauthImportPath,
  onBack,
  onOAuthImportComplete,
  openAuthorization,
  requestCancel,
}: {
  discardRef: RefObject<DiscardGuard | null>
  entry: CatalogEntry
  fetchOAuthImport: typeof fetch
  manifestYaml: string
  oauthImportPath: string
  onBack: () => void
  onOAuthImportComplete?: (name: string, signal: AbortSignal) => Promise<void> | void
  openAuthorization: (url: string) => unknown
  requestCancel: () => void
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

  // A manifest that asks for nothing costs nothing to close, so only typed
  // credentials make this branch worth confirming.
  useEffect(() => {
    discardRef.current = {
      discard: oauth.cancel,
      isDirty: () => Object.values(collection.values).some((value) => value.trim().length > 0),
    }
    return () => {
      discardRef.current = null
    }
  })

  async function submitOAuthImport() {
    if (!formRef.current || blocked) return
    await oauth.start(oauthImportPath, new FormData(formRef.current))
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
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

      <StepHeader />
      <SourceIdentityHeader
        description={entry.description}
        name={entry.name}
        origin={entry.origin}
        version={entry.version}
      />
      {entry.installed ? (
        <SourceError>A source named {entry.name} is already configured.</SourceError>
      ) : null}

      <SourceInputRows collection={collection} disabled={importing} submitLabel="Add source" />

      <OAuthProgressDialog
        error={oauth.error}
        inputLabel={formatFieldName}
        onCancel={oauth.cancel}
        progress={oauth.progress}
      />

      <Dialog.Actions>
        <ButtonContainer disabled={submitting} onClick={requestCancel} size="32" variant="bare">
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
              ? 'Adding…'
              : oauthActionLabel(oauth.progress, {
                  busy: 'Adding…',
                  idle: 'Add source',
                })}
          </ButtonText>
        </ButtonContainer>
      </Dialog.Actions>
    </Form>
  )
}

function StepHeader() {
  return (
    <SourceHeader
      className={styles.header}
      pill={
        <Pill as="span" color="graySubtle">
          Step {STEP_COUNT}/{STEP_COUNT}
        </Pill>
      }
      title={<Typography.HeadingMedium as="span">Add source</Typography.HeadingMedium>}
    />
  )
}
