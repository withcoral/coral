import { useRef } from 'react'
import { Form, useNavigate, useRevalidator } from 'react-router'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { SpinningButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Dialog } from '@/wax/components'

import { OAuthProgressDialog } from '@/components/sources/install/oauth-progress-dialog'
import { oauthActionLabel, useOAuthInstallFlow } from '@/lib/source-oauth-install-flow'
import type { CatalogEntry } from '@/lib/sources'
import { routePath } from '@/routing/routemap'

import { SourceInputRows, useSourceInputCollection } from './source-input-collection'
import { formatFieldName, SourceError, SourceIdentityHeader } from './source-presentation'

export function SourceInstallDialog({
  actionError,
  entry,
  fetchOAuthInstall = fetch,
  onOAuthInstallComplete,
  open,
  openAuthorization = (url) => window.open(url, '_blank', 'noopener,noreferrer'),
  onOpenChange,
  submitting,
  workspaceId,
}: {
  actionError?: string | null
  entry: CatalogEntry | null
  fetchOAuthInstall?: typeof fetch
  onOAuthInstallComplete?: () => Promise<void> | void
  open: boolean
  openAuthorization?: (url: string) => unknown
  onOpenChange: (open: boolean) => void
  submitting?: boolean
  workspaceId: string
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="l">
          {entry ? (
            <SourceInstallDialogContent
              actionError={actionError}
              entry={entry}
              fetchOAuthInstall={fetchOAuthInstall}
              key={entry.name}
              onOAuthInstallComplete={onOAuthInstallComplete}
              onCancel={() => onOpenChange(false)}
              openAuthorization={openAuthorization}
              submitting={submitting ?? false}
              workspaceId={workspaceId}
            />
          ) : null}
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function SourceInstallDialogContent({
  actionError,
  entry,
  fetchOAuthInstall,
  onOAuthInstallComplete,
  onCancel,
  openAuthorization,
  submitting,
  workspaceId,
}: {
  actionError?: string | null
  entry: CatalogEntry
  fetchOAuthInstall: typeof fetch
  onOAuthInstallComplete?: () => Promise<void> | void
  onCancel: () => void
  openAuthorization: (url: string) => unknown
  submitting: boolean
  workspaceId: string
}) {
  const navigate = useNavigate()
  const revalidator = useRevalidator()
  const formRef = useRef<HTMLFormElement>(null)
  const oauth = useOAuthInstallFlow({
    fetchOAuthInstall,
    openAuthorization,
    onComplete: async (_, signal) => {
      await revalidator.revalidate()
      if (signal.aborted) return
      await (onOAuthInstallComplete
        ? onOAuthInstallComplete()
        : navigate(routePath('workspaceSources', { workspaceId })))
    },
  })
  const collection = useSourceInputCollection(entry.inputSpecs ?? null)
  const oauthBusy = oauth.busy
  const busy = submitting || oauthBusy
  const usesOAuth = collection.usesOAuth

  function cancel() {
    oauth.cancel()
    onCancel()
  }

  async function submitOAuthInstall() {
    if (!formRef.current || oauthBusy) return
    await oauth.start(oauthInstallEndpoint(workspaceId, entry.name), new FormData(formRef.current))
  }

  function handleSubmit(event: React.FormEvent<HTMLFormElement>) {
    if (!usesOAuth) return
    event.preventDefault()
    void submitOAuthInstall()
  }

  return (
    <Form method="post" ref={formRef} onSubmit={handleSubmit}>
      <input type="hidden" name="_intent" value="install" />
      <input type="hidden" name="name" value={entry.name} />

      <SourceIdentityHeader
        description={entry.description}
        name={entry.name}
        origin={entry.origin}
        version={entry.version}
      />

      <SourceInputRows collection={collection} disabled={busy} submitLabel="Add source" />

      <OAuthProgressDialog
        error={oauth.error}
        inputLabel={formatFieldName}
        onCancel={oauth.cancel}
        progress={oauth.progress}
      />

      {actionError ? <SourceError>{actionError}</SourceError> : null}

      <Dialog.Actions>
        <ButtonContainer disabled={submitting} onClick={cancel} size="32" variant="bare">
          <ButtonText>Cancel</ButtonText>
        </ButtonContainer>
        <ButtonContainer
          disabled={busy || !collection.canSubmit}
          onClick={usesOAuth ? () => void submitOAuthInstall() : undefined}
          size="32"
          type={usesOAuth ? 'button' : 'submit'}
          variant="primary"
        >
          {busy ? <SpinningButtonIcon name="Loader" /> : null}
          <ButtonText>
            {submitting
              ? 'Adding…'
              : oauthActionLabel(oauth.progress, { busy: 'Adding…', idle: 'Add source' })}
          </ButtonText>
        </ButtonContainer>
      </Dialog.Actions>
    </Form>
  )
}

function oauthInstallEndpoint(workspaceId: string, name: string): string {
  return `${routePath('workspaceSource', { sourceName: name, workspaceId })}/oauth-install`
}
