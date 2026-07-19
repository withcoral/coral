import { useEffect, useMemo, useState } from 'react'
import { Form, useActionData, useNavigate, useNavigation } from 'react-router'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { SpinningButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import { Dialog } from '@/wax/components'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { Typography } from '@/wax/components/typography'

import type {
  CatalogEntry,
  CatalogSource,
  CatalogSourceInputSpec,
  SourceOriginLabel,
} from '@/lib/sources'
import type { action as sourceDetailAction } from '@/routes/source-detail'
import type { SourcesActionData } from '@/routes/sources-action'

import { formatSourceName, ProviderLogo } from '@/components/sources'
import * as styles from './source-detail.css'
import { SourceInstallDialog } from './source-install'

const SECRET_PLACEHOLDER = '••••••••'

const IMPORTED_EDIT_NOTICE =
  "Imported sources can't be edited here yet — re-import the source spec to change its credentials."

export function SourceDetailView({
  actionData,
  loaderData,
  sourcesPath,
  workspaceId,
}: {
  actionData: SourcesActionData | undefined
  loaderData: {
    entry: CatalogEntry
    loadError: string | null
  }
  sourcesPath: string
  workspaceId: string
}) {
  const navigate = useNavigate()
  const navigation = useNavigation()
  const actionError = actionData?.status === 'error' ? actionData : null
  const pendingIntent = formValue(navigation.formData, '_intent')
  const pendingName = formValue(navigation.formData, 'name')
  const entry = loaderData.entry

  if (!entry.installed) {
    return (
      <SourceInstallDialog
        actionError={
          actionError && actionError.intent === 'install' && actionError.name === entry.name
            ? actionError.message
            : loaderData.loadError
        }
        entry={entry}
        open
        onOpenChange={(open) => {
          if (!open) navigate(sourcesPath)
        }}
        submitting={pendingName === entry.name && pendingIntent === 'install'}
        workspaceId={workspaceId}
      />
    )
  }

  return (
    <SourceDetailDialog
      entry={entry}
      loadError={loaderData.loadError}
      open
      onOpenChange={(open) => {
        if (!open) navigate(sourcesPath)
      }}
    />
  )
}

export function SourceDetailDialog({
  entry,
  loadError,
  open,
  onOpenChange,
}: {
  entry: CatalogEntry | null
  loadError: string | null
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="l">
          {entry ? (
            <SourceDetailDialogContent
              key={entry.name}
              entry={entry}
              loadError={loadError}
              onClose={() => onOpenChange(false)}
            />
          ) : null}
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function SourceDetailDialogContent({
  entry,
  loadError,
  onClose,
}: {
  entry: CatalogEntry
  loadError: string | null
  onClose: () => void
}) {
  const [confirmingRemove, setConfirmingRemove] = useState(false)
  const [drafts, setDrafts] = useState<Record<string, string>>({})
  const { actionError, pendingIntent, removeError } = useSourceDetailActionState(entry, loadError)
  const sourceDisplayName = formatSourceName(entry.name)

  const source = entry.source ?? null
  const inputSpecs = entry.inputSpecs
  const deleting = pendingIntent === 'delete'
  const saving = pendingIntent === 'edit'
  const editable = source ? source.origin === 'bundled' : false

  useEffect(() => {
    if (removeError) setConfirmingRemove(true)
  }, [removeError])

  const hasChanges = useMemo(() => {
    if (!source) return false
    if (inputSpecs) {
      const variables = new Map(source.variables.map((v) => [v.key, v.value]))
      for (const input of inputSpecs) {
        if (input.input.case === 'variable') {
          const draft = drafts[`var:${input.key}`]
          const current = variables.get(input.key) ?? input.input.value.defaultValue ?? ''
          if (draft !== undefined && draft !== current) return true
        } else if (input.input.case === 'secret') {
          const draft = drafts[`sec:${input.key}`]
          if (draft !== undefined && draft.trim().length > 0) return true
        }
      }
      return false
    }
    for (const v of source.variables) {
      const draft = drafts[`var:${v.key}`]
      if (draft !== undefined && draft !== v.value) return true
    }
    for (const s of source.secrets) {
      const draft = drafts[`sec:${s.key}`]
      if (draft !== undefined && draft.trim().length > 0) return true
    }
    return false
  }, [drafts, source, inputSpecs])

  const origin = source ? source.origin : entry.origin

  return (
    <>
      <Form method="post">
        <input type="hidden" name="_intent" value="edit" />
        <input type="hidden" name="name" value={entry.name} />

        <div className={styles.header}>
          <ProviderLogo name={entry.name} size="large" />
          <div className={styles.headerText}>
            <Dialog.Title className={styles.headerTitleRow}>
              <Typography.HeadingMedium as="span" className={styles.headerTitle}>
                {sourceDisplayName}
              </Typography.HeadingMedium>
              {origin ? (
                <span className={styles.headerPill}>{originBadgeLabel(origin)}</span>
              ) : null}
            </Dialog.Title>
            <Dialog.Description render={<div />}>
              <Typography.BodySmall variant="secondary">
                {source?.version || entry.version
                  ? `v${source?.version || entry.version}`
                  : 'Configured source'}
              </Typography.BodySmall>
            </Dialog.Description>
          </div>
        </div>

        {!source ? (
          <div className={styles.alertError}>
            <Icon name="CircleAlert" size="14" color="inherit" />
            <Typography.BodySmall>Installed source details are unavailable.</Typography.BodySmall>
          </div>
        ) : null}

        {actionError ? (
          <div className={styles.alertError}>
            <Icon name="CircleAlert" size="14" color="inherit" />
            <Typography.BodySmall>{actionError}</Typography.BodySmall>
          </div>
        ) : null}

        {!source ? null : inputSpecs ? (
          <SourceInputBindings
            disabled={!editable || saving || deleting}
            drafts={drafts}
            editable={editable}
            inputSpecs={inputSpecs}
            onSecretBlur={(key) => {
              const draftKey = `sec:${key}`
              if (drafts[draftKey] !== '') return
              setDrafts((previous) => {
                const next = { ...previous }
                delete next[draftKey]
                return next
              })
            }}
            onSecretFocus={(key) => {
              const draftKey = `sec:${key}`
              if (drafts[draftKey] !== undefined) return
              setDrafts((previous) => ({ ...previous, [draftKey]: '' }))
            }}
            onValueChange={(key, value, secret) =>
              setDrafts((previous) => ({
                ...previous,
                [`${secret ? 'sec' : 'var'}:${key}`]: value,
              }))
            }
            source={source}
          />
        ) : source.variables.length === 0 && source.secrets.length === 0 ? (
          <section className={styles.section}>
            <Typography.HeadingXSmall as="h3">Configuration</Typography.HeadingXSmall>
            <Typography.BodySmall variant="tertiary">No bindings recorded.</Typography.BodySmall>
          </section>
        ) : (
          <InstalledBindings
            disabled={!editable || saving || deleting}
            drafts={drafts}
            editable={editable}
            onValueChange={(draftKey, value) =>
              setDrafts((previous) => ({ ...previous, [draftKey]: value }))
            }
            source={source}
          />
        )}

        <Dialog.Actions>
          <ButtonContainer
            variant="bare"
            size="32"
            onClick={() => setConfirmingRemove(true)}
            disabled={!source || saving || deleting}
          >
            <ButtonText>Remove</ButtonText>
          </ButtonContainer>
          {editable && hasChanges ? (
            <ButtonContainer variant="primary" size="32" type="submit" disabled={saving}>
              {saving ? <SpinningButtonIcon name="Loader" /> : null}
              <ButtonText>{saving ? 'Saving…' : 'Save changes'}</ButtonText>
            </ButtonContainer>
          ) : (
            <ButtonContainer variant="primary" size="32" onClick={onClose}>
              <ButtonText>Close</ButtonText>
            </ButtonContainer>
          )}
        </Dialog.Actions>
      </Form>

      <Dialog.Root open={confirmingRemove} onOpenChange={(open) => setConfirmingRemove(open)}>
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup size="m">
            <RemoveConfirmation
              deleting={deleting}
              error={removeError}
              name={sourceDisplayName}
              onCancel={() => setConfirmingRemove(false)}
            />
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
    </>
  )
}

function RemoveConfirmation({
  deleting,
  error,
  name,
  onCancel,
}: {
  deleting: boolean
  error?: string | null
  name: string
  onCancel: () => void
}) {
  return (
    <Form method="post">
      <input type="hidden" name="_intent" value="delete" />
      <input type="hidden" name="name" value={name} />

      <div className={styles.removeConfirmText}>
        <Dialog.Title>Remove {name}?</Dialog.Title>
        <Dialog.Description>
          This deletes the source configuration and stored credentials from this workspace. You can
          reinstall later, but you'll need to re-supply any secrets.
        </Dialog.Description>
      </div>
      {error ? (
        <div className={styles.alertError}>
          <Icon name="CircleAlert" size="14" color="inherit" />
          <Typography.BodySmall>{error}</Typography.BodySmall>
        </div>
      ) : null}
      <Dialog.Actions className={styles.removeConfirmActions}>
        <ButtonContainer variant="secondary" size="32" onClick={onCancel} disabled={deleting}>
          <ButtonText>Cancel</ButtonText>
        </ButtonContainer>
        <ButtonContainer variant="destructive" size="32" type="submit" disabled={deleting}>
          {deleting ? <SpinningButtonIcon name="Loader" /> : null}
          <ButtonText>{deleting ? 'Removing…' : 'Remove'}</ButtonText>
        </ButtonContainer>
      </Dialog.Actions>
    </Form>
  )
}

function InstalledBindings({
  disabled,
  drafts,
  editable,
  onValueChange,
  source,
}: {
  disabled: boolean
  drafts: Record<string, string>
  editable: boolean
  onValueChange: (draftKey: string, value: string) => void
  source: CatalogSource
}) {
  return (
    <section className={styles.section}>
      <Typography.HeadingXSmall as="h3">Configuration</Typography.HeadingXSmall>
      {!editable ? (
        <Typography.BodySmall variant="tertiary">{IMPORTED_EDIT_NOTICE}</Typography.BodySmall>
      ) : null}
      <div className={styles.fieldGroup}>
        {source.variables.map((v) => {
          const draftKey = `var:${v.key}`
          return (
            <div key={draftKey} className={styles.fieldItem}>
              <Typography.Body className={styles.fieldLabel}>{v.key}</Typography.Body>
              <TextInput
                name={draftKey}
                value={drafts[draftKey] ?? v.value}
                onChange={(value) => onValueChange(draftKey, value)}
                placeholder={v.key}
                disabled={disabled}
              />
            </div>
          )
        })}
        {source.secrets.map((s) => {
          const draftKey = `sec:${s.key}`
          return (
            <div key={draftKey} className={styles.fieldItem}>
              <Typography.Body className={styles.fieldLabel}>{s.key}</Typography.Body>
              <input type="hidden" name={draftKey} value={drafts[draftKey] ?? ''} />
              <TextInput
                type="password"
                value={drafts[draftKey] ?? ''}
                onChange={(value) => onValueChange(draftKey, value)}
                placeholder={SECRET_PLACEHOLDER}
                disabled={disabled}
              />
            </div>
          )
        })}
      </div>
    </section>
  )
}

function useSourceDetailActionState(entry: CatalogEntry, loadError: string | null) {
  const actionData = useActionData<typeof sourceDetailAction>()
  const navigation = useNavigation()
  const actionError = actionData?.status === 'error' ? actionData : null
  const pendingIntent = formValue(navigation.formData, '_intent')
  const pendingName = formValue(navigation.formData, 'name')

  return {
    actionError:
      actionError && actionError.intent === 'edit' && actionError.name === entry.name
        ? actionError.message
        : loadError,
    pendingIntent:
      pendingName === entry.name && (pendingIntent === 'delete' || pendingIntent === 'edit')
        ? pendingIntent
        : null,
    removeError:
      actionError && actionError.intent === 'delete' && actionError.name === entry.name
        ? actionError.message
        : null,
  }
}

function formValue(formData: FormData | undefined, key: string): string | null {
  const value = formData?.get(key)
  return typeof value === 'string' ? value : null
}

function SourceInputBindings({
  disabled,
  drafts,
  editable,
  inputSpecs,
  onSecretBlur,
  onSecretFocus,
  onValueChange,
  source,
}: {
  disabled: boolean
  drafts: Record<string, string>
  editable: boolean
  inputSpecs: CatalogSourceInputSpec[]
  onSecretBlur: (key: string) => void
  onSecretFocus: (key: string) => void
  onValueChange: (key: string, value: string, secret: boolean) => void
  source: CatalogSource
}) {
  const variables = useMemo(() => new Map(source.variables.map((v) => [v.key, v.value])), [source])
  const configuredSecrets = useMemo(() => new Set(source.secrets.map((s) => s.key)), [source])

  if (inputSpecs.length === 0) {
    return (
      <section className={styles.section}>
        <Typography.HeadingXSmall as="h3">Configuration</Typography.HeadingXSmall>
        <Typography.BodySmall variant="tertiary">No bindings recorded.</Typography.BodySmall>
      </section>
    )
  }

  return (
    <section className={styles.section}>
      <Typography.HeadingXSmall as="h3">Configuration</Typography.HeadingXSmall>
      {!editable ? (
        <Typography.BodySmall variant="tertiary">{IMPORTED_EDIT_NOTICE}</Typography.BodySmall>
      ) : null}
      <div className={styles.fieldGroup}>
        {inputSpecs.map((input) => (
          <SourceInfoInputRow
            key={input.key}
            configuredSecret={configuredSecrets.has(input.key)}
            disabled={disabled}
            draft={drafts[`${input.input.case === 'secret' ? 'sec' : 'var'}:${input.key}`]}
            input={input}
            onSecretBlur={onSecretBlur}
            onSecretFocus={onSecretFocus}
            onValueChange={onValueChange}
            value={variables.get(input.key)}
          />
        ))}
      </div>
    </section>
  )
}

function SourceInfoInputRow({
  configuredSecret,
  disabled,
  draft,
  input,
  onSecretBlur,
  onSecretFocus,
  onValueChange,
  value,
}: {
  configuredSecret: boolean
  disabled: boolean
  draft: string | undefined
  input: CatalogSourceInputSpec
  onSecretBlur: (key: string) => void
  onSecretFocus: (key: string) => void
  onValueChange: (key: string, value: string, secret: boolean) => void
  value: string | undefined
}) {
  if (input.input.case === 'variable') {
    const resolved = value ?? input.input.value.defaultValue ?? ''
    return (
      <Field input={input}>
        <TextInput
          name={`var:${input.key}`}
          value={draft ?? resolved}
          onChange={(next) => onValueChange(input.key, next, false)}
          placeholder={resolved || input.key}
          disabled={disabled}
        />
      </Field>
    )
  }

  if (input.input.case !== 'secret') return null

  return (
    <Field input={input}>
      <input type="hidden" name={`sec:${input.key}`} value={draft ?? ''} />
      <TextInput
        type="password"
        value={draft ?? (configuredSecret ? SECRET_PLACEHOLDER : '')}
        onBlur={() => onSecretBlur(input.key)}
        onChange={(next) => onValueChange(input.key, next, true)}
        onFocus={() => onSecretFocus(input.key)}
        placeholder={input.key}
        disabled={disabled}
      />
    </Field>
  )
}

function Field({ input, children }: { input: CatalogSourceInputSpec; children: React.ReactNode }) {
  return (
    <div className={styles.fieldItem}>
      <Typography.Body className={styles.fieldLabel}>{input.key}</Typography.Body>
      {children}
    </div>
  )
}

function originBadgeLabel(origin: SourceOriginLabel): string {
  if (origin === 'bundled') return 'Core'
  if (origin === 'imported') return 'Imported'
  return '—'
}
