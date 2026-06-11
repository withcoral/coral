import { useCallback, useEffect, useMemo, useState } from 'react'

import type { Source, SourceInfo, SourceInputSpec } from '@/generated/coral/v1/sources_pb'

import { Container as ButtonContainer } from '@/wax/components/button/container'
import { Icon as ButtonIcon } from '@/wax/components/button/icon'
import { Text as ButtonText } from '@/wax/components/button/text'
import * as Dialog from '@/wax/components/dialog'
import { Icon } from '@/wax/components/icon'
import { TextInput } from '@/wax/components/inputs/text'
import { addToast } from '@/wax/components/toast'
import { Typography } from '@/wax/components/typography'

import { providerIcon } from '@/lib/provider-icons'
import {
  createBundledSource,
  deleteSource,
  getInstalledSource,
  getSourceInfo,
  originLabel,
  type InstallInput,
  type SourceOriginLabel,
} from '@/lib/sources'

import * as styles from './source-detail.css'

const SECRET_PLACEHOLDER = '••••••••'

const IMPORTED_EDIT_NOTICE =
  "Imported sources can't be edited here yet — re-import the source spec to change its credentials."

export function SourceDetailDialog({
  name,
  open,
  onOpenChange,
  onRemoved,
}: {
  name: string | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onRemoved: (name: string) => void
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Backdrop />
        <Dialog.Popup size="l">
          {name ? (
            <SourceDetailDialogContent
              key={name}
              name={name}
              onClose={() => onOpenChange(false)}
              onRemoved={onRemoved}
            />
          ) : null}
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  )
}

function SourceDetailDialogContent({
  name,
  onClose,
  onRemoved,
}: {
  name: string
  onClose: () => void
  onRemoved: (name: string) => void
}) {
  const [source, setSource] = useState<Source | null>(null)
  const [sourceInfo, setSourceInfo] = useState<SourceInfo | null>(null)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [confirmingRemove, setConfirmingRemove] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [drafts, setDrafts] = useState<Record<string, string>>({})
  const [saving, setSaving] = useState(false)

  const refresh = useCallback(async () => {
    // getSourceInfo no longer depends on the installed origin, so fetch both in
    // parallel. Source info is best-effort (imported sources may not have it).
    const [installedResult, infoResult] = await Promise.allSettled([
      getInstalledSource(name),
      getSourceInfo(name),
    ])

    if (installedResult.status === 'rejected') {
      const reason = installedResult.reason
      setSource(null)
      setSourceInfo(null)
      setLoadError(reason instanceof Error ? reason.message : String(reason))
      return
    }

    setSource(installedResult.value)
    setSourceInfo(infoResult.status === 'fulfilled' ? infoResult.value.info : null)
    setDrafts({})
    setLoadError(null)
  }, [name])

  useEffect(() => {
    void refresh()
  }, [refresh])

  const onDelete = useCallback(async () => {
    setDeleting(true)
    try {
      await deleteSource(name)
      addToast('success', { title: `Removed ${name}` })
      setConfirmingRemove(false)
      onRemoved(name)
    } catch (e) {
      addToast('error', { title: e instanceof Error ? e.message : String(e) })
      setDeleting(false)
    }
  }, [name, onRemoved])

  const editable = source ? originLabel(source.origin) === 'bundled' : false

  const hasChanges = useMemo(() => {
    if (!source) return false
    if (sourceInfo) {
      const variables = new Map(source.variables.map((v) => [v.key, v.value]))
      for (const input of sourceInfo.inputs) {
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
  }, [drafts, source, sourceInfo])

  async function save() {
    if (!source) return
    setSaving(true)
    try {
      const bindings: InstallInput[] = []
      if (sourceInfo) {
        const variables = new Map(source.variables.map((v) => [v.key, v.value]))
        for (const input of sourceInfo.inputs) {
          if (input.input.case === 'variable') {
            const value = (
              drafts[`var:${input.key}`] ??
              variables.get(input.key) ??
              input.input.value.defaultValue ??
              ''
            ).trim()
            if (value.length > 0) bindings.push({ key: input.key, value, secret: false })
            continue
          }
          if (input.input.case !== 'secret') continue
          const draft = drafts[`sec:${input.key}`]
          if (draft === undefined) continue
          const trimmed = draft.trim()
          if (trimmed.length > 0) {
            bindings.push({ key: input.key, value: trimmed, secret: true })
          }
        }
      } else {
        bindings.push(
          ...source.variables.map((v) => ({
            key: v.key,
            value: drafts[`var:${v.key}`] ?? v.value,
            secret: false,
          })),
        )
        for (const s of source.secrets) {
          const draft = drafts[`sec:${s.key}`]
          if (draft === undefined) continue
          const trimmed = draft.trim()
          if (trimmed.length > 0) {
            bindings.push({ key: s.key, value: trimmed, secret: true })
          }
        }
      }
      await createBundledSource(name, bindings)
      onClose()
      addToast('success', { title: `Updated ${name}` })
    } catch (e) {
      addToast('error', { title: e instanceof Error ? e.message : String(e) })
    } finally {
      setSaving(false)
    }
  }

  const icon = providerIcon(name)
  const origin = source ? originLabel(source.origin) : null

  return (
    <>
      <div className={styles.header}>
        <div className={styles.headerLogo}>
          {icon ? (
            <img src={icon} alt="" className={styles.headerLogoImg} />
          ) : (
            <Icon name="Plug" size="22" color="secondary" />
          )}
        </div>
        <div className={styles.headerText}>
          <Dialog.Title className={styles.headerTitleRow}>
            <Typography.HeadingMedium as="span" className={styles.headerTitle}>
              {name}
            </Typography.HeadingMedium>
            {origin ? <span className={styles.headerPill}>{originBadgeLabel(origin)}</span> : null}
          </Dialog.Title>
          <Dialog.Description render={<div />}>
            <Typography.BodySmall variant="secondary">
              {source?.version ? `v${source.version}` : 'Configured source'}
            </Typography.BodySmall>
          </Dialog.Description>
        </div>
      </div>

      {loadError ? (
        <div className={styles.alertError}>
          <Icon name="CircleAlert" size="14" color="inherit" />
          <Typography.BodySmall>{loadError}</Typography.BodySmall>
        </div>
      ) : null}

      {!source && !loadError ? (
        <Typography.BodySmall variant="tertiary">Loading…</Typography.BodySmall>
      ) : !source ? null : sourceInfo ? (
        <SourceInfoBindings
          disabled={!editable || saving || deleting}
          drafts={drafts}
          editable={editable}
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
            setDrafts((previous) => ({ ...previous, [`${secret ? 'sec' : 'var'}:${key}`]: value }))
          }
          source={source}
          sourceInfo={sourceInfo}
        />
      ) : source.variables.length === 0 && source.secrets.length === 0 ? (
        <section className={styles.section}>
          <Typography.HeadingXSmall as="h3">Configuration</Typography.HeadingXSmall>
          <Typography.BodySmall variant="tertiary">No bindings recorded.</Typography.BodySmall>
        </section>
      ) : (
        <FallbackBindings
          disabled={!editable || saving || deleting}
          drafts={drafts}
          editable={editable}
          onValueChange={(draftKey, value) =>
            setDrafts((previous) => ({ ...previous, [draftKey]: value }))
          }
          source={source}
        />
      )}

      {confirmingRemove ? (
        <RemoveConfirmation
          deleting={deleting}
          name={name}
          onCancel={() => setConfirmingRemove(false)}
          onDelete={onDelete}
        />
      ) : (
        <Dialog.Actions>
          <ButtonContainer
            variant="bare"
            size="32"
            onClick={() => setConfirmingRemove(true)}
            disabled={saving || deleting}
          >
            <ButtonText>Remove</ButtonText>
          </ButtonContainer>
          {editable && hasChanges ? (
            <ButtonContainer
              variant="primary"
              size="32"
              onClick={() => void save()}
              disabled={saving}
            >
              {saving ? <ButtonIcon name="Loader" /> : null}
              <ButtonText>{saving ? 'Saving…' : 'Save changes'}</ButtonText>
            </ButtonContainer>
          ) : (
            <ButtonContainer variant="primary" size="32" onClick={onClose}>
              <ButtonText>Close</ButtonText>
            </ButtonContainer>
          )}
        </Dialog.Actions>
      )}
    </>
  )
}

function RemoveConfirmation({
  deleting,
  name,
  onCancel,
  onDelete,
}: {
  deleting: boolean
  name: string
  onCancel: () => void
  onDelete: () => void
}) {
  return (
    <section className={styles.removeConfirm} aria-live="polite">
      <div className={styles.removeConfirmText}>
        <Typography.BodySmallStrong variant="primary">Remove {name}?</Typography.BodySmallStrong>
        <Typography.BodySmall variant="secondary">
          This deletes the source configuration and stored credentials from this workspace. You can
          reinstall later, but you'll need to re-supply any secrets.
        </Typography.BodySmall>
      </div>
      <div className={styles.removeConfirmActions}>
        <ButtonContainer variant="secondary" size="32" onClick={onCancel} disabled={deleting}>
          <ButtonText>Cancel</ButtonText>
        </ButtonContainer>
        <ButtonContainer
          variant="destructive"
          size="32"
          onClick={() => void onDelete()}
          disabled={deleting}
        >
          {deleting ? <ButtonIcon name="Loader" /> : null}
          <ButtonText>{deleting ? 'Removing…' : 'Remove'}</ButtonText>
        </ButtonContainer>
      </div>
    </section>
  )
}

function FallbackBindings({
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
  source: Source
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

function SourceInfoBindings({
  disabled,
  drafts,
  editable,
  onSecretBlur,
  onSecretFocus,
  onValueChange,
  source,
  sourceInfo,
}: {
  disabled: boolean
  drafts: Record<string, string>
  editable: boolean
  onSecretBlur: (key: string) => void
  onSecretFocus: (key: string) => void
  onValueChange: (key: string, value: string, secret: boolean) => void
  source: Source
  sourceInfo: SourceInfo
}) {
  const variables = useMemo(() => new Map(source.variables.map((v) => [v.key, v.value])), [source])
  const configuredSecrets = useMemo(() => new Set(source.secrets.map((s) => s.key)), [source])

  if (sourceInfo.inputs.length === 0) {
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
        {sourceInfo.inputs.map((input) => (
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
  input: SourceInputSpec
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

function Field({ input, children }: { input: SourceInputSpec; children: React.ReactNode }) {
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
