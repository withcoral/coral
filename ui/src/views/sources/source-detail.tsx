import { useCallback, useEffect, useMemo, useState } from 'react'

import type { Source } from '@/generated/coral/v1/sources_pb'

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
  originLabel,
  type InstallInput,
  type SourceOriginLabel,
} from '@/lib/sources'

import * as styles from './source-detail.css'

const SECRET_PLACEHOLDER = '••••••••'

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
  const [loadError, setLoadError] = useState<string | null>(null)
  const [confirmingRemove, setConfirmingRemove] = useState(false)
  const [deleting, setDeleting] = useState(false)
  const [drafts, setDrafts] = useState<Record<string, string>>({})
  const [saving, setSaving] = useState(false)

  const refresh = useCallback(async () => {
    try {
      const installed = await getInstalledSource(name)
      setSource(installed)
      setDrafts({})
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : String(e))
    }
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
    for (const v of source.variables) {
      const draft = drafts[`var:${v.key}`]
      if (draft !== undefined && draft !== v.value) return true
    }
    for (const s of source.secrets) {
      const draft = drafts[`sec:${s.key}`]
      if (draft !== undefined && draft.length > 0) return true
    }
    return false
  }, [drafts, source])

  async function save() {
    if (!source) return
    setSaving(true)
    try {
      const bindings: InstallInput[] = source.variables.map((v) => ({
        key: v.key,
        value: drafts[`var:${v.key}`] ?? v.value,
        secret: false,
      }))
      for (const s of source.secrets) {
        const draft = drafts[`sec:${s.key}`]
        if (draft !== undefined && draft.length > 0) {
          bindings.push({ key: s.key, value: draft, secret: true })
        }
      }
      await createBundledSource(name, bindings)
      addToast('success', { title: `Updated ${name}` })
      await refresh()
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
              {source?.version ? `v${source.version}` : 'Connected source'}
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
      ) : !source ? null : source.variables.length === 0 && source.secrets.length === 0 ? (
        <section className={styles.section}>
          <Typography.HeadingXSmall as="h3">Configuration</Typography.HeadingXSmall>
          <Typography.BodySmall variant="tertiary">No bindings recorded.</Typography.BodySmall>
        </section>
      ) : (
        <section className={styles.section}>
          <Typography.HeadingXSmall as="h3">Configuration</Typography.HeadingXSmall>
          {!editable ? (
            <Typography.BodySmall variant="tertiary">
              Imported sources can't be edited here yet — re-import the source spec to change its
              credentials.
            </Typography.BodySmall>
          ) : null}
          <div className={styles.bindingList}>
            {source.variables.map((v) => {
              const draftKey = `var:${v.key}`
              return (
                <div key={draftKey} className={styles.bindingRow}>
                  <span className={styles.keyLabel}>{v.key}</span>
                  <TextInput
                    value={drafts[draftKey] ?? v.value}
                    onChange={(value) => setDrafts((p) => ({ ...p, [draftKey]: value }))}
                    placeholder={v.key}
                    disabled={!editable || saving}
                  />
                </div>
              )
            })}
            {source.secrets.map((s) => {
              const draftKey = `sec:${s.key}`
              return (
                <div key={draftKey} className={styles.bindingRow}>
                  <span className={styles.keyLabel}>{s.key}</span>
                  <TextInput
                    type="password"
                    value={drafts[draftKey] ?? ''}
                    onChange={(value) => setDrafts((p) => ({ ...p, [draftKey]: value }))}
                    placeholder={SECRET_PLACEHOLDER}
                    disabled={!editable || saving}
                  />
                </div>
              )
            })}
          </div>
        </section>
      )}

      <Dialog.Actions>
        <ButtonContainer variant="bare" size="32" onClick={() => setConfirmingRemove(true)}>
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

      <Dialog.Root open={confirmingRemove} onOpenChange={setConfirmingRemove}>
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup size="m">
            <Dialog.Title>Remove {name}?</Dialog.Title>
            <Dialog.Description>
              This deletes the source configuration and stored credentials from this workspace. You
              can reinstall later, but you'll need to re-supply any secrets.
            </Dialog.Description>
            <Dialog.Actions>
              <ButtonContainer
                variant="secondary"
                size="32"
                onClick={() => setConfirmingRemove(false)}
                disabled={deleting}
              >
                <ButtonText>Cancel</ButtonText>
              </ButtonContainer>
              <ButtonContainer
                variant="primary"
                size="32"
                onClick={() => void onDelete()}
                disabled={deleting}
              >
                {deleting ? <ButtonIcon name="Loader" /> : null}
                <ButtonText>{deleting ? 'Removing…' : 'Remove'}</ButtonText>
              </ButtonContainer>
            </Dialog.Actions>
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
    </>
  )
}

function originBadgeLabel(origin: SourceOriginLabel): string {
  if (origin === 'bundled') return 'Core'
  if (origin === 'imported') return 'Imported'
  return '—'
}
