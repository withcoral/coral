import { useEffect, useRef, useState, type ReactNode } from 'react'
import { useFetcher } from 'react-router'
import { Button, Inputs, Typography } from '@/wax/components'
import { ErrorMessage } from '@/wax/components/error-message/error-message'
import { Fields } from './forms'
import type { ActionResult, FormContext } from './types'
import * as styles from './management.css'

/**
 * A page's name as its heading. A pencil beside it swaps the text for an
 * input: Enter or Save saves, Escape or Cancel discards, and leaving the
 * field does neither. A failed save keeps the draft beside its error.
 */
export function NameHeading({
  name,
  noun,
  canRename,
  form,
  intent,
  etag,
  action,
  fields,
  children,
}: {
  name: string
  /** What the name belongs to, in lowercase: "provider", "workspace". */
  noun: string
  canRename: boolean
  form: FormContext
  intent: string
  etag: string
  /** The route that saves the name. Defaults to the current route. */
  action?: string
  /** More hidden fields the save needs. */
  fields?: ReactNode
  /** Status beside the name, such as pills. */
  children?: ReactNode
}) {
  const fetcher = useFetcher<ActionResult>()
  const [open, setOpen] = useState(false)
  // The result the last edit started from: anything newer belongs to this edit.
  const [earlier, setEarlier] = useState<ActionResult | undefined>(undefined)
  const [draft, setDraft] = useState(name)
  const pencil = useRef<HTMLButtonElement>(null)
  const formRef = useRef<HTMLFormElement>(null)
  const saving = fetcher.state !== 'idle'
  const result = !saving && fetcher.data !== earlier ? fetcher.data : undefined
  // A saved name closes the editor; a refused one keeps the draft open.
  const editing = open && !(result && !result.error)
  // Closing the editor, by saving or cancelling, returns focus to the pencil.
  const wasEditing = useRef(false)
  useEffect(() => {
    if (wasEditing.current && !editing) pencil.current?.focus()
    wasEditing.current = editing
  }, [editing])
  const cancel = () => setOpen(false)
  const label = `${noun.charAt(0).toUpperCase()}${noun.slice(1)} name`
  if (!editing)
    return (
      <div className={styles.nameHeading}>
        <Typography.HeadingLarge as="h2">{name}</Typography.HeadingLarge>
        {canRename && (
          <Button.IconButton
            ref={pencil}
            name="Pencil"
            variant="bare"
            size="32"
            tooltipText={`Edit ${noun} name`}
            onClick={() => {
              setDraft(name)
              setEarlier(fetcher.data)
              setOpen(true)
            }}
          />
        )}
        {children}
      </div>
    )
  const error = result?.error
  return (
    <fetcher.Form
      method="post"
      action={action}
      ref={formRef}
      className={styles.nameForm}
      aria-busy={saving}
    >
      <Fields form={form} intent={intent} etag={etag} />
      {fields}
      <div className={styles.nameRow}>
        <div className={styles.nameInput}>
          <Inputs.TextInput
            ariaLabel={label}
            name="displayName"
            value={draft}
            onChange={setDraft}
            autoFocus
            required
            maxLength={256}
            invalid={Boolean(error)}
            onKeyDown={(event) => {
              if (event.key === 'Escape') {
                event.preventDefault()
                cancel()
              } else if (event.key === 'Enter') {
                event.preventDefault()
                if (!saving) formRef.current?.requestSubmit()
              }
            }}
          />
        </div>
        <Button.TextButton type="submit" size="32" disabled={saving}>
          {saving ? 'Saving…' : 'Save'}
        </Button.TextButton>
        <Button.TextButton
          type="button"
          variant="secondary"
          size="32"
          disabled={saving}
          onClick={cancel}
        >
          Cancel
        </Button.TextButton>
      </div>
      {error && <ErrorMessage message={error} />}
    </fetcher.Form>
  )
}
