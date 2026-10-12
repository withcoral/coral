import classNames from 'classnames'
import { useEffect, useId, useImperativeHandle, useRef, useState } from 'react'

import { TextButton } from '@/wax/components/button'
import * as inputStyles from '@/wax/components/inputs/base-input.css'
import { visuallyHidden } from '@/wax/components/visually-hidden.css'
import * as styles from './file-input.css'

export interface FileInputProps {
  ariaLabel?: string
  /** Comma-separated accept list, such as ".json,.yaml". */
  accept?: string
  className?: string
  disabled?: boolean
  /** Enables the drop area. The caller supplies format and limit instructions. */
  dropLabel?: string
  id?: string
  invalid?: boolean
  name?: string
  /** Reports picker selections, drops, and native form resets. */
  onChange?: (file: File | null) => void
  ref?: React.Ref<HTMLInputElement>
  required?: boolean
}

/** An uncontrolled single-file picker. Drops become the native form's file value. */
export function FileInput({
  ariaLabel,
  accept,
  className,
  disabled,
  dropLabel,
  id,
  invalid,
  name,
  onChange,
  ref,
  required,
}: FileInputProps) {
  const inputRef = useRef<HTMLInputElement>(null)
  const dragDepth = useRef(0)
  const [dropping, setDropping] = useState(false)
  const [filename, setFilename] = useState('')
  const descriptionId = useId()
  useImperativeHandle(ref, () => inputRef.current!)

  useEffect(() => {
    const form = inputRef.current?.form
    if (!form) return
    const reset = (event: Event) => {
      // Reset fires before the browser clears the input and can be canceled.
      queueMicrotask(() => {
        const input = inputRef.current
        if (event.defaultPrevented || !input || input.form !== form) return
        const file = input.files?.[0] ?? null
        setFilename(file?.name ?? '')
        dragDepth.current = 0
        setDropping(false)
        onChange?.(file)
      })
    }
    form.addEventListener('reset', reset)
    return () => form.removeEventListener('reset', reset)
  }, [onChange])

  const input = (
    <input
      aria-label={ariaLabel}
      aria-describedby={dropLabel ? descriptionId : undefined}
      aria-invalid={invalid || undefined}
      data-invalid={invalid || undefined}
      accept={accept}
      className={dropLabel ? visuallyHidden : classNames(inputStyles.input, className)}
      disabled={disabled}
      id={id}
      name={name}
      onChange={(event) => {
        const file = event.currentTarget.files?.[0] ?? null
        setFilename(file?.name ?? '')
        onChange?.(file)
      }}
      ref={inputRef}
      required={required}
      tabIndex={dropLabel ? -1 : undefined}
      type="file"
    />
  )
  if (!dropLabel) return input

  return (
    <div
      className={classNames(styles.dropZone, className)}
      data-dropping={(!disabled && dropping) || undefined}
      data-disabled={disabled || undefined}
      data-invalid={invalid || undefined}
      onDragEnter={(event) => {
        if (inputRef.current?.matches(':disabled') || !event.dataTransfer.types.includes('Files'))
          return
        event.preventDefault()
        dragDepth.current += 1
        setDropping(true)
      }}
      onDragLeave={() => {
        dragDepth.current = Math.max(0, dragDepth.current - 1)
        if (!dragDepth.current) setDropping(false)
      }}
      onDragOver={(event) => {
        if (!event.dataTransfer.types.includes('Files')) return
        event.preventDefault()
        event.dataTransfer.dropEffect = inputRef.current?.matches(':disabled') ? 'none' : 'copy'
      }}
      onDrop={(event) => {
        // Rejected drops must not navigate the browser or alter the selection.
        event.preventDefault()
        dragDepth.current = 0
        setDropping(false)
        const file = event.dataTransfer.files[0]
        if (!file) return
        const picker = inputRef.current
        if (!picker || picker.matches(':disabled')) return
        const transfer = new DataTransfer()
        transfer.items.add(file)
        picker.files = transfer.files
        // Use the same native change seam as the picker, including bubbling form listeners.
        picker.dispatchEvent(new Event('input', { bubbles: true }))
        picker.dispatchEvent(new Event('change', { bubbles: true }))
      }}
    >
      <p className={styles.instructions} id={descriptionId}>
        {dropLabel}
      </p>
      <p className={styles.filename} role="status">
        {filename}
      </p>
      {input}
      <TextButton
        type="button"
        variant="secondary"
        size="22"
        disabled={disabled}
        onClick={() => inputRef.current?.click()}
      >
        Choose a file
      </TextButton>
    </div>
  )
}
