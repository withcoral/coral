import { flushSync } from 'react-dom'
import { useLayoutEffect, useRef, useState } from 'react'

import { Button } from '@/wax/components'
import type { TextButtonProps } from '@/wax/components/button/text-button'

type PendingSubmission = { event: SubmitEvent; submitter: HTMLButtonElement }

const pendingForms = new WeakMap<HTMLFormElement, PendingSubmission>()

export interface DocumentSubmitButtonProps extends Omit<TextButtonProps, 'busyStatus' | 'ref'> {
  /** Announced while the document request is in progress. */
  busyStatus: string
  ref?: React.Ref<HTMLButtonElement>
}

/**
 * A submit button for a form that bypasses React Router. It reports the exact
 * submitter while the current document waits for its replacement.
 */
export function DocumentSubmitButton({
  busyStatus,
  children,
  ref,
  ...props
}: DocumentSubmitButtonProps) {
  const buttonRef = useRef<HTMLButtonElement | null>(null)
  const [busy, setBusy] = useState(false)

  useLayoutEffect(() => {
    const button = buttonRef.current
    const formId = button?.getAttribute('form')
    const form =
      button?.form ?? (formId ? (document.getElementById(formId) as HTMLFormElement | null) : null)
    if (!button || !form) return

    const clear = () => {
      const pending = pendingForms.get(form)
      if (pending?.submitter === button) pendingForms.delete(form)
      setBusy(false)
    }
    const onPageShow = (event: PageTransitionEvent) => {
      if (event.persisted) clear()
    }
    const onSubmit = (event: SubmitEvent) => {
      const pending = pendingForms.get(form)
      if (pending && pending.event !== event) {
        event.preventDefault()
        return
      }
      if (event.submitter !== button || (form.target && form.target !== '_self')) return

      pendingForms.set(form, { event, submitter: button })
      queueMicrotask(() => {
        if (event.defaultPrevented) {
          if (pendingForms.get(form)?.event === event) pendingForms.delete(form)
          return
        }
        flushSync(() => setBusy(true))
      })
    }

    form.addEventListener('submit', onSubmit)
    window.addEventListener('pageshow', onPageShow)
    return () => {
      form.removeEventListener('submit', onSubmit)
      window.removeEventListener('pageshow', onPageShow)
      const pending = pendingForms.get(form)
      if (pending?.submitter === button) pendingForms.delete(form)
    }
  }, [])

  const setRef = (button: HTMLButtonElement | null) => {
    buttonRef.current = button
    if (typeof ref === 'function') ref(button)
    else if (ref) ref.current = button
  }

  return (
    <Button.TextButton
      {...props}
      ref={setRef}
      busyStatus={busy ? busyStatus : undefined}
      type={props.type ?? 'submit'}
    >
      {children}
    </Button.TextButton>
  )
}
