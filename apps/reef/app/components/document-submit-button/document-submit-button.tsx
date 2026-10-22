import { useLayoutEffect, useRef, useState } from 'react'

import { Button } from '@/wax/components'
import type { TextButtonProps } from '@/wax/components/button/text-button'

/** The submission each form is waiting on, so a second one cannot replace it. */
const pendingForms = new WeakMap<HTMLFormElement, SubmitEvent>()

export interface DocumentSubmitButtonProps extends Omit<TextButtonProps, 'busy' | 'ref'> {
  ref?: React.Ref<HTMLButtonElement>
}

/**
 * A submit button for a form that bypasses React Router. It reports the exact
 * submitter while the current document waits for its replacement.
 */
export function DocumentSubmitButton({ children, ref, ...props }: DocumentSubmitButtonProps) {
  const buttonRef = useRef<HTMLButtonElement | null>(null)
  const [busy, setBusy] = useState(false)

  useLayoutEffect(() => {
    let submitted: { form: HTMLFormElement; event: SubmitEvent } | undefined
    const release = () => {
      if (submitted && pendingForms.get(submitted.form) === submitted.event) {
        pendingForms.delete(submitted.form)
      }
      submitted = undefined
    }
    // The window sees a submit last, after React's handlers, so a cancellation
    // by any of them is already visible here.
    const onSubmit = (event: SubmitEvent) => {
      const form = event.target as HTMLFormElement
      const pending = pendingForms.get(form)
      if (pending && pending !== event) {
        event.preventDefault()
        return
      }
      const button = buttonRef.current
      if (event.defaultPrevented || !button || event.submitter !== button) return
      const target = button.formTarget || form.target
      if (target && target !== '_self') return
      pendingForms.set(form, event)
      submitted = { form, event }
      setBusy(true)
    }
    const onPageShow = (event: PageTransitionEvent) => {
      if (!event.persisted) return
      release()
      setBusy(false)
    }

    window.addEventListener('submit', onSubmit)
    window.addEventListener('pageshow', onPageShow)
    return () => {
      window.removeEventListener('submit', onSubmit)
      window.removeEventListener('pageshow', onPageShow)
      release()
    }
  }, [])

  const setRef = (button: HTMLButtonElement | null) => {
    buttonRef.current = button
    if (typeof ref === 'function') ref(button)
    else if (ref) ref.current = button
  }

  return (
    <Button.TextButton {...props} ref={setRef} busy={busy} type={props.type ?? 'submit'}>
      {children}
    </Button.TextButton>
  )
}
