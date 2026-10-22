import { useEffect, useId, useRef, type ReactNode } from 'react'
import { Form } from 'react-router'
import {
  attemptParams,
  attemptPath,
  reviseAttempt,
  freezeAttempt,
  attemptProgress,
  type PublicationAttempt,
} from '@/lib/provider-publication-attempt/model'
import { Banner, Button } from '@/wax/components'
import { Fields } from '@/views/management/forms'
import type { FormContext, SetupView, StepperResult } from './types'
import { StepFrame } from './step-frame'
import * as styles from './providers.css'

type PublicationProps = {
  attempt: PublicationAttempt
  setup: SetupView
  basePath: string
  form: FormContext
  result?: StepperResult
  pending: boolean
  disabled?: boolean
  /** The step before this one; absent when there is nothing to go back to. */
  backPath?: string
  /** Where Start over goes after a failure; the attempt's start by default. */
  startOverPath?: string
  /** What the submission commits, when the step states it above the actions. */
  children?: ReactNode
  /**
   * Sends the submission once, without a click, as soon as this holds. For an
   * answer the person already gave; a failure stays on the page as usual.
   */
  submitNow?: boolean
}

/** The form action and hidden fields freeze the URL-owned choices into one submission. */
export function PublicationForm(props: PublicationProps) {
  const { actions, body } = usePublication(props)
  return <StepFrame actions={actions}>{body}</StepFrame>
}

/** The submission's form and the actions that send it, for a step that frames them itself. */
export function usePublication({
  attempt,
  setup,
  basePath,
  form,
  result,
  pending,
  disabled = false,
  backPath,
  startOverPath = basePath,
  children,
  submitNow = false,
}: PublicationProps) {
  const formId = useId()
  const formRef = useRef<HTMLFormElement>(null)
  const submitted = useRef(false)
  useEffect(() => {
    if (!submitNow || submitted.current || pending || disabled || result) return
    submitted.current = true
    formRef.current?.requestSubmit()
  }, [submitNow, pending, disabled, result])
  const updating = attempt.kind === 'update'
  const retrying = attempt.state === 'frozen'
  const submission = freezeAttempt(attempt, setup, form.requestId)
  const { canRevise } = attemptProgress(attempt, setup, result)
  return {
    formId,
    actions: !result?.failed && (
      <>
        {/* One way back: after a retry it revises the whole attempt, otherwise
            it is the step before this one. */}
        {retrying && canRevise ? (
          <Button.InternalLink
            size="small"
            to={attemptPath(basePath, reviseAttempt(attempt))}
            variant="secondary"
          >
            Change choices
          </Button.InternalLink>
        ) : backPath && !retrying ? (
          <Button.InternalLink size="small" to={backPath} variant="secondary">
            Back
          </Button.InternalLink>
        ) : null}
        <Button.TextButton
          type="submit"
          form={formId}
          disabled={pending || disabled}
          busy={pending}
        >
          {retrying ? 'Retry' : updating ? 'Apply changes' : 'Add provider'}
        </Button.TextButton>
      </>
    ),
    body: (
      <Form
        ref={formRef}
        id={formId}
        method="post"
        action={attemptPath(basePath, submission)}
        className={styles.stack}
        aria-busy={pending}
      >
        <Fields form={{ ...form, requestId: submission.requestId }} intent="publish" />
        {[...attemptParams(submission)]
          .filter(([name]) => name !== 'requestId')
          .map(([name, value], index) => (
            <input key={`${name}:${index}`} type="hidden" name={name} value={value} />
          ))}
        {children}
        {result?.failed ? (
          <Banner
            variant="error"
            title={updating ? 'Not updated' : 'Not added'}
            action={
              <Button.InternalLink to={startOverPath} size="small">
                Start over
              </Button.InternalLink>
            }
          >
            {result.failed}
            {updating ? ' The provider is unchanged.' : ''}
          </Banner>
        ) : result?.error ? (
          <Banner variant="error">{result.error}</Banner>
        ) : retrying ? (
          <Banner variant="warning" title="Coral did not respond">
            Select Retry to check the result of this request. Coral applies the request only once.
          </Banner>
        ) : null}
      </Form>
    ),
  }
}
