import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useId,
  useState,
  type ReactNode,
} from 'react'
import { useBeforeUnload, useBlocker, useFetcher } from 'react-router'
import { Banner, Button, Inputs, Typography } from '@/wax/components'
import * as Dialog from '@/wax/components/dialog'
import { Disclosure } from '@/wax/components/disclosure'
import * as Card from '@/wax/components/card'
import { CodeBlock } from '@/wax/components/code-block'
import { Deadline } from '@/wax/components/deadline'
import type { RequestDetail } from '../activity/types'
import { useActivityConnected } from '../activity/live'
import { settledApproval } from './approval-state'
import { useShellCsrf } from '@/lib/use-shell-csrf'
import type { action } from '@/routes/activity-resolve'
import * as styles from './actions.css'

type Approval = Extract<RequestDetail, { kind: 'approval' }>

type Draft = { name: string; comment: string }
const DraftContext = createContext<{
  draft: Draft
  /** An unsent comment would be lost, so leaving asks first. */
  dirty: boolean
  update: (value: Draft) => void
  settle: (name: string) => void
} | null>(null)

/** Keeps only one unsent draft across concealed/reconnecting exact reads, never server evidence. */
export function ActionReviewSession({
  children,
  request,
}: {
  children: ReactNode
  request?: RequestDetail | null
}) {
  const [draft, update] = useState<Draft>({ name: '', comment: '' })
  const settle = useCallback(
    (name: string) =>
      update((current) =>
        current.name === name && current.comment ? { name: '', comment: '' } : current,
      ),
    [],
  )
  if (
    draft.comment &&
    request?.kind === 'approval' &&
    request.name === draft.name &&
    !request.actionable
  ) {
    update({ name: '', comment: '' })
  }
  const dirty = Boolean(draft.comment)
  const blocker = useBlocker(({ currentLocation, nextLocation }) => {
    if (!dirty) return false
    const current = new URLSearchParams(currentLocation.search)
    const next = new URLSearchParams(nextLocation.search)
    const currentRecord = current.get('record') ?? currentLocation.pathname.slice(1)
    return (
      currentLocation.pathname !== nextLocation.pathname ||
      (current.get('record') !== next.get('record') &&
        !(
          request?.kind === 'approval' &&
          currentRecord === request.run &&
          next.get('record') === request.invocation
        )) ||
      (current.get('request') ?? request?.name ?? null) !== next.get('request')
    )
  })
  useBeforeUnload((event) => {
    if (dirty) {
      event.preventDefault()
      event.returnValue = ''
    }
  })
  return (
    <DraftContext value={{ draft, dirty, update, settle }}>
      {children}
      <Dialog.Root
        open={blocker.state === 'blocked'}
        onOpenChange={(open) => {
          if (!open && blocker.state === 'blocked') blocker.reset()
        }}
      >
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup>
            <Dialog.Title>Discard your comment?</Dialog.Title>
            <Dialog.Description>
              Your comment has not been submitted. Stay here to keep editing it.
            </Dialog.Description>
            <Dialog.Actions>
              <Button.TextButton
                variant="secondary"
                onClick={() => blocker.state === 'blocked' && blocker.reset()}
              >
                Keep editing
              </Button.TextButton>
              <Button.TextButton
                variant="destructive"
                onClick={() => {
                  if (blocker.state === 'blocked') {
                    update({ name: '', comment: '' })
                    blocker.proceed()
                  }
                }}
              >
                Discard comment
              </Button.TextButton>
            </Dialog.Actions>
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
    </DraftContext>
  )
}

/** Whether leaving the review would lose an unsent comment, so the session asks first. */
export function useUnsentComment() {
  return useContext(DraftContext)?.dirty ?? false
}

/** One exact call, one draft, one durable decision in Activity. */
export function ApprovalReview({ detail }: { detail: Approval }) {
  const csrf = useShellCsrf()
  const connected = useActivityConnected()
  const fetcher = useFetcher<typeof action>({ key: detail.name })
  const session = useContext(DraftContext)
  if (!session) throw new Error('Approval review needs its action review session')
  const comment = session.draft.name === detail.name ? session.draft.comment : ''
  const setComment = (value: string) => session.update({ name: detail.name, comment: value })
  const id = useId()
  const available = detail.actionable === true
  const pending = fetcher.state !== 'idle'
  // `in` narrowing leaves `state` unknown on the error and cancel results.
  const winning =
    fetcher.data && 'state' in fetcher.data && typeof fetcher.data.state === 'string'
      ? fetcher.data.state
      : undefined
  const settled = Boolean(winning && winning !== 'pending')
  const settle = session.settle
  useEffect(() => {
    // A terminal response settles this draft even before the next authorized loader snapshot.
    if (settled) settle(detail.name)
  }, [settled, settle, detail.name])
  const commentInvalid = new TextEncoder().encode(comment).length > 1024 || comment.includes('\0')
  const state = winning ?? detail.state
  const stateLabel = settledApproval(state)?.label ?? 'Approval unavailable'
  return (
    <section className={styles.review} aria-label={`Review ${detail.callPath}`}>
      <Card.Header sticky>
        <div className={styles.summary}>
          <Typography.HeadingSmall as="h3" className={styles.title}>
            {detail.callPath}
          </Typography.HeadingSmall>
          <Typography.BodySmall>
            {detail.providerLabel || 'Provider unavailable'}
          </Typography.BodySmall>
          {state !== 'pending' && (
            <Typography.BodyStrong role="status">{stateLabel}</Typography.BodyStrong>
          )}
          {state === 'pending' && !available && (
            <Typography.BodyStrong role="status">Approval unavailable</Typography.BodyStrong>
          )}
          {available && !settled && <Deadline at={detail.deadline} />}
        </div>
      </Card.Header>
      <Typography.HeadingSmall as="h4">Arguments to review</Typography.HeadingSmall>
      <CodeBlock value={detail.arguments} format="json" />
      {detail.approver && (
        <Typography.BodySmall>
          Reviewed by: {detail.approverLabel || 'Previous reviewer'}
        </Typography.BodySmall>
      )}
      {detail.comment !== null && <CodeBlock value={detail.comment} />}
      <fetcher.Form method="post" action="/activity/resolve" className={styles.stack}>
        <input type="hidden" name="csrf" value={csrf} />
        <input type="hidden" name="name" value={detail.name} />
        {available && !settled ? (
          <>
            <Disclosure title="Add a comment" open={Boolean(comment)}>
              <Typography.BodySmall as="label" htmlFor={id}>
                Comment (optional)
              </Typography.BodySmall>
              <Inputs.TextArea
                id={id}
                name="comment"
                value={comment}
                onChange={setComment}
                ariaDescribedBy={`${id}-help`}
                invalid={commentInvalid || undefined}
              />
              <Typography.BodySmall id={`${id}-help`}>
                {commentInvalid ? 'Use at most 1024 UTF-8 bytes. Remove null characters.' : ''}
              </Typography.BodySmall>
            </Disclosure>
            {!connected && (
              <Banner variant="warning">
                Reconnecting. Wait for the current approval state before you decide.
              </Banner>
            )}
            <div className={styles.actions}>
              <Button.TextButton
                type="submit"
                name="intent"
                value="decline"
                variant="destructive"
                disabled={pending || !connected || commentInvalid}
              >
                {pending && fetcher.formData?.get('intent') === 'decline'
                  ? 'Denying…'
                  : 'Deny this call'}
              </Button.TextButton>
              <Button.TextButton
                type="submit"
                name="intent"
                value="approve"
                disabled={pending || !connected || commentInvalid}
              >
                {pending && fetcher.formData?.get('intent') === 'approve'
                  ? 'Approving…'
                  : 'Approve this call'}
              </Button.TextButton>
            </div>
          </>
        ) : null}
        {fetcher.data && 'error' in fetcher.data && (
          <Banner variant="error">{fetcher.data.error}</Banner>
        )}
      </fetcher.Form>
    </section>
  )
}

/** Approves or denies one exact request from the queue without opening its review.
 * After a decision the row keeps its controls, disabled, until the live read removes it. */
export function QuickDecision({ name, label }: { name: string; label: string }) {
  const csrf = useShellCsrf()
  const connected = useActivityConnected()
  const fetcher = useFetcher<typeof action>({ key: `queue:${name}` })
  const pending = fetcher.state !== 'idle'
  const winning =
    fetcher.data && 'state' in fetcher.data && typeof fetcher.data.state === 'string'
      ? fetcher.data.state
      : undefined
  const decided = pending || Boolean(winning && settledApproval(winning))
  return (
    <fetcher.Form method="post" action="/activity/resolve" className={styles.controls}>
      <input type="hidden" name="csrf" value={csrf} />
      <input type="hidden" name="name" value={name} />
      <Button.TextButton
        type="submit"
        name="intent"
        value="decline"
        variant="destructive"
        disabled={decided || !connected}
        ariaLabel={`Deny: ${label}`}
      >
        {pending && fetcher.formData?.get('intent') === 'decline' ? 'Denying…' : 'Deny'}
      </Button.TextButton>
      <Button.TextButton
        type="submit"
        name="intent"
        value="approve"
        disabled={decided || !connected}
        ariaLabel={`Approve: ${label}`}
      >
        {pending && fetcher.formData?.get('intent') === 'approve' ? 'Approving…' : 'Approve'}
      </Button.TextButton>
      {fetcher.data && 'error' in fetcher.data && (
        <Typography.BodySmall role="alert">{fetcher.data.error}</Typography.BodySmall>
      )}
    </fetcher.Form>
  )
}

/** Evidence shared by Run and operation inspection, inside their technical disclosure. */
export function ApprovalTechnicalDetails({ detail }: { detail: Approval }) {
  return (
    <>
      <Typography.BodySmall>
        Approval applies only to this call. Coral checks access again before it sends the call.
      </Typography.BodySmall>
      <Typography.Code className={styles.technical}>{detail.name}</Typography.Code>
      <Typography.BodySmall>provider: {detail.provider}</Typography.BodySmall>
      <Typography.BodySmall>Deadline: {detail.deadline}</Typography.BodySmall>
      {detail.decidedAt && (
        <Typography.BodySmall as="time" dateTime={detail.decidedAt}>
          Decided: {detail.decidedAt}
        </Typography.BodySmall>
      )}
    </>
  )
}

export function CancelRun({ name }: { name: string }) {
  const csrf = useShellCsrf()
  const connected = useActivityConnected()
  const fetcher = useFetcher<typeof action>()
  const [confirming, setConfirming] = useState(false)
  return (
    <>
      <Button.TextButton
        variant="bare"
        disabled={!connected || fetcher.state !== 'idle'}
        onClick={() => setConfirming(true)}
      >
        {fetcher.state === 'idle' ? 'Cancel run' : 'Requesting cancellation…'}
      </Button.TextButton>
      {fetcher.data && (
        <Typography.Body role="status">
          {'message' in fetcher.data
            ? fetcher.data.message
            : 'error' in fetcher.data
              ? fetcher.data.error
              : null}
        </Typography.Body>
      )}
      <Dialog.Root open={confirming} onOpenChange={setConfirming}>
        <Dialog.Portal>
          <Dialog.Backdrop />
          <Dialog.Popup>
            <Dialog.Title>Cancel this run?</Dialog.Title>
            <Dialog.Description>
              Cancellation stops the whole run. It does not undo completed external effects.
            </Dialog.Description>
            <fetcher.Form
              method="post"
              action="/activity/resolve"
              onSubmit={() => setConfirming(false)}
            >
              <input type="hidden" name="csrf" value={csrf} />
              <input type="hidden" name="name" value={name} />
              <input type="hidden" name="intent" value="cancel" />
              <Dialog.Actions>
                <Button.TextButton variant="secondary" onClick={() => setConfirming(false)}>
                  Keep running
                </Button.TextButton>
                <Button.TextButton type="submit" variant="destructive" disabled={!connected}>
                  Cancel run
                </Button.TextButton>
              </Dialog.Actions>
            </fetcher.Form>
          </Dialog.Popup>
        </Dialog.Portal>
      </Dialog.Root>
    </>
  )
}
