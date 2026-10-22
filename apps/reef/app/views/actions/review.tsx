import { Instant } from '../activity/instant'
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
import { Deadline } from '@/wax/components/deadline'
import * as DescriptionList from '@/wax/components/description-list'
import type { IconName, IconPlateTone } from '@/wax/components/icon'
import { Pill } from '@/wax/components/pill'
import type { RequestDetail, RunDetail } from '../activity/types'
import { restoreFirst, useActivityArchived, useActivityConnected } from '../activity/live'
import { settledApproval } from './approval-state'
import { PanelFooter, PanelHeader, usePanelLabel } from '../activity/panel'
import { requestedBy } from '../activity/labels'
import { Arguments } from './arguments'
import { ProviderChanges } from './provider-changes'
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
export function ApprovalReview({
  detail,
  context,
  callStartedAt,
}: {
  detail: Approval
  context: RunDetail
  /** When the call this approval decides started, if that call is loaded. An approval records
   * no start of its own; the Run's start and the deadline are not this call's. */
  callStartedAt?: string
}) {
  const csrf = useShellCsrf()
  const connected = useActivityConnected()
  const archived = useActivityArchived()
  const fetcher = useFetcher<typeof action>({ key: detail.name })
  const session = useContext(DraftContext)
  if (!session) throw new Error('Approval review needs its action review session')
  const comment = session.draft.name === detail.name ? session.draft.comment : ''
  const setComment = (value: string) => session.update({ name: detail.name, comment: value })
  const id = useId()
  const formId = `${id}-decision`
  const available = detail.actionable === true
  const pending = fetcher.state !== 'idle'
  const submitting = pending ? fetcher.formData?.get('intent') : undefined
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
  const deciding = available && !settled
  const tone = deciding ? 'amber' : (tones[state] ?? 'gray')
  const run = context.run
  const runIntent = context.submission?.intent || run.intent
  // The pinned decision renders at the Panel's end. A Panel named for this review holds it
  // inside the region; one name, so there is one region to find.
  const label = `Review ${detail.callPath}`
  const panelLabel = usePanelLabel()
  return (
    <section className={styles.review} aria-label={panelLabel === label ? undefined : label}>
      <PanelHeader
        icon={plateIcons[tone]}
        tone={tone}
        heading="h3"
        title={detail.callPath}
        details={
          <>
            <Typography.BodySmall variant="tertiary">approval request</Typography.BodySmall>
            {detail.providerLabel && (
              <Typography.BodySmall variant="tertiary">
                Provider: {detail.providerLabel}
              </Typography.BodySmall>
            )}
            {deciding && <Deadline at={detail.deadline} variant="tertiary" />}
          </>
        }
        status={
          deciding ? (
            <Pill color={tone}>Approval required</Pill>
          ) : (
            <Pill color={tone} role="status">
              {settledApproval(state)?.label ?? 'Approval unavailable'}
            </Pill>
          )
        }
      />
      <DescriptionList.List>
        {detail.taskIntent && (
          <DescriptionList.Row label="Task intent">{detail.taskIntent}</DescriptionList.Row>
        )}
        {runIntent && <DescriptionList.Row label="Run intent">{runIntent}</DescriptionList.Row>}
        <DescriptionList.Row label="Requested by">{requestedBy(run)}</DescriptionList.Row>
        {callStartedAt && (
          <DescriptionList.Row label="Call started">
            <Instant value={callStartedAt} />
          </DescriptionList.Row>
        )}
      </DescriptionList.List>
      <Arguments source={detail.arguments} />
      {detail.providerChanges && <ProviderChanges changes={detail.providerChanges} />}
      {!deciding && (detail.approver || detail.decidedAt || detail.comment !== null) && (
        <DescriptionList.List>
          {detail.approver && (
            <DescriptionList.Row label="Reviewed by">
              {detail.approverLabel || 'Previous reviewer'}
            </DescriptionList.Row>
          )}
          {detail.decidedAt && (
            <DescriptionList.Row label="Decided">
              <Instant value={detail.decidedAt} />
            </DescriptionList.Row>
          )}
          {detail.comment !== null && (
            <DescriptionList.Row label="Comment">{detail.comment}</DescriptionList.Row>
          )}
        </DescriptionList.List>
      )}
      <fetcher.Form id={formId} method="post" action="/activity/resolve" className={styles.stack}>
        <input type="hidden" name="csrf" value={csrf} />
        <input type="hidden" name="name" value={detail.name} />
        {deciding && archived ? (
          <Typography.Body role="status">{restoreFirst(archived, 'decide')}</Typography.Body>
        ) : deciding ? (
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
          </>
        ) : null}
        {fetcher.data && 'error' in fetcher.data && (
          <Banner variant="error">{fetcher.data.error}</Banner>
        )}
      </fetcher.Form>
      {/* The decision stays in view while you read. The buttons submit the form above. */}
      {deciding && !archived && (
        <PanelFooter>
          <Button.TextButton
            type="submit"
            form={formId}
            name="intent"
            value="decline"
            variant="destructive"
            disabled={pending || !connected || commentInvalid}
            busy={submitting === 'decline'}
          >
            Deny this call
          </Button.TextButton>
          <Button.TextButton
            type="submit"
            form={formId}
            name="intent"
            value="approve"
            disabled={pending || !connected || commentInvalid}
            busy={submitting === 'approve'}
          >
            Approve this call
          </Button.TextButton>
        </PanelFooter>
      )}
    </section>
  )
}

const tones: Record<string, IconPlateTone> = { approved: 'green', declined: 'red' }
const plateIcons: Record<IconPlateTone, IconName> = {
  amber: 'ShieldAlert',
  green: 'ShieldCheck',
  red: 'ShieldX',
  gray: 'Shield',
}

/** Approves or denies one exact request from the queue without opening its review.
 * After a decision the row keeps its controls, disabled, until the live read removes it. */
export function QuickDecision({ name, label }: { name: string; label: string }) {
  const csrf = useShellCsrf()
  const connected = useActivityConnected()
  const fetcher = useFetcher<typeof action>({ key: `queue:${name}` })
  const pending = fetcher.state !== 'idle'
  const submitting = pending ? fetcher.formData?.get('intent') : undefined
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
        busy={submitting === 'decline'}
      >
        Deny
      </Button.TextButton>
      <Button.TextButton
        type="submit"
        name="intent"
        value="approve"
        disabled={decided || !connected}
        ariaLabel={`Approve: ${label}`}
        busy={submitting === 'approve'}
      >
        Approve
      </Button.TextButton>
      {fetcher.data && 'error' in fetcher.data && (
        <Typography.BodySmall role="alert">{fetcher.data.error}</Typography.BodySmall>
      )}
    </fetcher.Form>
  )
}

/** Cancels the whole Run after a confirmation. A Run panel shows it as a destructive button. */
export function CancelRun({
  name,
  variant = 'bare',
}: {
  name: string
  variant?: 'bare' | 'destructive'
}) {
  const csrf = useShellCsrf()
  const connected = useActivityConnected()
  const fetcher = useFetcher<typeof action>()
  const [confirming, setConfirming] = useState(false)
  return (
    <>
      <Button.TextButton
        variant={variant}
        disabled={!connected || fetcher.state !== 'idle'}
        busy={fetcher.state !== 'idle'}
        onClick={() => setConfirming(true)}
      >
        Cancel run
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
