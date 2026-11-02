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
import { useBeforeUnload, useBlocker, useFetcher, type Location } from 'react-router'
import { Banner, Button, Inputs, Typography } from '@/wax/components'
import * as Dialog from '@/wax/components/dialog'
import { Disclosure } from '@/wax/components/disclosure'
import { Deadline } from '@/wax/components/deadline'
import * as DescriptionList from '@/wax/components/description-list'
import type { IconName, IconPlateTone } from '@/wax/components/icon'
import { Logo, type LogoIcons } from '@/wax/components/logo'
import { Pill } from '@/wax/components/pill'
import type { ApprovalContext, ApprovalDetail, RequestDetail } from '../activity/types'
import { restoreFirst, useActivityArchived, useActivityConnected } from '../activity/live'
import { settledApproval } from './approval-state'
import { PanelFooter, PanelHeader, usePanelLabel } from '../activity/panel'
import { requestedBy } from '../activity/labels'
import { Arguments } from './arguments'
import { useShellCsrf } from '@/lib/use-shell-csrf'
import type { action } from '@/routes/activity-resolve'
import * as styles from './actions.css'

/** Coral holds the request but has not recorded it yet. The live read shows the outcome. */
const isRecording = (data: object | undefined) => Boolean(data && 'recording' in data)

/** The query key that names the request a review shows. */
export const REVIEW_PARAM = 'request'

/** A review ends when the path or the request it shows changes. Other query keys keep it. */
function leavesReview(current: Location, next: Location) {
  return current.pathname !== next.pathname || shownRequest(current) !== shownRequest(next)
}
const shownRequest = ({ search }: Location) => new URLSearchParams(search).get(REVIEW_PARAM)

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
  leaves = leavesReview,
}: {
  children: ReactNode
  request?: RequestDetail | null
  /** Whether a navigation leaves this review, for a page whose address says more. */
  leaves?: (current: Location, next: Location) => boolean
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
  const blocker = useBlocker(
    ({ currentLocation, nextLocation }) => dirty && leaves(currentLocation, nextLocation),
  )
  // A navigation you discarded the comment for. The comment stays until it lands, so
  // nothing that waits on it, such as live mode, can start a navigation that cancels it.
  const [discarding, setDiscarding] = useState(false)
  if (discarding && blocker.state === 'unblocked') {
    setDiscarding(false)
    update({ name: '', comment: '' })
  }
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
                    setDiscarding(true)
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

/**
 * Whether a navigation would still ask about a comment. The blocker lets go after the commit
 * that cleared the comment, so this stays true until the next frame. A page that moves on its
 * own waits for it, or it would ask you to discard a comment you already sent.
 */
export function useCommentGuard() {
  const unsent = useUnsentComment()
  const [guarded, setGuarded] = useState(unsent)
  if (unsent && !guarded) setGuarded(true)
  useEffect(() => {
    if (unsent || !guarded) return
    const frame = requestAnimationFrame(() => setGuarded(false))
    return () => cancelAnimationFrame(frame)
  }, [unsent, guarded])
  return guarded
}

/** The state your decision sets. Until it is `recorded`, Coral only holds it, and it may lose. */
export interface DecisionOutcome {
  state: string
  recorded: boolean
}

/**
 * Reads the decision sent through the fetcher `key`, and reports its outcome once, when the
 * read after it lands. It runs `onOutcome` while rendering, so that may only set this
 * component's state. A decision already shown, another review's, or one that another
 * reviewer's opposite decision beat, reports nothing.
 */
export function useDecisionOutcome(key: string, onOutcome: (outcome: DecisionOutcome) => void) {
  const decision = useFetcher<typeof action>({ key })
  const [seen, setSeen] = useState<{ key: string; data: unknown; intent?: string }>({
    key,
    data: decision.data,
  })
  const submitted = decision.formData?.get('intent')
  if (seen.key !== key) setSeen({ key, data: decision.data })
  else if (typeof submitted === 'string' && submitted !== seen.intent)
    setSeen({ ...seen, intent: submitted })
  else if (decision.state === 'idle' && seen.data !== decision.data) {
    setSeen({ key, data: decision.data })
    const state = decision.data && 'state' in decision.data ? decision.data.state : undefined
    const won = { approve: 'approved', decline: 'declined' }[seen.intent ?? '']
    if (won && isRecording(decision.data)) onOutcome({ state: won, recorded: false })
    else if (state !== undefined && state === won) onOutcome({ state, recorded: true })
  }
  return decision
}

/** One exact call, one draft, one durable decision. */
export function ApprovalReview({
  detail,
  context,
  callStartedAt,
  variant = 'activity',
  providerIcons,
}: {
  detail: ApprovalDetail
  context: ApprovalContext
  /** When the call this approval decides started, if that call is loaded. An approval records
   * no start of its own; the Run's start and the deadline are not this call's. */
  callStartedAt?: string
  /** On the Approvals page, every review is an approval of one call and a pending one waits on
   * you, so the review leaves that unsaid and shows the provider by its logo. */
  variant?: 'activity' | 'approvals'
  /** The provider's logo, on the Approvals page. Without one, its initial shows. */
  providerIcons?: LogoIcons
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
  const state = winning ?? detail.state
  const deciding = available && !settled
  const recording = deciding && isRecording(fetcher.data)
  const sent = settled || recording
  const settle = session.settle
  useEffect(() => {
    // A sent decision settles this draft even before the next authorized loader snapshot.
    if (sent) settle(detail.name)
  }, [sent, settle, detail.name])
  const commentInvalid = new TextEncoder().encode(comment).length > 1024 || comment.includes('\0')
  const tone = deciding ? 'amber' : (tones[state] ?? 'gray')
  const run = context.run
  const runIntent = context.submission?.intent || run.intent
  // The pinned decision renders at the Panel's end. A Panel named for this review holds it
  // inside the region; one name, so there is one region to find.
  const label = `Review ${detail.callPath}`
  const approvals = variant === 'approvals'
  const provider = detail.providerLabel || 'Provider'
  const panelLabel = usePanelLabel()
  return (
    <section className={styles.review} aria-label={panelLabel === label ? undefined : label}>
      <PanelHeader
        // On the Approvals page the provider's logo leads the heading, and the pill names the state.
        icon={approvals ? undefined : plateIcons[tone]}
        tone={tone}
        heading="h3"
        title={
          approvals ? (
            <span className={styles.titleWithLogo}>
              <span role="img" aria-label={provider} className={styles.logo}>
                <Logo name={provider} icons={providerIcons} size="20" />
              </span>
              {detail.callPath}
            </span>
          ) : (
            detail.callPath
          )
        }
        details={
          <>
            {!approvals && (
              <Typography.BodySmall variant="tertiary">approval request</Typography.BodySmall>
            )}
            {!approvals && detail.providerLabel && (
              <Typography.BodySmall variant="tertiary">
                Provider: {detail.providerLabel}
              </Typography.BodySmall>
            )}
            {deciding && <Deadline at={detail.deadline} variant="tertiary" />}
          </>
        }
        status={
          deciding ? (
            !approvals && <Pill color={tone}>Approval required</Pill>
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
        ) : deciding && !recording ? (
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
          {recording && <Typography.Body role="status">Saving your decision…</Typography.Body>}
          <Button.TextButton
            type="submit"
            form={formId}
            name="intent"
            value="decline"
            variant="destructive"
            disabled={pending || recording || !connected || commentInvalid}
            busy={submitting === 'decline'}
          >
            {approvals ? 'Decline' : 'Decline this call'}
          </Button.TextButton>
          <Button.TextButton
            type="submit"
            form={formId}
            name="intent"
            value="approve"
            disabled={pending || recording || !connected || commentInvalid}
            busy={submitting === 'approve'}
          >
            {approvals ? 'Approve' : 'Approve this call'}
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

/** Approves or declines one exact request from its row without opening its review.
 * After a decision the row keeps its controls, disabled, until the live read removes it. */
export function QuickDecision({ name, label }: { name: string; label: string }) {
  const csrf = useShellCsrf()
  const connected = useActivityConnected()
  const fetcher = useFetcher<typeof action>({ key: `quick-decision:${name}` })
  const pending = fetcher.state !== 'idle'
  const submitting = pending ? fetcher.formData?.get('intent') : undefined
  const winning =
    fetcher.data && 'state' in fetcher.data && typeof fetcher.data.state === 'string'
      ? fetcher.data.state
      : undefined
  // The status shows until the live read removes the row.
  const recording = isRecording(fetcher.data)
  const decided = pending || recording || Boolean(winning && settledApproval(winning))
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
        ariaLabel={`Decline: ${label}`}
        busy={submitting === 'decline'}
      >
        Decline
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
      {recording && (
        <Typography.BodySmall role="status">Saving your decision…</Typography.BodySmall>
      )}
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
  // The panel that shows this button removes it once the live read shows the Run ended.
  const recording = isRecording(fetcher.data)
  const status = !fetcher.data
    ? null
    : recording
      ? 'Canceling the run…'
      : 'message' in fetcher.data
        ? fetcher.data.message
        : 'error' in fetcher.data
          ? fetcher.data.error
          : null
  return (
    <>
      <Button.TextButton
        variant={variant}
        disabled={!connected || recording || fetcher.state !== 'idle'}
        busy={fetcher.state !== 'idle'}
        onClick={() => setConfirming(true)}
      >
        Cancel run
      </Button.TextButton>
      {status && <Typography.Body role="status">{status}</Typography.Body>}
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
