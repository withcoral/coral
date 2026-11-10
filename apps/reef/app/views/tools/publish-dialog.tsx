import classNames from 'classnames'
import { useEffect, useId, useMemo, useRef, useState, type ReactNode } from 'react'
import { useFetcher } from 'react-router'

import { PolicyToggle, type PolicyState } from '@/components/policy-toggle'
import { sharedToolCallPath, sharedToolPath } from '@/lib/call-root'
import { countLabel } from '@/utils/count-label'
import { Fields } from '@/views/management/forms'
import { Section } from '@/views/management/layout'
import { SavedAt } from '@/views/management/saved-at'
import type { ActionResult, FormContext } from '@/views/management/types'
import { Banner, Button, Dialog, Inputs, Typography } from '@/wax/components'
import { CodeBlock, sourceLines } from '@/wax/components/code-block'
import { CodeChip } from '@/wax/components/code-chip'
import { DiffView, diffCounts } from '@/wax/components/diff-view'
import { Icon, type IconColor, type IconName } from '@/wax/components/icon'
import { Markdown } from '@/wax/components/markdown'
import { SegmentedControl } from '@/wax/components/segmented-control'
import { InfoTip } from '@/wax/components/tooltip'

import { sharedToolPublishPath } from './addresses'
import { publisherName } from './shared-tool-detail'
import { Diagnostics } from './tool-source'
import type {
  PublishOrigin,
  PublishReviewResult,
  PublishReviewView,
  PublishedRevisionView,
  ToolDiagnosticView,
} from './types'
import * as styles from './tools.css'

// The dialog caps the source and each diff; the rest scroll in place.
const REVIEW_LINES = 24
// A pause in typing this long reviews the path in the field.
const PATH_REVIEW_DELAY_MS = 500
const SHARED_ROOT = sharedToolCallPath('')

export interface PublishDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  workspacePath: string
  form: FormContext
  /** What the publish copies: a private tool, or a revision to restore. */
  origin: PublishOrigin
  /** The shared tool path the dialog starts on. A restore keeps it. */
  path: string
  /** The policy the dialog starts on when no revision at the path sets one. */
  policy: PolicyState
  /** The reader's user, so the dialog names their own revisions as theirs. */
  currentUser: string
}

/**
 * The review of a publish, and the publish of what it shows. The dialog reads
 * the review from the publish route's loader, and posts the exact reviewed
 * description, source and declaration to its action. When Coral refuses the
 * publish, the dialog says why and reviews again.
 */
export function PublishDialog({ open, onOpenChange, ...props }: PublishDialogProps) {
  const [pending, setPending] = useState(false)
  return (
    <Dialog.Root
      open={open}
      onOpenChange={(next) => {
        if (!pending) onOpenChange(next)
      }}
    >
      {open && (
        <Dialog.Portal>
          <Dialog.Backdrop />
          <PublishDialogBody
            {...props}
            onPendingChange={setPending}
            close={() => onOpenChange(false)}
          />
        </Dialog.Portal>
      )}
    </Dialog.Root>
  )
}

/** The address of the review of a publish from `origin` at `path`. */
function reviewQuery(origin: PublishOrigin, path: string) {
  const query = new URLSearchParams({ path })
  if (origin.kind === 'restore') query.set('restore', String(origin.revision))
  else {
    query.set('tool', origin.path)
    if (origin.agent) query.set('agent', origin.agent)
  }
  return query
}

/** The shared tool path in the field, without the root the field shows before it. */
function enteredPathOf(input: string) {
  const path = input.trim()
  return path.startsWith(SHARED_ROOT) ? path.slice(SHARED_ROOT.length) : path
}

function PublishDialogBody({
  workspacePath,
  form,
  origin,
  path: initialPath,
  policy,
  currentUser,
  close,
  onPendingChange,
}: Omit<PublishDialogProps, 'open' | 'onOpenChange'> & {
  close: () => void
  onPendingChange: (pending: boolean) => void
}) {
  const review = useFetcher<PublishReviewResult>()
  const publish = useFetcher<ActionResult>()
  // The fields of the form as the dialog opened, as every action dialog keeps them.
  const [submission] = useState(form)
  const [pathInput, setPathInput] = useState(initialPath)
  const [reviewPath, setReviewPath] = useState(initialPath)
  // The owner's choice, once they make one.
  const [chosenPolicy, setChosenPolicy] = useState<PolicyState | null>(null)
  const pathId = useId()
  const pathStatusId = useId()
  const action = sharedToolPublishPath(workspacePath)
  const href = `${action}?${reviewQuery(origin, reviewPath)}`
  const { load } = review
  useEffect(() => {
    void load(href)
  }, [load, href])

  // A pause in typing reviews the path in the field, so the owner sees at
  // once what is there. Enter reviews it without the pause.
  const enteredPath = enteredPathOf(pathInput)
  useEffect(() => {
    if (!enteredPath || enteredPath === reviewPath) return
    const timer = setTimeout(() => setReviewPath(enteredPath), PATH_REVIEW_DELAY_MS)
    return () => clearTimeout(timer)
  }, [enteredPath, reviewPath])

  const publishing = publish.state !== 'idle'
  useEffect(() => {
    onPendingChange(publishing)
  }, [publishing, onPendingChange])
  // A publish that Coral accepts opens the shared tool. One that it refuses
  // stays here with the reason, and the review runs again: another owner's
  // revision or a changed declaration is then what the owner sees.
  const settling = useRef(false)
  const error = publish.data?.error ?? null
  useEffect(() => {
    if (publishing) {
      settling.current = true
      return
    }
    if (!settling.current) return
    settling.current = false
    if (error) void load(href)
    else close()
  }, [publishing, error, load, href, close])

  // The last review stays on screen while the next one runs, so the dialog
  // does not jump as the owner types. It cannot publish until the review of
  // the path in the field arrives.
  const loading = review.state !== 'idle'
  const shown = review.data ?? null
  const reviewed = shown?.review ?? null
  const checking = loading || !shown || enteredPath !== reviewPath
  const revision = (reviewed?.previous?.revision ?? 0) + 1
  const cannotPublish = publishing || checking || !reviewed || enteredPath !== reviewed.path
  const access = chosenPolicy ?? reviewed?.defaultPolicy ?? policy
  return (
    <Dialog.Popup size="xxl" fullWidthOnMobile>
      <publish.Form
        method="post"
        action={action}
        className={styles.stack}
        aria-busy={publishing}
        onSubmit={(event) => {
          if (cannotPublish) event.preventDefault()
        }}
      >
        <Dialog.Header>
          <Dialog.Title>
            {origin.kind === 'restore'
              ? `Restore revision ${origin.revision}`
              : 'Publish shared tool'}
          </Dialog.Title>
          <Dialog.Close ariaLabel="Close" />
        </Dialog.Header>
        <div className={styles.publishPath}>
          {origin.kind === 'restore' ? (
            <div className={styles.callPath}>
              <CodeChip>{sharedToolCallPath(initialPath)}</CodeChip>
            </div>
          ) : (
            <>
              <Typography.BodySmallStrong as="label" htmlFor={pathId}>
                Shared tool path
              </Typography.BodySmallStrong>
              <Inputs.TextInput
                id={pathId}
                // The owner starts on the path, the one thing they may change.
                autoFocus
                prefix={SHARED_ROOT}
                value={pathInput}
                onChange={setPathInput}
                onKeyDown={(event) => {
                  // Enter reviews the path. It does not publish a review of another path.
                  if (event.key !== 'Enter') return
                  event.preventDefault()
                  if (enteredPath) setReviewPath(enteredPath)
                }}
                ariaDescribedBy={pathStatusId}
                autoComplete="off"
                spellCheck={false}
                maxLength={256}
                disabled={publishing}
                invalid={!checking && Boolean(shown?.refusal)}
              />
            </>
          )}
          <div id={pathStatusId} role="status">
            <PathStatus
              empty={!enteredPath}
              checking={checking}
              review={reviewed}
              currentUser={currentUser}
            />
          </div>
        </div>
        {!checking && shown?.refusal && shown.refusal !== error && (
          <Banner variant="error">{shown.refusal}</Banner>
        )}
        {reviewed && <Review key={reviewed.path} review={reviewed} />}
        <Fields form={submission} intent="publishSharedTool" />
        {reviewed && <PublishFields review={reviewed} access={access} />}
        <div
          className={classNames(
            styles.publishFooter,
            Dialog.pinnedBottomClassName,
            Dialog.flushInlineClassName,
          )}
        >
          {error && <Banner variant="error">{error}</Banner>}
          <div className={styles.publishFooterRow}>
            {/* The policy shows once a review names the one the shared tool has, so it does not jump. */}
            {reviewed ? (
              <div className={styles.publishPolicy}>
                <Typography.BodySmallStrong as="span">Default policy</Typography.BodySmallStrong>
                <PolicyToggle
                  label="Default policy"
                  value={access}
                  onChange={setChosenPolicy}
                  disabled={publishing}
                  withLabels
                />
                <InfoTip ariaLabel="About the default policy">
                  The shared tool starts with this policy. You can change it on the tools page.
                </InfoTip>
              </div>
            ) : (
              <span />
            )}
            <Dialog.Actions>
              <Button.TextButton
                type="button"
                variant="secondary"
                disabled={publishing}
                onClick={close}
              >
                Cancel
              </Button.TextButton>
              <Button.TextButton
                type="submit"
                variant="primary"
                disabled={cannotPublish}
                busy={publishing}
              >
                {reviewed ? `Publish revision ${revision}` : 'Publish'}
              </Button.TextButton>
            </Dialog.Actions>
          </div>
        </div>
      </publish.Form>
    </Dialog.Popup>
  )
}

/**
 * What is at the path: nothing yet, or the revision the publish replaces and
 * who published it. An owner who overwrites another owner's tool sees whose.
 */
function PathStatus({
  empty,
  checking,
  review,
  currentUser,
}: {
  empty: boolean
  checking: boolean
  review: PublishReviewView | null
  currentUser: string
}) {
  if (empty) return <Fact icon="Info">Enter a shared tool path.</Fact>
  if (checking) return <Fact icon="Search">Checking the path…</Fact>
  if (!review) return null
  const { previous } = review
  if (!previous) return <Fact icon="CirclePlus">This publish creates a new shared tool.</Fact>
  return (
    <Fact icon="RefreshCw" color="warning">
      <span>
        {`This publish replaces revision ${previous.revision}, published by ${publisherName(previous.publisher, currentUser)} on `}
        <SavedAt at={previous.publishedAt} />.
      </span>
    </Fact>
  )
}

/** One line of the review with an icon that tells its kind apart, not only its color. */
function Fact({
  icon,
  color = 'secondary',
  children,
}: {
  icon: IconName
  color?: IconColor
  children: ReactNode
}) {
  return (
    <span className={styles.reviewFact}>
      <Icon name={icon} size="16" color={color} className={styles.reviewFactIcon} />
      <Typography.BodySmall as="span">{children}</Typography.BodySmall>
    </span>
  )
}

/**
 * What the publish sends beside the policy: what it copies, where, the
 * revision it replaces, and the reviewed content. The content is one JSON
 * field, because a form post would rewrite the line breaks of the source.
 */
function PublishFields({ review, access }: { review: PublishReviewView; access: PolicyState }) {
  const { origin } = review
  return (
    <>
      {origin.kind === 'restore' ? (
        <input type="hidden" name="restore" value={origin.revision} />
      ) : (
        <>
          <input type="hidden" name="tool" value={origin.path} />
          {origin.agent && <input type="hidden" name="agent" value={origin.agent} />}
        </>
      )}
      <input type="hidden" name="path" value={review.path} />
      <input type="hidden" name="expectedRevision" value={review.previous?.revision ?? 0} />
      <input type="hidden" name="access" value={access} />
      <input
        type="hidden"
        name="reviewed"
        value={JSON.stringify({
          description: review.description,
          source: review.source,
          declaration: review.declaration,
        })}
      />
    </>
  )
}

/** The diagnostics as counts by kind, errors first, such as "1 error, 2 warnings". */
function diagnosticCounts(diagnostics: ToolDiagnosticView[]) {
  const nouns = [
    ['error', 'error'],
    ['warning', 'warning'],
    ['suggestion', 'suggestion'],
    ['message', 'message'],
  ] as const
  return nouns
    .map(([category, noun]) => {
      const count = diagnostics.filter((d) => d.category === category).length
      return count > 0 ? countLabel(count, noun) : null
    })
    .filter(Boolean)
    .join(', ')
}

/** What the publish changes from `previous`, line by line, or what it creates. */
function sourceChange(review: PublishReviewView, previous: PublishedRevisionView | null) {
  if (!previous) {
    const lines = sourceLines(review.source)
    // A final line break ends the last line; it does not start another.
    const count = lines.at(-1) === '' ? lines.length - 1 : lines.length
    return `The source has ${countLabel(count, 'line')}.`
  }
  if (previous.source === review.source) return 'The source does not change.'
  const { added, removed } = diffCounts(previous.source, review.source)
  return `The source changes: ${countLabel(added, 'line')} added, ${removed.toLocaleString('en-US')} removed.`
}

/** The facts an owner decides on, before the details that show them. */
function Summary({ review }: { review: PublishReviewView }) {
  const { previous, calledTools, sharedTools, runtimeBuiltPaths, diagnostics } = review
  const errors = diagnostics.some((d) => d.category === 'error')
  const counts = diagnosticCounts(diagnostics)
  return (
    <ul aria-label="Summary" className={styles.reviewSummary}>
      {previous && (
        <li>
          <Fact icon="NotepadText">
            {previous.description === review.description
              ? 'The description does not change.'
              : 'The description changes.'}
          </Fact>
        </li>
      )}
      <li>
        <Fact icon="FileCode">{sourceChange(review, previous)}</Fact>
      </li>
      <li>
        <Fact icon="Waypoints">
          {calledTools.length === 0
            ? 'It calls no tool by a fixed path.'
            : `It calls ${countLabel(calledTools.length, 'tool')} by a fixed path${
                sharedTools.length > 0
                  ? `, ${sharedTools.length.toLocaleString('en-US')} of them shared`
                  : ''
              }.`}
          {runtimeBuiltPaths.length > 0 && ' It also builds tool paths when it runs.'}
        </Fact>
      </li>
      <li>
        <Fact icon={errors ? 'CircleAlert' : 'ListChecks'} color={errors ? 'error' : 'secondary'}>
          {counts ? `Diagnostics: ${counts}.` : 'It has no diagnostics.'}
        </Fact>
      </li>
    </ul>
  )
}

/** What the owner approves: a summary, then the content, what it changes, and what it calls. */
function Review({ review }: { review: PublishReviewView }) {
  const { previous } = review
  const [sourceView, setSourceView] = useState<'changes' | 'full'>(previous ? 'changes' : 'full')
  const revisions = useMemo(
    () => new Map(review.sharedTools.map((tool) => [tool.path, tool.revision])),
    [review.sharedTools],
  )
  const sharedRevision = (callPath: string) => revisions.get(sharedToolPath(callPath) ?? '')
  const declarationChanges = previous !== null && previous.declaration !== review.declaration
  return (
    <div className={styles.stack}>
      <Summary review={review} />
      {declarationChanges && (
        <Banner variant="warning" title="The declaration changes.">
          Programs and tools that call this shared tool can fail. Check the declaration changes.
        </Banner>
      )}
      <Section title="Description">
        {previous && previous.description !== review.description ? (
          <DiffView
            label="Description changes"
            before={previous.description}
            after={review.description}
          />
        ) : (
          <Markdown>{review.description}</Markdown>
        )}
      </Section>
      <Section title="Declaration">
        {declarationChanges ? (
          <DiffView
            label="Declaration changes"
            before={previous.declaration}
            after={review.declaration}
            maxLines={REVIEW_LINES}
          />
        ) : (
          <CodeBlock value={review.declaration} format="typescript" framed />
        )}
      </Section>
      <Section
        title="Source"
        actions={
          previous && (
            <SegmentedControl
              label="Source view"
              value={sourceView}
              onChange={setSourceView}
              options={[
                { value: 'changes', label: `Changes from revision ${previous.revision}` },
                { value: 'full', label: 'Full source' },
              ]}
            />
          )
        }
      >
        {review.origin.kind === 'private' && (
          <Banner variant="warning" title="Remove tokens and IDs that only you should see.">
            Every member of the workspace can read this source.
          </Banner>
        )}
        {previous && sourceView === 'changes' ? (
          <DiffView
            label="Source changes"
            before={previous.source}
            after={review.source}
            maxLines={REVIEW_LINES}
            unchanged="The source does not change."
          />
        ) : (
          <CodeBlock value={review.source} format="typescript" framed maxLines={REVIEW_LINES} />
        )}
      </Section>
      <Section title="Tools it calls">
        {review.calledTools.length === 0 ? (
          <Typography.Body as="p">This source calls no tool by a fixed path.</Typography.Body>
        ) : (
          <ul aria-label="Tools it calls" className={styles.calledTools}>
            {review.calledTools.map((callPath) => {
              const shared = sharedRevision(callPath)
              return (
                <li key={callPath} className={styles.calledTool}>
                  <CodeChip>{callPath}</CodeChip>
                  {shared !== undefined && (
                    <Typography.BodySmall as="span" variant="secondary">
                      {`Revision ${shared}`}
                    </Typography.BodySmall>
                  )}
                </li>
              )
            })}
          </ul>
        )}
        {review.runtimeBuiltPaths.length > 0 && (
          <Banner variant="warning" title="This list can be incomplete.">
            <span>The source builds tool paths when it runs. They start with: </span>
            {review.runtimeBuiltPaths.map((start, index) => (
              <span key={start}>
                {index > 0 && ', '}
                <CodeChip>{start}</CodeChip>
              </span>
            ))}
          </Banner>
        )}
      </Section>
      <Section
        title="Diagnostics"
        description="These diagnostics do not stop the shared tool from running."
      >
        <Diagnostics
          diagnostics={review.diagnostics}
          truncated={review.diagnosticsTruncated}
          none="This source has no diagnostics."
        />
      </Section>
    </div>
  )
}
