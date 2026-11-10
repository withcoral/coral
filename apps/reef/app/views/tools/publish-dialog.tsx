import { useEffect, useId, useRef, useState } from 'react'
import { useFetcher } from 'react-router'

import { PolicyToggle, type PolicyState } from '@/components/policy-toggle'
import { sharedToolCallPath, sharedToolPath } from '@/lib/call-root'
import { Fields } from '@/views/management/forms'
import { Section } from '@/views/management/layout'
import type { ActionResult, FormContext } from '@/views/management/types'
import { Banner, Button, Dialog, Inputs, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { CodeChip } from '@/wax/components/code-chip'
import { DiffView } from '@/wax/components/diff-view'
import { Markdown } from '@/wax/components/markdown'
import { SegmentedControl } from '@/wax/components/segmented-control'

import { sharedToolPublishPath } from './addresses'
import { Diagnostics } from './tool-source'
import type { PublishOrigin, PublishReviewResult, PublishReviewView } from './types'
import * as styles from './tools.css'

// The dialog caps the source and each diff; the rest scroll in place.
const REVIEW_LINES = 24

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

function PublishDialogBody({
  workspacePath,
  form,
  origin,
  path: initialPath,
  policy,
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
  const action = sharedToolPublishPath(workspacePath)
  const href = `${action}?${reviewQuery(origin, reviewPath)}`
  const { load } = review
  useEffect(() => {
    void load(href)
  }, [load, href])

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

  const reviewing = review.state !== 'idle' || !review.data
  const shown = reviewing ? null : review.data!
  const reviewed = shown?.review ?? null
  const enteredPath = pathInput.trim()
  const revision = (reviewed?.previous?.revision ?? 0) + 1
  // The publish sends the review on screen, so it waits for one of the path in the field.
  const cannotPublish = publishing || !reviewed || enteredPath !== reviewed.path
  return (
    <Dialog.Popup size="xxl" fullWidthOnMobile>
      <Dialog.Title>
        {origin.kind === 'restore' ? `Restore revision ${origin.revision}` : 'Publish shared tool'}
      </Dialog.Title>
      {origin.kind === 'restore' ? (
        <div className={styles.callPath}>
          <CodeChip>{sharedToolCallPath(initialPath)}</CodeChip>
        </div>
      ) : (
        <form
          className={styles.publishPath}
          onSubmit={(event) => {
            event.preventDefault()
            if (enteredPath) setReviewPath(enteredPath)
          }}
        >
          <div className={styles.publishPathField}>
            <Typography.BodySmallStrong as="label" htmlFor={pathId}>
              Shared tool path
            </Typography.BodySmallStrong>
            <Inputs.TextInput
              id={pathId}
              value={pathInput}
              onChange={setPathInput}
              autoComplete="off"
              spellCheck={false}
              maxLength={256}
              disabled={publishing}
            />
          </div>
          <Button.TextButton
            type="submit"
            variant="secondary"
            disabled={publishing || !enteredPath || enteredPath === reviewPath}
          >
            Review path
          </Button.TextButton>
        </form>
      )}
      {reviewing && (
        <Typography.Body as="p" role="status">
          Reviewing…
        </Typography.Body>
      )}
      {shown?.refusal && shown.refusal !== error && (
        <Banner variant="error">{shown.refusal}</Banner>
      )}
      {reviewed && <Review review={reviewed} />}
      <publish.Form
        method="post"
        action={action}
        className={styles.stack}
        aria-busy={publishing}
        onSubmit={(event) => {
          if (cannotPublish) event.preventDefault()
        }}
      >
        <Fields form={submission} intent="publishSharedTool" />
        {reviewed && (
          <>
            <PublishFields review={reviewed} />
            <section aria-label="Default policy" className={styles.drawerSection}>
              <Typography.HeadingSmall as="h2">Default policy</Typography.HeadingSmall>
              <Typography.Body as="p">The shared tool starts with this policy.</Typography.Body>
              <input
                type="hidden"
                name="access"
                value={chosenPolicy ?? reviewed.defaultPolicy ?? policy}
              />
              <PolicyToggle
                label="Default policy"
                value={chosenPolicy ?? reviewed.defaultPolicy ?? policy}
                onChange={setChosenPolicy}
                disabled={publishing}
              />
            </section>
          </>
        )}
        {error && <Banner variant="error">{error}</Banner>}
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
      </publish.Form>
    </Dialog.Popup>
  )
}

/**
 * What the publish sends beside the policy: what it copies, where, the
 * revision it replaces, and the reviewed content. The content is one JSON
 * field, because a form post would rewrite the line breaks of the source.
 */
function PublishFields({ review }: { review: PublishReviewView }) {
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

/** What the owner approves: the content, what it changes, and what it calls. */
function Review({ review }: { review: PublishReviewView }) {
  const { previous } = review
  const [sourceView, setSourceView] = useState<'changes' | 'full'>(previous ? 'changes' : 'full')
  const revisions = new Map(review.sharedTools.map((tool) => [tool.path, tool.revision]))
  const sharedRevision = (callPath: string) => revisions.get(sharedToolPath(callPath) ?? '')
  return (
    <div className={styles.stack}>
      <Typography.Body as="p">
        {previous
          ? `This publish replaces revision ${previous.revision} of ${sharedToolCallPath(review.path)}.`
          : `This publish creates ${sharedToolCallPath(review.path)}.`}
      </Typography.Body>
      {review.origin.kind === 'private' && (
        <Banner variant="warning" title="Remove tokens and IDs that only you should see.">
          Every member of the workspace can read this source.
        </Banner>
      )}
      <Section title="Description">
        <Markdown>{review.description}</Markdown>
        {previous && previous.description !== review.description && (
          <DiffView
            label="Description changes"
            before={previous.description}
            after={review.description}
          />
        )}
      </Section>
      <section aria-label="Source" className={styles.stack}>
        <div className={styles.reviewHeading}>
          <Typography.HeadingSmall as="h2">Source</Typography.HeadingSmall>
          {previous && (
            <SegmentedControl
              label="Source view"
              value={sourceView}
              onChange={setSourceView}
              options={[
                { value: 'changes', label: `Changes from revision ${previous.revision}` },
                { value: 'full', label: 'Full source' },
              ]}
            />
          )}
        </div>
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
      </section>
      <Section title="Declaration">
        <CodeBlock value={review.declaration} format="typescript" framed />
        {previous && (
          <DiffView
            label="Declaration changes"
            before={previous.declaration}
            after={review.declaration}
            maxLines={REVIEW_LINES}
            unchanged="The declaration does not change."
          />
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
