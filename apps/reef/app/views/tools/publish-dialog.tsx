import classNames from 'classnames'
import { useEffect, useId, useMemo, useRef, useState, type ReactNode } from 'react'
import { useFetcher } from 'react-router'

import { PolicyToggle, type PolicyState } from '@/components/policy-toggle'
import { sharedToolCallPath, sharedToolPath } from '@/lib/call-root'
import { countLabel } from '@/utils/count-label'
import { CallTool } from '@/views/activity/call-tool'
import { Fields } from '@/views/management/forms'
import { SavedAt } from '@/views/management/saved-at'
import type { ActionResult, FormContext } from '@/views/management/types'
import { Banner, Button, Dialog, Inputs, Tabs, Typography } from '@/wax/components'
import { CodeBlock } from '@/wax/components/code-block'
import { DiffView } from '@/wax/components/diff-view'
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
  ToolDiagnosticView,
} from './types'
import * as styles from './tools.css'

// The code pane caps the source and each diff; the rest scroll in place.
const REVIEW_LINES = 28
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
        {/* What the owner decides on the left; the code they review on the right. */}
        <div className={styles.publishSplit}>
          <div className={styles.publishSide}>
            <div className={styles.publishPath}>
              {origin.kind === 'restore' ? (
                // A restore keeps its shared tool's path, named as the page names tools.
                <CallTool callPath={sharedToolCallPath(initialPath)} />
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
            {reviewed && <Decisions key={reviewed.path} review={reviewed} />}
            {/* The policy shows once a review names the one the shared tool has, so it does not jump. */}
            {reviewed && (
              <div className={styles.publishPart}>
                <span className={styles.drawerGroup}>
                  <Typography.BodySmallStrong as="span">Default policy</Typography.BodySmallStrong>
                  <InfoTip ariaLabel="About the default policy">
                    The shared tool starts with this policy. You can change it on the tools page.
                  </InfoTip>
                </span>
                <PolicyToggle
                  label="Default policy"
                  value={access}
                  onChange={setChosenPolicy}
                  disabled={publishing}
                  withLabels
                />
              </div>
            )}
          </div>
          {reviewed && <Code key={reviewed.path} review={reviewed} />}
        </div>
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
          <div className={styles.publishFooterActions}>
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
                Publish
              </Button.TextButton>
            </Dialog.Actions>
          </div>
        </div>
      </publish.Form>
    </Dialog.Popup>
  )
}

/**
 * What the review found at the path, when the owner must know it: that the
 * field is empty, that the review is still running, or the revision the
 * publish replaces and who published it. An owner who overwrites another
 * owner's tool sees whose. A new shared tool needs no word.
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
  const previous = review?.previous
  if (!previous) return null
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

const declarationChanges = (review: PublishReviewView) =>
  review.previous !== null && review.previous.declaration !== review.declaration

/**
 * What the owner decides on, beside the code: the description every member
 * reads, the tools the source calls, and what to look at before the publish.
 * A part with nothing to say shows nothing.
 */
function Decisions({ review }: { review: PublishReviewView }) {
  const revisions = useMemo(
    () => new Map(review.sharedTools.map((tool) => [tool.path, tool.revision])),
    [review.sharedTools],
  )
  const errors = review.diagnostics.some((d) => d.category === 'error')
  return (
    <>
      <div className={styles.publishPart}>
        <Typography.BodySmallStrong as="span">Description</Typography.BodySmallStrong>
        <Markdown>{review.description}</Markdown>
      </div>
      {(review.calledTools.length > 0 || review.runtimeBuiltPaths.length > 0) && (
        <div className={styles.publishPart}>
          <Typography.BodySmallStrong as="span">Tools it calls</Typography.BodySmallStrong>
          {review.calledTools.length > 0 && (
            <ul aria-label="Tools it calls" className={styles.calledTools}>
              {review.calledTools.map((callPath) => {
                const shared = revisions.get(sharedToolPath(callPath) ?? '')
                return (
                  <li key={callPath} className={styles.calledTool}>
                    <CallTool callPath={callPath} />
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
            <Fact icon="TriangleAlert" color="warning">
              {`This list can be incomplete. The source also builds tool paths when it runs, from ${review.runtimeBuiltPaths.join(', ')}.`}
            </Fact>
          )}
        </div>
      )}
      {(declarationChanges(review) || review.diagnostics.length > 0) && (
        <div className={styles.publishPart}>
          {declarationChanges(review) && (
            <Fact icon="TriangleAlert" color="warning">
              The declaration changes. Programs and tools that call this shared tool can fail.
            </Fact>
          )}
          {review.diagnostics.length > 0 && (
            <Fact
              icon={errors ? 'CircleAlert' : 'ListChecks'}
              color={errors ? 'error' : 'secondary'}
            >
              {`Diagnostics: ${diagnosticCounts(review.diagnostics)}.`}
            </Fact>
          )}
        </div>
      )}
    </>
  )
}

/**
 * The code the owner reviews, in tabs: the source, the declaration, and the
 * diagnostics when there are some. A publish that replaces a revision shows
 * the source and the declaration as changes from it, or in full: one switch
 * at the end of the tabs sets both.
 */
function Code({ review }: { review: PublishReviewView }) {
  const { previous } = review
  const [view, setView] = useState<'changes' | 'full'>(previous ? 'changes' : 'full')
  const changes = previous !== null && view === 'changes'
  return (
    <Tabs.Root defaultValue="source" className={styles.stack}>
      <div className={styles.codeTabs}>
        <Tabs.List inset="none" className={styles.codeTabList}>
          <Tabs.Tab value="source">Source</Tabs.Tab>
          <Tabs.Tab value="declaration">Declaration</Tabs.Tab>
          {review.diagnostics.length > 0 && (
            <Tabs.Tab value="diagnostics">
              Diagnostics{' '}
              <Typography.BodySmall as="span" variant="secondary">
                {review.diagnostics.length.toLocaleString('en-US')}
              </Typography.BodySmall>
            </Tabs.Tab>
          )}
          <Tabs.Indicator />
        </Tabs.List>
        {previous && (
          <span className={styles.codeView}>
            <SegmentedControl
              label="View"
              size="small"
              value={view}
              onChange={setView}
              options={[
                { value: 'changes', label: `Changes from revision ${previous.revision}` },
                { value: 'full', label: 'Full' },
              ]}
            />
          </span>
        )}
      </div>
      <Tabs.Panel value="source" className={styles.stack}>
        {review.origin.kind === 'private' && (
          <Fact icon="TriangleAlert" color="warning">
            Every member can read this source. Remove tokens and IDs that only you should see.
          </Fact>
        )}
        {changes ? (
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
      </Tabs.Panel>
      <Tabs.Panel value="declaration">
        {changes ? (
          <DiffView
            label="Declaration changes"
            before={previous.declaration}
            after={review.declaration}
            maxLines={REVIEW_LINES}
            unchanged="The declaration does not change."
          />
        ) : (
          <CodeBlock value={review.declaration} format="typescript" framed />
        )}
      </Tabs.Panel>
      <Tabs.Panel value="diagnostics" className={styles.stack}>
        <Typography.BodySmall as="p" variant="secondary">
          These diagnostics do not stop the shared tool from running.
        </Typography.BodySmall>
        <Diagnostics
          diagnostics={review.diagnostics}
          truncated={review.diagnosticsTruncated}
          none=""
        />
      </Tabs.Panel>
    </Tabs.Root>
  )
}
