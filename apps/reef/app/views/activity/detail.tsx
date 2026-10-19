import { Link } from 'react-router'
import { Typography } from '@/wax/components'
import { Disclosure } from '@/wax/components/disclosure'
import { Elapsed } from '@/wax/components/elapsed'
import { CodeBlock as Native } from '@/wax/components/code-block'
import type {
  ActivityPage,
  RunDetail,
  InvocationDetail,
  InteractionEvidence,
  ProgramError,
  RunEvent,
  RunTerminal,
} from './types'
import { ApprovalTechnicalDetails, CancelRun } from '../actions/review'
import { RunActions } from '../actions/run-actions'
import { DiscoveryEvidence } from './discovery/discovery'
import { useActivityConnected } from './live'
import { useActivityNavigation } from './navigation'
import * as styles from './task-activity.css'

const seconds = (milliseconds: number) => `${(milliseconds / 1000).toFixed(3)} s`

export function ActivityDetail({ page }: { page: ActivityPage }) {
  const detail = page.detail
  if (!detail) return null
  const context =
    detail.kind === 'run' ? detail : detail.kind === 'invocation' ? detail.context : undefined
  if (context) {
    const operation = detail.kind === 'invocation' ? detail : null
    return <ExecutionDetail page={page} detail={context} operation={operation} />
  }
  return <RecordDetail page={page} />
}

function ExecutionDetail({
  page,
  detail,
  operation,
}: {
  page: ActivityPage
  detail: RunDetail
  operation: InvocationDetail | null
}) {
  const { recordHref, href, query, revealHref } = useActivityNavigation(page)
  const connected = useActivityConnected()
  const request = operation?.request ?? detail.request
  return (
    <>
      <Typography.HeadingSmall as="h2">
        {detail.submission?.intent || detail.run.intent || 'Program run'}
      </Typography.HeadingSmall>
      <Typography.BodySmall>
        {detail.run.actorLabel || (detail.run.agent ? 'Agent unavailable' : 'You')} ·{' '}
        {detail.run.workspaceLabel || 'Workspace unavailable'}
      </Typography.BodySmall>
      <RunActions
        page={detail.run.actionPage}
        request={request}
        unavailable={operation?.requestUnavailable ?? detail.requestUnavailable}
        href={(name) => href({ record: detail.run.name, request: name })}
        returnTo={href({})}
        firstHref={query.has('actionsAfter') ? href({ actionsAfter: null }) : undefined}
        nextHref={
          detail.run.actionPage?.next
            ? href({ actionsAfter: detail.run.actionPage.next })
            : undefined
        }
      />
      {operation && <InvocationOutcome detail={operation} />}
      {/* A Run that waits on you is running; its actions already say so. */}
      {!(detail.run.requiresAction && detail.run.state === 'running') && (
        <Typography.Body>Run state: {detail.run.state.replaceAll('_', ' ')}</Typography.Body>
      )}
      <TerminalEvidence terminal={detail.terminal} running={detail.run.state === 'running'} />
      <Link
        to={revealHref(
          operation
            ? { ...operation.invocation, kind: 'invocation' }
            : { ...detail.run, kind: 'run' },
          { select: false },
        )}
        preventScrollReset
      >
        View in history
      </Link>
      <Disclosure title="Technical details">
        {request?.kind === 'approval' && <ApprovalTechnicalDetails detail={request} />}
        {operation && (
          <Typography.BodySmall>Operation state: {operation.invocation.state}</Typography.BodySmall>
        )}
        {detail.submission && (
          <Link to={recordHref(detail.submission.name)} preventScrollReset>
            Inspect original exec submission
          </Link>
        )}
        {detail.run.task && (
          <Link to={recordHref(detail.run.task)} preventScrollReset>
            Open task
          </Link>
        )}
        <Disclosure title="Accepted program source">
          <Native value={detail.source} format="typescript" />
        </Disclosure>
        {detail.linkedFunctions.map((source) => (
          <Disclosure key={source.savedFunction} title={`Saved-function snapshot: ${source.path}`}>
            <Typography.BodySmall>
              Accepted for this execution. This is not the current saved function.
            </Typography.BodySmall>
            <Native value={source.source} format="typescript" />
          </Disclosure>
        ))}
        <Typography.Code className={styles.native}>{detail.run.name}</Typography.Code>
        <Typography.BodySmall>Started: {detail.run.startedAt}</Typography.BodySmall>
        {detail.run.endedAt && (
          <Typography.BodySmall>Ended: {detail.run.endedAt}</Typography.BodySmall>
        )}
        <Typography.BodySmall>
          Duration:{' '}
          <Elapsed
            milliseconds={Number(detail.run.durationNanos) / 1_000_000}
            running={connected && detail.run.state === 'running'}
          >
            {seconds}
          </Elapsed>
        </Typography.BodySmall>
        {detail.run.state === 'running' && <CancelRun name={detail.run.name} />}
      </Disclosure>
    </>
  )
}

/** Records without a Run context. An approval always has one, so none is reviewed here. */
function RecordDetail({ page }: { page: ActivityPage }) {
  const { recordHref, href } = useActivityNavigation(page)
  const connected = useActivityConnected()
  const detail = page.detail
  const row = page.selectedRow
  if (!detail || detail.kind === 'run') return null
  return (
    <>
      <Typography.HeadingSmall as="h2">
        {detail.kind === 'task'
          ? detail.task.intent
          : detail.kind === 'tool'
            ? detail.interaction.tool
            : detail.kind === 'invocation'
              ? detail.invocation.callPath || detail.invocation.operation
              : detail.kind === 'output'
                ? detail.event.kind
                : 'exec'}
      </Typography.HeadingSmall>
      {row && (
        <div className={styles.metadata}>
          <Typography.BodySmall>Started: {row.startedAt}</Typography.BodySmall>
          {'actor' in row && row.actor && (
            <Typography.BodySmall>Initiated by: {row.actor}</Typography.BodySmall>
          )}
          {'durationNanos' in row && (
            <Typography.BodySmall>
              Duration:{' '}
              <Elapsed
                milliseconds={Number(row.durationNanos) / 1_000_000}
                running={connected && ['running', 'pending', 'dispatching'].includes(row.state)}
              >
                {seconds}
              </Elapsed>
            </Typography.BodySmall>
          )}
          {'endedAt' in row && row.endedAt && (
            <Typography.BodySmall>Ended: {row.endedAt}</Typography.BodySmall>
          )}
        </div>
      )}
      <div className={styles.filters}>
        {row && 'task' in row && row.task && (
          <Link to={recordHref(row.task)} preventScrollReset>
            Open task
          </Link>
        )}
        {detail.kind === 'invocation' && (
          <Link to={recordHref(detail.invocation.run)} preventScrollReset>
            Open parent execution
          </Link>
        )}
      </div>
      <Disclosure title="Record identity">
        <Typography.Code className={styles.native}>{page.selected}</Typography.Code>
        {detail.kind === 'invocation' && (
          <Typography.BodySmall>provider: {detail.invocation.provider}</Typography.BodySmall>
        )}
      </Disclosure>
      {detail.kind === 'task' && (
        <>
          <Typography.Body>Goal outcome: {detail.task.outcome}</Typography.Body>
          {detail.feedback !== undefined && (
            <>
              <Typography.HeadingSmall as="h3">Completion feedback</Typography.HeadingSmall>
              <Typography.Body className={styles.native}>{detail.feedback}</Typography.Body>
            </>
          )}
          {detail.task.endedAt && (
            <Typography.Body>
              This task accepts no new tool calls. Existing executions can continue.
            </Typography.Body>
          )}
        </>
      )}
      {detail.kind === 'tool' && (
        <>
          <Typography.Body>Intent: {detail.interaction.intent}</Typography.Body>
          <Typography.Body>
            Submission outcome:{' '}
            {detail.interaction.outcome === 'unknown'
              ? 'unknown / incomplete'
              : detail.interaction.outcome}
          </Typography.Body>
          {detail.interaction.completedAt && (
            <Typography.BodySmall>Completed: {detail.interaction.completedAt}</Typography.BodySmall>
          )}
          <Evidence evidence={detail.evidence} recordHref={recordHref} />
        </>
      )}
      {detail.kind === 'invocation' && (
        <>
          <Typography.Body>State: {detail.invocation.state}</Typography.Body>
          <Typography.BodySmall>
            Waiting time:{' '}
            <Elapsed
              milliseconds={Number(detail.invocation.waitingNanos) / 1_000_000}
              running={connected && detail.invocation.state === 'pending'}
            >
              {seconds}
            </Elapsed>
          </Typography.BodySmall>
          <Typography.BodySmall>
            {detail.invocation.dispatchedAt
              ? `Dispatch started: ${detail.invocation.dispatchedAt}`
              : 'Dispatch has not started.'}
          </Typography.BodySmall>
          {detail.invocation.requiresAction && (
            <Typography.Body>Requires your action.</Typography.Body>
          )}
          <Disclosure title="Exact logical arguments" open>
            <Native value={detail.arguments} format="json" />
          </Disclosure>
          {detail.requestUnavailable && (
            <Typography.Body>This request is unavailable.</Typography.Body>
          )}
          {detail.request?.kind === 'account_connection' && (
            <Disclosure title="Retained request detail" open>
              <Typography.Body>State: {detail.request.state}</Typography.Body>
              <Typography.Code className={styles.native}>{detail.request.name}</Typography.Code>
              {detail.request.expiresAt && (
                <Typography.BodySmall>Expires: {detail.request.expiresAt}</Typography.BodySmall>
              )}
            </Disclosure>
          )}
          <InvocationOutcome detail={detail} />
          {!!detail.requests.length && (
            <Disclosure title="Retained requests" open>
              <Typography.BodySmall>
                These references do not indicate current action availability.
              </Typography.BodySmall>
              {detail.requests.map((request, index) =>
                request.kind === 'unknown' ? (
                  <Typography.Body key={index}>
                    This request reference is unavailable.
                  </Typography.Body>
                ) : (
                  <Typography.Body as="p" key={request.name}>
                    <Link to={href({ request: request.name })} preventScrollReset>
                      Inspect {request.kind === 'approval' ? 'approval' : 'account connection'}
                    </Link>
                  </Typography.Body>
                ),
              )}
            </Disclosure>
          )}
        </>
      )}
      {detail.kind === 'output' && (
        <>
          <Link to={recordHref(detail.run)} preventScrollReset>
            Open parent execution
          </Link>
          <EventEvidence event={detail.event} recordHref={recordHref} />
        </>
      )}
    </>
  )
}
function InvocationOutcome({ detail }: { detail: InvocationDetail }) {
  if (detail.invocation.state !== 'ended') return null
  switch (detail.result.kind) {
    case 'upstream_outcome':
      return (
        <Disclosure title="Operation outcome" open>
          <Typography.BodySmall>
            Your program receives only a declared success. It receives any other response as an
            operation error.
          </Typography.BodySmall>
          <Native value={detail.result.value} format="json" />
        </Disclosure>
      )
    case 'coral_failure':
      return (
        <>
          <Typography.Body>Coral failure: {detail.result.code}</Typography.Body>
          <Native value={detail.result.message} />
        </>
      )
    case 'indeterminate':
      return (
        <Typography.Body>
          Dispatch started. Coral did not retain a complete result. External effects may be
          uncertain.
        </Typography.Body>
      )
    case 'run_terminated':
      return <Typography.Body>The run terminated this call before dispatch.</Typography.Body>
    default:
      return <Typography.Body>No retained outcome is available.</Typography.Body>
  }
}

function Evidence({
  evidence,
  recordHref,
}: {
  evidence: InteractionEvidence
  recordHref: (name: string) => string
}) {
  switch (evidence.kind) {
    case 'discovery':
      return <DiscoveryEvidence evidence={evidence} />
    case 'rejection':
      return (
        <>
          <Typography.Body>Rejected: {evidence.code}</Typography.Body>
          <Native value={evidence.message} />
        </>
      )
    case 'run':
      return (
        <>
          <Typography.Body>Run accepted.</Typography.Body>
          <Link to={recordHref(evidence.reference)}>Open execution</Link>
        </>
      )
    case 'unknown':
      return (
        <Typography.Body>
          Completion is unknown or incomplete. External effects may be uncertain.
        </Typography.Body>
      )
    default:
      return <Native value={evidence.reference} />
  }
}
function EventEvidence({
  event,
  recordHref,
}: {
  event: RunEvent
  recordHref: (name: string) => string
}) {
  return (
    <>
      <Typography.BodySmall>
        {event.occurredAt} · record #{event.sequence}
      </Typography.BodySmall>
      {event.kind === 'program.emitted' && <Native value={event.value} format="json" />}
      {event.kind === 'program.logged' && (
        <>
          <Typography.Body>Diagnostic log · {event.level}</Typography.Body>
          <Typography.BodySmall>This log is not a user-facing result.</Typography.BodySmall>
          <Native value={event.message} />
        </>
      )}
      {event.kind === 'run.ended' && <Typography.Body>Run state: {event.state}</Typography.Body>}
      {event.kind === 'run.cancellation_requested' && (
        <Typography.Body>
          Requested by: {event.actorKind} {event.actor}
        </Typography.Body>
      )}
      {(event.kind === 'invocation.started' || event.kind === 'invocation.ended') && (
        <Link to={recordHref(event.invocation)}>Open operation call</Link>
      )}
      {event.kind === 'invocation.ended' && (
        <Typography.Body>{event.outcome.replaceAll('_', ' ')}</Typography.Body>
      )}
      {event.kind === 'unknown' && (
        <Typography.Body>This event detail is unavailable.</Typography.Body>
      )}
    </>
  )
}
function TerminalEvidence({ terminal, running }: { terminal: RunTerminal; running: boolean }) {
  switch (terminal.kind) {
    case 'succeeded':
      return (
        <Disclosure title="Final value" open>
          {terminal.finalValue === null ? (
            <Typography.Body>The program returned no final value.</Typography.Body>
          ) : (
            <Native value={terminal.finalValue} format="json" />
          )}
        </Disclosure>
      )
    case 'failed':
      return (
        <Disclosure title="Failure details" open>
          <Typography.Body>{terminal.code}</Typography.Body>
          <Native value={terminal.message} />
          {terminal.error && <ErrorDetail error={terminal.error} />}
        </Disclosure>
      )
    case 'unavailable':
      return running ? null : <Typography.Body>Terminal evidence is unavailable.</Typography.Body>
    default:
      return (
        <Typography.Body>Terminal outcome: {terminal.kind.replaceAll('_', ' ')}</Typography.Body>
      )
  }
}
function ErrorDetail({ error }: { error: ProgramError }) {
  return (
    <div className={styles.stack}>
      <Typography.BodySmallStrong>Program error: {error.name}</Typography.BodySmallStrong>
      <Native value={error.message} />
      {error.frames.map((frame, index) => (
        <Typography.Code key={index} className={styles.native}>
          {frame.source}:{frame.line}:{frame.column}
        </Typography.Code>
      ))}
      {error.operationCode && (
        <Typography.Body>
          Coral operation code: {error.operationCode} · Trace: {error.operationTraceId}
        </Typography.Body>
      )}
      {error.operationStatus > 0 && (
        <Typography.Body>Upstream HTTP status: {error.operationStatus}</Typography.Body>
      )}
      {error.operationBody && (
        <>
          <Typography.BodySmallStrong>Upstream response body</Typography.BodySmallStrong>
          <Native value={error.operationBody} format="json" />
          {error.operationBodyTruncated && (
            <Typography.Body>This is the start of the body.</Typography.Body>
          )}
        </>
      )}
      {error.truncated && (
        <Typography.Body>Coral retained bounded diagnostic content.</Typography.Body>
      )}
      {error.circular && <Typography.Body>This error references an earlier error.</Typography.Body>}
      {error.causeOmitted && <Typography.Body>The cause is omitted.</Typography.Body>}
      {error.omittedErrors > 0 && (
        <Typography.Body>Omitted errors: {error.omittedErrors}</Typography.Body>
      )}
      {error.cause && (
        <Disclosure title="Cause" open>
          <ErrorDetail error={error.cause} />
        </Disclosure>
      )}
      {error.errors.map((member, index) => (
        <Disclosure key={index} title={`Grouped error ${index + 1}`}>
          <ErrorDetail error={member} />
        </Disclosure>
      ))}
    </div>
  )
}
