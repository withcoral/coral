import { useId } from 'react'
import { Link } from 'react-router'
import { Typography } from '@/wax/components'
import { Disclosure } from '@/wax/components/disclosure'
import { Elapsed } from '@/wax/components/elapsed'
import { CodeBlock as Native } from '@/wax/components/code-block'
import * as DescriptionList from '@/wax/components/description-list'
import type {
  ActivityPage,
  RunDetail,
  InvocationDetail,
  InteractionEvidence,
  InteractionRow,
  ProgramError,
  RunEvent,
  RunTerminal,
} from './types'
import { CancelRun } from '../actions/review'
import { RunActions } from '../actions/run-actions'
import { Arguments } from '../actions/arguments'
import { DiscoveryEvidence, SearchParameters } from './discovery/discovery'
import { DetailHeader, StateBadge } from './detail-header'
import { Instant } from './instant'
import { ValueCard } from './value-card'
import { Panel } from './panel'
import { useActivityConnected } from './live'
import { useActivityNavigation } from './navigation'
import * as styles from './task-activity.css'

const seconds = (milliseconds: number) => `${(milliseconds / 1000).toFixed(3)} s`

export function ActivityDetail({ page }: { page: ActivityPage }) {
  const detail = page.detail
  if (!detail) return null
  // An approval is always reviewed inside its Run; any other selected call leads with itself.
  if (detail.kind === 'invocation' && detail.request?.kind !== 'approval') {
    return <InvocationDetailView page={page} detail={detail} />
  }
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
  const { href, query, revealHref } = useActivityNavigation(page)
  const connected = useActivityConnected()
  const request = operation?.request ?? detail.request
  const running = detail.run.state === 'running'
  // An approval review names the call, shows this Run as its context, and carries the
  // Run's history link and cancel control in its own technical details.
  const review = request?.kind === 'approval'
  return (
    <Panel>
      {!review && (
        <>
          <DetailHeader
            title={detail.submission?.intent || detail.run.intent || 'No intent recorded'}
            status={detail.run.state}
            kind="program run"
          />
          <DescriptionList.List>
            <DescriptionList.Row label="Started">
              <Instant value={detail.run.startedAt} />
            </DescriptionList.Row>
            {detail.run.endedAt && (
              <DescriptionList.Row label="Ended">
                <Instant value={detail.run.endedAt} />
              </DescriptionList.Row>
            )}
            <DescriptionList.Row label="Duration">
              <Elapsed
                milliseconds={Number(detail.run.durationNanos) / 1_000_000}
                running={connected && running}
              >
                {seconds}
              </Elapsed>
            </DescriptionList.Row>
          </DescriptionList.List>
        </>
      )}
      <RunActions
        context={detail}
        runControls={
          review && (
            <>
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
              {running && <CancelRun name={detail.run.name} />}
            </>
          )
        }
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
      {review && !(detail.run.requiresAction && running) && (
        <div>
          <StateBadge entity="program run" state={detail.run.state} />
        </div>
      )}
      <TerminalEvidence terminal={detail.terminal} running={running} />
      {!review && running && (
        <div className={styles.endActions}>
          <CancelRun name={detail.run.name} variant="destructive" />
        </div>
      )}
    </Panel>
  )
}

/** The program a Run accepted, with the saved-function snapshots it linked. */
function AcceptedSource({ detail }: { detail: RunDetail }) {
  return (
    <>
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
    </>
  )
}

/** One exact call, leading with its own identity. Its Run, when retained, is context:
 * the Run's intent, evidence, source and whole-run controls, never the call's identity or outcome. */
function InvocationDetailView({ page, detail }: { page: ActivityPage; detail: InvocationDetail }) {
  const { recordHref, revealHref, href } = useActivityNavigation(page)
  const connected = useActivityConnected()
  const context = detail.context
  const call = detail.invocation
  const evidenceId = useId()
  const execIntent = context?.submission?.intent || context?.run.intent
  const cancel = context && context.run.state === 'running' && <CancelRun name={context.run.name} />
  return (
    <Panel>
      <DetailHeader
        title={detail.invocation.callPath || detail.invocation.operation}
        status={
          detail.invocation.state === 'ended' ? detail.invocation.outcome : detail.invocation.state
        }
        kind="operation"
        provider={detail.invocation.provider}
      />
      <DescriptionList.List>
        <DescriptionList.Row label="Started">
          <Instant value={call.startedAt} />
        </DescriptionList.Row>
        <DescriptionList.Row label="Duration">
          <Elapsed
            milliseconds={Number(call.durationNanos) / 1_000_000}
            running={connected && ['pending', 'dispatching'].includes(call.state)}
          >
            {seconds}
          </Elapsed>
        </DescriptionList.Row>
        {call.endedAt && (
          <DescriptionList.Row label="Ended">
            <Instant value={call.endedAt} />
          </DescriptionList.Row>
        )}
        <DescriptionList.Row label="Waiting time">
          <Elapsed
            milliseconds={Number(detail.invocation.waitingNanos) / 1_000_000}
            running={connected && detail.invocation.state === 'pending'}
          >
            {seconds}
          </Elapsed>
        </DescriptionList.Row>
        <DescriptionList.Row label="Dispatch">
          {detail.invocation.dispatchedAt ? (
            <time dateTime={detail.invocation.dispatchedAt}>{detail.invocation.dispatchedAt}</time>
          ) : (
            'Not started'
          )}
        </DescriptionList.Row>
      </DescriptionList.List>
      {detail.invocation.requiresAction && <Typography.Body>Requires your action.</Typography.Body>}
      {execIntent && (
        <DescriptionList.List title="Context">
          <DescriptionList.Row label="Exec intent">{execIntent}</DescriptionList.Row>
        </DescriptionList.List>
      )}
      <Arguments source={detail.arguments} />
      {detail.requestUnavailable && <Typography.Body>This request is unavailable.</Typography.Body>}
      {/* A shared Account Connection Request is Run-level: retained here, not actioned as this call's own Approval. */}
      {detail.request?.kind === 'account_connection' && (
        <>
          <DescriptionList.List title="Retained request">
            <DescriptionList.Row label="Request ID">{detail.request.name}</DescriptionList.Row>
            {detail.request.expiresAt && (
              <DescriptionList.Row label="Expires">{detail.request.expiresAt}</DescriptionList.Row>
            )}
          </DescriptionList.List>
          <div>
            <StateBadge entity="connection request" state={detail.request.state} />
          </div>
        </>
      )}
      <InvocationOutcome detail={detail} />
      {/* The Run's own outcome, apart from this call's: a Run can fail after a call succeeds. */}
      {context && (
        <section className={styles.stack} aria-labelledby={evidenceId}>
          <Typography.BodySmallStrong as="h3" id={evidenceId}>
            Run evidence
          </Typography.BodySmallStrong>
          <div>
            <StateBadge entity="program run" state={context.run.state} />
          </div>
          <TerminalEvidence terminal={context.terminal} running={context.run.state === 'running'} />
        </section>
      )}
      {!!detail.requests.length && (
        <Disclosure title="Retained requests" open>
          <Typography.BodySmall>
            These references do not indicate current action availability.
          </Typography.BodySmall>
          {detail.requests.map((request, index) =>
            request.kind === 'unknown' ? (
              <Typography.Body key={index}>This request reference is unavailable.</Typography.Body>
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
      <Disclosure title="Technical details">
        <DescriptionList.List>
          <DescriptionList.Row label="Invocation ID">{detail.invocation.name}</DescriptionList.Row>
        </DescriptionList.List>
        {context && (
          <>
            <DescriptionList.List title="Run">
              <DescriptionList.Row label="Run ID">{context.run.name}</DescriptionList.Row>
              <DescriptionList.Row label="Run started">
                <Instant value={context.run.startedAt} />
              </DescriptionList.Row>
              {context.run.endedAt && (
                <DescriptionList.Row label="Run ended">
                  <Instant value={context.run.endedAt} />
                </DescriptionList.Row>
              )}
              <DescriptionList.Row label="Run duration">
                <Elapsed
                  milliseconds={Number(context.run.durationNanos) / 1_000_000}
                  running={connected && context.run.state === 'running'}
                >
                  {seconds}
                </Elapsed>
              </DescriptionList.Row>
            </DescriptionList.List>
            {context.submission && (
              <Link to={recordHref(context.submission.name)} preventScrollReset>
                Inspect original exec submission
              </Link>
            )}
            <AcceptedSource detail={context} />
            <Typography.BodySmallStrong>Whole-run controls</Typography.BodySmallStrong>
            <Link
              to={revealHref({ ...detail.invocation, kind: 'invocation' }, { select: false })}
              preventScrollReset
            >
              View in history
            </Link>
            {cancel}
          </>
        )}
      </Disclosure>
    </Panel>
  )
}

/** Records without a Run context. An approval always has one, so none is reviewed here.
 * A selected operation invocation leads with itself, so none is reviewed here either. */
function RecordDetail({ page }: { page: ActivityPage }) {
  const { recordHref } = useActivityNavigation(page)
  const connected = useActivityConnected()
  const detail = page.detail
  const row = page.selectedRow
  if (!detail || detail.kind === 'run' || detail.kind === 'invocation') return null
  const { title, kind } =
    detail.kind === 'task'
      ? { title: detail.task.intent, kind: 'task' }
      : detail.kind === 'tool'
        ? toolHeading(detail.interaction, detail.evidence)
        : eventHeading(detail.event)
  const status =
    detail.kind === 'task'
      ? detail.task.outcome
      : detail.kind === 'tool'
        ? detail.interaction.outcome
        : detail.event.kind === 'program.logged'
          ? 'diagnostic'
          : 'retained'
  const label = detail.kind === 'tool' && status === 'unknown' ? 'unknown / incomplete' : undefined
  // A log or an emission states its time here; its title names it and a card holds its content.
  const payload = detail.kind === 'output' && hasPayload(detail.event) ? detail.event : null
  const started = row?.startedAt ?? payload?.occurredAt
  // A search is titled by its intent; it leads with what it asked and its times, then results.
  const search = detail.kind === 'tool' && detail.interaction.tool === 'search' ? detail : null
  const asked =
    search?.evidence.kind === 'discovery' && search.evidence.context.kind === 'search'
      ? search.evidence.context
      : null
  return (
    <Panel>
      <DetailHeader title={title} status={status} label={label} kind={kind} showStatus={!payload} />
      {/* A Task states its own facts in its Task list below. */}
      {started && detail.kind !== 'task' && !search && (
        <DescriptionList.List title={payload ? undefined : 'Context'}>
          <DescriptionList.Row label="Started">
            <Instant value={started} />
          </DescriptionList.Row>
          {row && 'durationNanos' in row && (
            <DescriptionList.Row label="Duration">
              <Elapsed
                milliseconds={Number(row.durationNanos) / 1_000_000}
                running={connected && ['running', 'pending', 'dispatching'].includes(row.state)}
              >
                {seconds}
              </Elapsed>
            </DescriptionList.Row>
          )}
          {row && 'endedAt' in row && row.endedAt && (
            <DescriptionList.Row label="Ended">
              <Instant value={row.endedAt} />
            </DescriptionList.Row>
          )}
        </DescriptionList.List>
      )}
      {detail.kind === 'task' && (
        <>
          <DescriptionList.List>
            <DescriptionList.Row label="Started">
              <Instant value={detail.task.startedAt} />
            </DescriptionList.Row>
            {detail.task.endedAt && (
              <DescriptionList.Row label="Ended">
                <Instant value={detail.task.endedAt} />
              </DescriptionList.Row>
            )}
            {detail.feedback !== undefined && (
              <DescriptionList.Row label="Feedback">
                {/* Keeps the agent's line breaks; the row owns the typography. */}
                <span className={styles.native}>{detail.feedback}</span>
              </DescriptionList.Row>
            )}
          </DescriptionList.List>
        </>
      )}
      {detail.kind === 'tool' && (
        <>
          {asked && <SearchParameters context={asked} />}
          {search ? (
            <DescriptionList.List>
              <DescriptionList.Row label="Started">
                <Instant value={detail.interaction.startedAt} />
              </DescriptionList.Row>
              {detail.interaction.completedAt && (
                <DescriptionList.Row label="Completed">
                  <Instant value={detail.interaction.completedAt} />
                </DescriptionList.Row>
              )}
            </DescriptionList.List>
          ) : (
            <DescriptionList.List title="Call">
              <DescriptionList.Row label="Intent">{detail.interaction.intent}</DescriptionList.Row>
              <DescriptionList.Row label="Submission outcome">
                {detail.interaction.outcome === 'unknown'
                  ? 'unknown / incomplete'
                  : detail.interaction.outcome}
              </DescriptionList.Row>
              {detail.interaction.completedAt && (
                <DescriptionList.Row label="Completed">
                  <Instant value={detail.interaction.completedAt} />
                </DescriptionList.Row>
              )}
            </DescriptionList.List>
          )}
          <Evidence evidence={detail.evidence} recordHref={recordHref} />
          {!search && (
            <Disclosure title="Technical details">
              <Typography.Code className={styles.native}>{page.selected}</Typography.Code>
            </Disclosure>
          )}
        </>
      )}
      {detail.kind === 'output' && (
        <>
          <EventEvidence event={detail.event} recordHref={recordHref} />
          {/* A log or an emission is its content; it keeps no technical details. */}
          {!payload && (
            <Disclosure title="Technical details">
              <Typography.Code className={styles.native}>{page.selected}</Typography.Code>
            </Disclosure>
          )}
        </>
      )}
    </Panel>
  )
}
const toolKinds: Record<string, string> = {
  search: 'search',
  describe: 'describe',
  exec: 'exec submission',
  save_function: 'save function',
  delete_function: 'delete function',
}
/** The most specific retained name for a tool call: a search's intent, then its query; what a
 * describe call described; otherwise its intent, then its target. The bare tool only when
 * nothing else is retained. */
function toolHeading(interaction: InteractionRow, evidence: InteractionEvidence) {
  const context = evidence.kind === 'discovery' ? evidence.context : null
  const reference =
    evidence.kind === 'run' || evidence.kind === 'function' ? evidence.reference : ''
  const name =
    context?.kind === 'search'
      ? interaction.intent || context.query
      : context?.kind === 'describe'
        ? context.callPath || interaction.intent
        : interaction.intent || reference
  return {
    title: name || interaction.tool,
    kind: toolKinds[interaction.tool] ?? 'tool interaction',
  }
}
/** A retained event's identity. A log message or an emitted value is the program's own
 * content, so it stays in the body and never becomes the title. */
function eventHeading(event: RunEvent): { title: string; kind: string } {
  switch (event.kind) {
    // The stored level names the console method; `log` is stored as `info`.
    case 'program.logged':
      return { title: `console.${event.level} #${event.sequence}`, kind: 'program log' }
    case 'program.emitted':
      return { title: `coral.emit #${event.sequence}`, kind: 'program output' }
    case 'run.started':
      return { title: 'Program run started', kind: 'run event' }
    case 'run.ended':
      return { title: `Program run ended: ${event.state.replaceAll('_', ' ')}`, kind: 'run event' }
    case 'run.cancellation_requested':
      return { title: 'Cancellation requested', kind: 'run event' }
    case 'invocation.started':
      return { title: 'Operation call started', kind: 'run event' }
    case 'invocation.ended':
      return {
        title: `Operation call ended: ${event.outcome.replaceAll('_', ' ')}`,
        kind: 'run event',
      }
    default:
      return { title: 'Unknown run event', kind: 'run event' }
  }
}
function InvocationOutcome({ detail }: { detail: InvocationDetail }) {
  if (detail.invocation.state !== 'ended') return null
  switch (detail.result.kind) {
    case 'upstream_outcome':
      return (
        <Disclosure title="Operation outcome" open>
          <Typography.BodySmall>
            HTTP status and MCP isError describe the upstream response, not the goal outcome.
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
type PayloadEvent = Extract<RunEvent, { kind: 'program.logged' | 'program.emitted' }>
const hasPayload = (event: RunEvent): event is PayloadEvent =>
  event.kind === 'program.logged' || event.kind === 'program.emitted'
function EventEvidence({
  event,
  recordHref,
}: {
  event: RunEvent
  recordHref: (name: string) => string
}) {
  if (event.kind === 'program.logged')
    return <ValueCard value={event.message} format="text" label="Log message" />
  if (event.kind === 'program.emitted')
    return <ValueCard value={event.value} format="json" label="Emitted value" />
  return (
    <>
      <Typography.BodySmall>
        {event.occurredAt} · record #{event.sequence}
      </Typography.BodySmall>
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
      // A program's return value, apart from anything it emitted while it ran.
      return terminal.finalValue === null ? (
        <Typography.Body>The program returned no final value.</Typography.Body>
      ) : (
        <ValueCard value={terminal.finalValue} format="json" label="Final value" />
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
