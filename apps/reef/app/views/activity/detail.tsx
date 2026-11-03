import { useId } from 'react'
import { duration } from './duration'
import { Link } from 'react-router'
import { List, Typography } from '@/wax/components'
import { CodeChip } from '@/wax/components/code-chip'
import { Disclosure } from '@/wax/components/disclosure'
import { Elapsed } from '@/wax/components/elapsed'
import { CodeBlock as Native } from '@/wax/components/code-block'
import * as DescriptionList from '@/wax/components/description-list'
import type {
  ActivityPage,
  RunDetail,
  InvocationDetail,
  InvocationResult,
  InteractionEvidence,
  InteractionRow,
  ProgramError,
  RunEvent,
  RunTerminal,
  RunFailureCode,
} from './types'
import { CancelRun } from '../actions/review'
import { RunActions } from '../actions/run-actions'
import { Arguments } from '../actions/arguments'
import { DescribeParameters, DiscoveryEvidence, SearchParameters } from './discovery/discovery'
import { DetailHeader, StateBadge } from './detail-header'
import { ContextImpact } from './context-impact'
import { Instant } from './instant'
import { PayloadViewer, type PayloadViewerProps } from '@/wax/components/payload-viewer'
import { invocationOutcomeLabels, requestedBy, resultUnknown } from './labels'
import { Panel } from './panel'
import { useActivityConnected, useActivityInspect } from './live'
import { useActivityNavigation } from './navigation'
import * as styles from './task-activity.css'

/** An elapsed time in whole seconds, as the Activity table shows it. */
const seconds = (milliseconds: number) => duration(milliseconds * 1_000_000)

/** The Run whose accepted source a selected record shows beside its own detail, if any.
 * A tool or an output carries it as context; it never replaces the record's detail. */
export function runContextOf(detail: ActivityPage['detail']): RunDetail | undefined {
  if (detail?.kind === 'run') return detail
  if (detail?.kind === 'invocation' || detail?.kind === 'output') return detail.context
  return undefined
}

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
    const tool = detail.kind === 'invocation' ? detail : null
    return <ExecutionDetail page={page} detail={context} tool={tool} />
  }
  return <RecordDetail page={page} />
}

function ExecutionDetail({
  page,
  detail,
  tool,
}: {
  page: ActivityPage
  detail: RunDetail
  tool: InvocationDetail | null
}) {
  const { href } = useActivityNavigation(page)
  const connected = useActivityConnected()
  const request = tool?.request ?? detail.request
  const running = detail.run.state === 'running'
  // An approval review names the call and decides only it: no program, history or Run cancel.
  const review = request?.kind === 'approval'
  // A selected connection request leads the panel as an approval does; the Run is its context.
  const connection = request?.kind === 'account_connection'
  const run = detail.run
  const runContext = connection && (
    <DescriptionList.List>
      {(detail.submission?.intent || run.intent) && (
        <DescriptionList.Row label="Run intent">
          {detail.submission?.intent || run.intent}
        </DescriptionList.Row>
      )}
      <DescriptionList.Row label="Requested by">{requestedBy(run)}</DescriptionList.Row>
      {/* A connection request records no start of its own; these times are the Run's. */}
      <DescriptionList.Row label="Run duration">
        <Elapsed
          milliseconds={Number(run.durationNanos) / 1_000_000}
          running={connected && running}
        >
          {seconds}
        </Elapsed>
      </DescriptionList.Row>
      <DescriptionList.Row label="Run started">
        <Instant value={run.startedAt} />
      </DescriptionList.Row>
      {run.endedAt && (
        <DescriptionList.Row label="Run ended">
          <Instant value={run.endedAt} />
        </DescriptionList.Row>
      )}
    </DescriptionList.List>
  )
  return (
    <Panel label={request?.kind === 'approval' ? `Review ${request.callPath}` : undefined}>
      {!review && !connection && (
        <>
          <DetailHeader
            title={detail.submission?.intent || detail.run.intent || 'Program run'}
            status={detail.run.state}
            kind="program run"
          />
          {/* Its times together, last. */}
          <DescriptionList.List>
            <DescriptionList.Row label="Duration">
              <Elapsed
                milliseconds={Number(detail.run.durationNanos) / 1_000_000}
                running={connected && running}
              >
                {seconds}
              </Elapsed>
            </DescriptionList.Row>
            <DescriptionList.Row label="Started">
              <Instant value={detail.run.startedAt} />
            </DescriptionList.Row>
            {detail.run.endedAt && (
              <DescriptionList.Row label="Ended">
                <Instant value={detail.run.endedAt} />
              </DescriptionList.Row>
            )}
          </DescriptionList.List>
        </>
      )}
      <RunActions
        context={detail}
        callStartedAt={tool?.invocation.startedAt}
        connectionContext={runContext}
        page={detail.run.actionPage}
        request={request}
        unavailable={tool?.requestUnavailable ?? detail.requestUnavailable}
        href={(name) => href({ record: detail.run.name, request: name })}
        pageHref={(cursor) =>
          href({ record: detail.run.name, request: null, actionsAfter: cursor })
        }
        returnTo={href({})}
      />
      {tool && <InvocationOutcome detail={tool} />}
      {/* An approval states its own status; a connection request shows its Run's state, unless
          the Run waits on you, which its actions already say. */}
      {connection && !(detail.run.requiresAction && running) && (
        <div>
          <StateBadge entity="program run" state={detail.run.state} />
        </div>
      )}
      {/* The Run's final value or failure is not a selected request's outcome: the Run's own
          panel holds it. */}
      {!review && !connection && <TerminalEvidence terminal={detail.terminal} />}
      {/* A selected request decides itself; the Run's cancel stays on the Run's own panel. */}
      {!review && !connection && running && (
        <div className={styles.endActions}>
          <CancelRun name={detail.run.name} variant="destructive" />
        </div>
      )}
    </Panel>
  )
}

/** One exact call, leading with its own identity and ending with its own outcome. Its Run, when
 * retained, adds only its intent. The Run's state, outcome, source, raw IDs, times and controls
 * stay out: the Run's panel holds them, and a Run's lifecycle is not this call's. */
function InvocationDetailView({ page, detail }: { page: ActivityPage; detail: InvocationDetail }) {
  const { href } = useActivityNavigation(page)
  const connected = useActivityConnected()
  const context = detail.context
  const call = detail.invocation
  const runIntent = context?.submission?.intent || context?.run.intent
  // A connection request belongs to the call's Run, which is where you connect or decline.
  const connectionHref = (name: string) => href({ record: call.run, request: name })
  const expiresId = useId()
  // The selected connection's card links to it, so the list does not repeat it.
  const selected = detail.request
  const otherRequests = detail.requests.filter(
    (request) =>
      !(
        selected?.kind === 'account_connection' &&
        request.kind === 'account_connection' &&
        request.name === selected.name
      ),
  )
  return (
    <Panel>
      <DetailHeader
        title={detail.invocation.callPath || detail.invocation.tool}
        status={
          detail.invocation.state !== 'ended'
            ? detail.invocation.state
            : detail.invocation.outcome === 'upstream_outcome'
              ? answeredStatus(detail.result)
              : detail.invocation.outcome
        }
        label={
          detail.invocation.state !== 'ended'
            ? undefined
            : detail.invocation.outcome === 'upstream_outcome'
              ? answeredStatus(detail.result)
              : invocationOutcomeLabels[detail.invocation.outcome]
        }
        kind="tool"
      />
      {/* What the call belongs to and how it waited, then its times together, last. */}
      <DescriptionList.List>
        {runIntent && <DescriptionList.Row label="Run intent">{runIntent}</DescriptionList.Row>}
        <DescriptionList.Row label="Dispatch">
          {detail.invocation.dispatchedAt ? (
            <Instant value={detail.invocation.dispatchedAt} />
          ) : (
            'Not started'
          )}
        </DescriptionList.Row>
        <DescriptionList.Row label="Waiting time">
          <Elapsed
            milliseconds={Number(detail.invocation.waitingNanos) / 1_000_000}
            running={connected && detail.invocation.state === 'pending'}
          >
            {seconds}
          </Elapsed>
        </DescriptionList.Row>
        <DescriptionList.Row label="Duration">
          <Elapsed
            milliseconds={Number(call.durationNanos) / 1_000_000}
            running={connected && ['pending', 'dispatching'].includes(call.state)}
          >
            {seconds}
          </Elapsed>
        </DescriptionList.Row>
        <DescriptionList.Row label="Started">
          <Instant value={call.startedAt} />
        </DescriptionList.Row>
        {call.endedAt && (
          <DescriptionList.Row label="Ended">
            <Instant value={call.endedAt} />
          </DescriptionList.Row>
        )}
      </DescriptionList.List>
      {detail.invocation.requiresAction && <Typography.Body>Requires your action.</Typography.Body>}
      <Arguments source={detail.arguments} />
      {detail.requestUnavailable && <Typography.Body>This request is unavailable.</Typography.Body>}
      {/* A shared Account Connection Request is Run-level: retained here, not actioned as this call's own Approval. */}
      {detail.request?.kind === 'account_connection' && (
        <List.Container as="section" aria-label="Account connection">
          {/* The whole card opens the request on its Run, where Connect and Decline are. */}
          <List.Item
            as={Link}
            to={connectionHref(detail.request.name)}
            preventScrollReset
            interactive
            className={styles.cardLink}
            aria-label={`Open account connection request: ${detail.request.state.replaceAll('_', ' ')}`}
            aria-describedby={detail.request.expiresAt ? expiresId : undefined}
          >
            <StateBadge entity="connection request" state={detail.request.state} />
            {detail.request.expiresAt && (
              <Typography.BodySmall variant="tertiary" id={expiresId}>
                Expires <Instant value={detail.request.expiresAt} />
              </Typography.BodySmall>
            )}
          </List.Item>
        </List.Container>
      )}
      <InvocationOutcome detail={detail} />
      {!!otherRequests.length && (
        <List.Container as="ul" aria-label="Requests for this call">
          {otherRequests.map((request, index) => (
            <List.Item as="li" key={request.kind === 'unknown' ? index : request.name}>
              {request.kind === 'unknown' ? (
                <Typography.Body>This request reference is unavailable.</Typography.Body>
              ) : (
                <Link
                  to={
                    request.kind === 'approval'
                      ? href({ request: request.name })
                      : connectionHref(request.name)
                  }
                  preventScrollReset
                >
                  Inspect {request.kind === 'approval' ? 'approval' : 'account connection'}
                </Link>
              )}
            </List.Item>
          ))}
        </List.Container>
      )}
    </Panel>
  )
}

/** Records without a Run context. An approval always has one, so none is reviewed here.
 * A selected tool invocation leads with itself, so none is reviewed here either. */
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
        : // An output's title and kind name it; a status would say nothing more.
          null
  const label = detail.kind === 'tool' && status === 'unknown' ? resultUnknown : undefined
  // A run event states its own time when no row does.
  const started = row?.startedAt ?? (detail.kind === 'output' ? detail.event.occurredAt : undefined)
  // A search is titled by its intent; it leads with what it asked and its times, then results.
  const search = detail.kind === 'tool' && detail.interaction.tool === 'search' ? detail : null
  const asked =
    search?.evidence.kind === 'discovery' && search.evidence.context.kind === 'search'
      ? search.evidence.context
      : null
  // A describe reads like a search: what it asked, its times, a compact Result, then Details.
  const describe = detail.kind === 'tool' && detail.interaction.tool === 'describe' ? detail : null
  const described =
    describe?.evidence.kind === 'discovery' && describe.evidence.context.kind === 'describe'
      ? {
          context: describe.evidence.context,
          description:
            describe.evidence.view.kind === 'describe' ? describe.evidence.view.description : null,
        }
      : null
  // An exec submission states its times and intent, then any error; its badge states its outcome.
  const exec = detail.kind === 'tool' && detail.interaction.tool === 'exec' ? detail : null
  // A save or a delete states its intent, the function a delete removed, then its times; a
  // rejection shows its error below.
  const save =
    detail.kind === 'tool' &&
    (detail.interaction.tool === 'save_function' || detail.interaction.tool === 'delete_function')
      ? detail
      : null
  const deleted = save?.interaction.tool === 'delete_function'
  // A tool call's intent, unless the title already states it.
  const intent = detail.kind === 'tool' &&
    detail.interaction.intent &&
    detail.interaction.intent !== title && (
      <DescriptionList.Row label="Intent">{detail.interaction.intent}</DescriptionList.Row>
    )
  // The call's own times, closing the one list a search, describe, exec or save shows.
  const times = detail.kind === 'tool' && (
    <>
      <DescriptionList.Row label="Started">
        <Instant value={detail.interaction.startedAt} />
      </DescriptionList.Row>
      {detail.interaction.completedAt && (
        <DescriptionList.Row label="Completed">
          <Instant value={detail.interaction.completedAt} />
        </DescriptionList.Row>
      )}
    </>
  )
  return (
    <Panel>
      <DetailHeader
        title={title}
        status={status ?? 'output'}
        label={label}
        kind={kind}
        showStatus={status !== null}
      />
      {/* A Task and a tool call state their own facts in their lists below. */}
      {started && detail.kind === 'output' && (
        <DescriptionList.List>
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
            {/* The Task's stored total, as its table row shows it: — when unavailable. */}
            <DescriptionList.Row label="Context">
              <ContextImpact
                bytes={detail.task.contextImpactBytes}
                partial={detail.task.contextImpactPartial}
              />
            </DescriptionList.Row>
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
          {/* What a describe asked and its times read as one list, with no gap between them. */}
          {described ? (
            <DescribeParameters
              context={described.context}
              description={described.description}
              intent={detail.interaction.intent}
            >
              {times}
            </DescribeParameters>
          ) : search || describe ? (
            <DescriptionList.List>{times}</DescriptionList.List>
          ) : save ? (
            <DescriptionList.List>
              {intent}
              {/* The call retains only the function's ID. No route opens a function by ID, so
                  a delete shows the ID without a link, and a save states no target. */}
              {deleted && save.evidence.kind === 'function' && (
                <DescriptionList.Row label="Deleted">
                  <CodeChip>{savedFunctionId(save.evidence.reference)}</CodeChip>
                </DescriptionList.Row>
              )}
              {times}
            </DescriptionList.List>
          ) : (
            // The header badge states the outcome.
            <DescriptionList.List>
              {intent}
              {times}
            </DescriptionList.List>
          )}
          {!search && !describe ? (
            <CallEvidence
              evidence={detail.evidence}
              outcome={detail.interaction.outcome}
              // A save says nothing beyond its badge when it kept no message or evidence.
              unavailable={
                deleted
                  ? 'The delete evidence is unavailable.'
                  : save
                    ? null
                    : exec
                      ? 'The submission evidence is unavailable.'
                      : 'The call evidence is unavailable.'
              }
              noMessage={!save || deleted}
              recordHref={recordHref}
            />
          ) : search && detail.evidence.kind === 'rejection' && detail.evidence.message.trim() ? (
            // The badge states the rejection; the card holds its exact error.
            <ErrorCard message={detail.evidence.message} />
          ) : (
            <Evidence
              evidence={detail.evidence}
              recordHref={recordHref}
              record={page.selected}
              describe={describe !== null}
            />
          )}
        </>
      )}
      {detail.kind === 'output' && <EventEvidence event={detail.event} recordHref={recordHref} />}
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
 * describe call described; otherwise its intent, then the Run it started. The tool only when
 * nothing else is retained. */
function toolHeading(interaction: InteractionRow, evidence: InteractionEvidence) {
  const context = evidence.kind === 'discovery' ? evidence.context : null
  const name =
    context?.kind === 'search'
      ? interaction.intent || context.query
      : context?.kind === 'describe'
        ? context.callPath || interaction.intent
        : interaction.intent || (evidence.kind === 'run' ? evidence.reference : '')
  const kind = toolKinds[interaction.tool] ?? 'tool interaction'
  // A save or a delete retains only a function ID that no page opens, so its tool titles it.
  const functionTool =
    interaction.tool === 'save_function' || interaction.tool === 'delete_function'
  return {
    title: name || (functionTool ? kind[0]!.toUpperCase() + kind.slice(1) : interaction.tool),
    kind,
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
      return { title: 'Tool call started', kind: 'run event' }
    case 'invocation.ended':
      return {
        // The event holds no response to tell, so a complete one reads only as completed.
        title: `Tool call ended: ${invocationOutcomeLabels[event.outcome]}`,
        kind: 'run event',
      }
    default:
      return { title: 'Unknown run event', kind: 'run event' }
  }
}
/**
 * How a call that got a complete response reads. The response proves a failure only through
 * HTTP: Coral counts any status outside 2xx as the provider's error. Nothing in it proves
 * success: Coral classes a 2xx against the statuses the Tool declares, which Reef does not
 * receive, and an MCP `isError` counts only for a tool call, which the response does not record.
 * So the rest reads as completed: a response came back. A property in the provider's own data
 * never decides it.
 */
function answeredStatus(result: InvocationResult): 'failed' | 'completed' {
  if (result.kind !== 'upstream_outcome') return 'completed'
  let response: unknown
  try {
    response = JSON.parse(result.value)
  } catch {
    return 'completed'
  }
  const http = response as { status?: unknown; metadata?: { kind?: unknown } } | null
  const status = http?.metadata?.kind === 'http' ? http.status : undefined
  return Number.isInteger(status) && ((status as number) < 200 || (status as number) > 299)
    ? 'failed'
    : 'completed'
}

function InvocationOutcome({ detail }: { detail: InvocationDetail }) {
  if (detail.invocation.state !== 'ended') return null
  switch (detail.result.kind) {
    case 'upstream_outcome':
      // What the provider answered, which says nothing of whether the goal succeeded.
      return <Payload value={detail.result.value} format="json" label="Provider response" />
    case 'coral_failure':
      return (
        <ErrorCard
          message={detail.result.message}
          reason={`Coral failure: ${detail.result.code}`}
        />
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
      return null
  }
}

function Evidence({
  evidence,
  recordHref,
  record,
  describe,
}: {
  evidence: InteractionEvidence
  recordHref: (name: string) => string
  record?: string | null
  describe?: boolean
}) {
  switch (evidence.kind) {
    case 'discovery':
      return <DiscoveryEvidence evidence={evidence} record={record} describe={describe} />
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
/** What an exec submission, a save or a delete retained. A rejection shows its exact error in the card
 * a search result uses. An accepted submission shows nothing more: its Run is a row of its
 * own, and the submission's outcome says nothing about how the Run ends. A delete states the
 * function it removed in its list; a save states no target. */
function CallEvidence({
  evidence,
  outcome,
  unavailable,
  noMessage,
  recordHref,
}: {
  evidence: InteractionEvidence
  outcome: string
  /** What a known outcome without its evidence says; null says nothing. */
  unavailable: string | null
  /** Whether a rejection without a message says so, with its code. */
  noMessage: boolean
  recordHref: (name: string) => string
}) {
  switch (evidence.kind) {
    case 'run':
      return null
    case 'function':
      return null
    case 'rejection':
      return evidence.message.trim() ? (
        <ErrorCard message={evidence.message} />
      ) : noMessage ? (
        <Typography.Body>
          Rejected with no message{evidence.code ? `: ${evidence.code}` : '.'}
        </Typography.Body>
      ) : null
    case 'unknown':
      // An unknown outcome already shows in the badge and its icon.
      return outcome === 'unknown' || unavailable === null ? null : (
        <Typography.Body>{unavailable}</Typography.Body>
      )
    default:
      return <Evidence evidence={evidence} recordHref={recordHref} />
  }
}
/** An error's exact message, whitespace and all, in the card a search result uses. `reason`
 * names its kind when the badge does not; `children` adds what else it retained. */
function ErrorCard({
  message,
  reason,
  code,
  children,
}: {
  message: string
  reason?: string
  /** Coral's exact code for the failure, a diagnostic under the plain heading. */
  code?: string
  children?: React.ReactNode
}) {
  return (
    <List.Container as="section" aria-label="Error">
      <List.Item>
        {reason && <Typography.BodySmallStrong>{reason}</Typography.BodySmallStrong>}
        <Native value={message} />
        {children}
        {code && (
          <Typography.BodySmall variant="tertiary">
            Code: <Typography.CodeSmallInline>{code}</Typography.CodeSmallInline>
          </Typography.BodySmall>
        )}
      </List.Item>
    </List.Container>
  )
}
/** The ID in `workspaces/{workspace}/savedFunctions/{id}`, else the name as stored. */
const savedFunctionId = (name: string) =>
  /^workspaces\/[^/]+\/savedFunctions\/([^/]+)$/.exec(name)?.[1] ?? name
function EventEvidence({
  event,
  recordHref,
}: {
  event: RunEvent
  recordHref: (name: string) => string
}) {
  if (event.kind === 'program.logged')
    return <Payload value={event.message} format="text" label="Log message" />
  if (event.kind === 'program.emitted')
    return <Payload value={event.value} format="json" label="Emitted value" />
  // Its time is in the list above.
  return (
    <>
      {event.kind === 'run.cancellation_requested' && (
        <Typography.Body>
          Requested by: {event.actorKind} {event.actor}
        </Typography.Body>
      )}
      {(event.kind === 'invocation.started' || event.kind === 'invocation.ended') && (
        <Link to={recordHref(event.invocation)}>Open tool call</Link>
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
/** What a Run's end retained. A returned value, JSON null included, shows in its card; a Run
 * that returned nothing shows none. A failure shows its reason, message and program error in
 * the error card, never folded away. The badge already states a cancel, a time-out or an
 * interruption, and a missing record adds nothing to it. */
function TerminalEvidence({ terminal }: { terminal: RunTerminal }) {
  switch (terminal.kind) {
    case 'succeeded':
      // A program's return value, apart from anything it emitted while it ran.
      return terminal.finalValue === null ? null : (
        <Payload value={terminal.finalValue} format="json" label="Final value" />
      )
    case 'failed':
      return (
        <ErrorCard
          message={terminal.message}
          reason={runFailureHeadings[terminal.code]}
          // An unknown code is this Reef's own word, not Coral's, so it is no diagnostic.
          code={terminal.code === 'unknown' ? undefined : terminal.code}
        >
          {terminal.error && <ErrorDetail error={terminal.error} />}
        </ErrorCard>
      )
    default:
      return null
  }
}
/** A Run failure in plain words, from the reason Coral set; the exact code shows under it. */
const runFailureHeadings = {
  program_rejected: 'The program threw an error',
  invalid_program_value: 'The program returned a value Coral cannot store',
  resource_limit_exceeded: 'A value or the output was too large',
  memory_limit_exceeded: 'The program ran out of memory',
  program_execution_failed: 'The program failed',
  run_authority_lost: 'The run lost the access it needed',
  unknown: 'The run failed',
} satisfies Record<RunFailureCode, string>
/**
 * One Program error, its cause and its grouped errors. `place` says where it sits, such as
 * "cause of ToolError", so the upstream data of each error in a chain has its own name.
 */
function ErrorDetail({ error, place }: { error: ProgramError; place?: string }) {
  const named = [error.name, place].filter(Boolean).join(', ')
  // What a cause or a grouped error says it belongs to: an error with no name is "the error".
  const parent = named || 'the error'
  return (
    <div className={styles.stack}>
      <Typography.BodySmallStrong>Program error: {error.name}</Typography.BodySmallStrong>
      <Native value={error.message} />
      {error.frames.map((frame, index) => (
        <Typography.Code key={index} className={styles.native}>
          {frame.source}:{frame.line}:{frame.column}
        </Typography.Code>
      ))}
      {error.toolCode && (
        <Typography.Body>
          Coral tool code: {error.toolCode} · Trace: {error.toolTraceId}
        </Typography.Body>
      )}
      {error.toolStatus > 0 && (
        <Typography.Body>Upstream HTTP status: {error.toolStatus}</Typography.Body>
      )}
      {error.toolBody && (
        <>
          <Typography.BodySmallStrong>Upstream response data</Typography.BodySmallStrong>
          <Payload
            value={error.toolBody}
            format="json"
            label={named ? `Upstream response data for ${named}` : 'Upstream response data'}
            truncated={error.toolBodyTruncated}
          />
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
          <ErrorDetail error={error.cause} place={`cause of ${parent}`} />
        </Disclosure>
      )}
      {error.errors.map((member, index) => (
        <Disclosure key={index} title={`Grouped error ${index + 1}`}>
          <ErrorDetail error={member} place={`grouped error ${index + 1} of ${parent}`} />
        </Disclosure>
      ))}
    </div>
  )
}

/** A payload in its viewer. Live mode holds while you inspect it expanded. */
function Payload(props: PayloadViewerProps) {
  return <PayloadViewer {...props} onExpandedChange={useActivityInspect()} />
}
