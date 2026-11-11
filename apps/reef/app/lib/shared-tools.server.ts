import { Code, ConnectError } from '@connectrpc/connect'
import { data, redirect, type RouterContextProvider } from 'react-router'
import { ToolAccessState } from '@/generated/coral/management/v1/policy_pb'
import type { SharedToolRevision } from '@/generated/coral/management/v1/shared_tool_pb'
import type { PersonView } from '@/views/management/types'
import { SHARED_ICONS } from '@/views/providers/coral-icons'
import type { ProviderView, WorkspaceView } from '@/views/providers/types'
import {
  SHARED_TOOL_PUBLISHED,
  SHARED_TOOL_REMOVED,
  sharedToolHref,
  sharedToolsPath,
} from '@/views/tools/addresses'
import type {
  PublishOrigin,
  PublishReviewResult,
  SharedToolDetailView,
  ToolSourceView,
} from '@/views/tools/types'
import { sharedToolCallPath } from './call-root'
import { refusalMetadata } from './error-info.server'
import {
  agentName,
  failureReason,
  field,
  management,
  privateHeaders,
  resourcePath,
  workspaceName,
  type ManagementApi,
} from './management.server'
import { diagnosticView, isoTime, ownerView } from './management-views.server'
import { authoredGroupView, authoredToolRow, authoredToolRows } from './private-tools.server'
import { accessStateOf, policyState, toolView } from './providers.server'

/**
 * The shared group of the Tools page: the Shared Provider with the workspace's
 * shared tools, which every member reads.
 */
export async function sharedGroup(
  api: ManagementApi,
  workspace: Pick<WorkspaceView, 'name' | 'path'>,
): Promise<ProviderView> {
  // Coral's provider list leaves the Shared Provider out. Every page of the
  // shared tool list names it, so the smallest page is enough.
  const { sharedProvider: name } = await api.sharedTools.listSharedTools({
    parent: workspace.name,
    pageSize: 1,
  })
  const { provider, rows } = await authoredToolRows(api, name)
  return {
    ...authoredGroupView(
      provider,
      rows.map((row) => authoredToolRow(toolView(row))),
    ),
    kind: 'shared',
    path: sharedToolsPath(workspace.path),
    icons: SHARED_ICONS,
  }
}

/**
 * Reads the one shared tool the drawer shows: its current revision with its
 * source, and every revision.
 */
export async function sharedToolDetail(
  api: Pick<ManagementApi, 'sharedTools' | 'memberships'>,
  workspace: string,
  path: string,
): Promise<SharedToolDetailView> {
  const tool = await api.sharedTools.getSharedTool({ parent: workspace, path }).catch((error) => {
    // A path that Coral cannot parse names no shared tool.
    if (error instanceof ConnectError && error.code === Code.InvalidArgument)
      throw new Response('Not found', { status: 404, headers: privateHeaders })
    throw error
  })
  const current = tool.currentRevision
  if (!current) throw new Response('Coral is not available.', { status: 503 })
  const [{ revisions }, person] = await Promise.all([
    api.sharedTools.listSharedToolRevisions({ sharedTool: tool.name }),
    ownersByUser(api, workspace),
  ])
  return {
    kind: 'shared',
    relativePath: tool.path,
    description: current.description,
    ...sourceView(current),
    revision: Number(current.revision),
    publisher: person(current.publisher),
    publishedAt: isoTime(current.publishTime),
    revisions: revisions.map((revision) => ({
      revision: Number(revision.revision),
      description: revision.description,
      publisher: person(revision.publisher),
      publishedAt: isoTime(revision.publishTime),
      restoredFrom: Number(revision.restoredFromRevision) || null,
    })),
  }
}

function sourceView(
  revision: Pick<
    SharedToolRevision,
    'declaration' | 'source' | 'diagnostics' | 'diagnosticsTruncated'
  >,
): ToolSourceView {
  return {
    declaration: revision.declaration,
    source: revision.source,
    diagnostics: revision.diagnostics.map(diagnosticView),
    diagnosticsTruncated: revision.diagnosticsTruncated,
  }
}

/**
 * Names the person who published or removed a shared tool. Only an owner does
 * either, and every member reads the workspace's owners, so the page names a
 * person while they are an owner. A shared tool outlives its author: anybody
 * else comes back as null, and the page calls them a former owner.
 */
async function ownersByUser(api: Pick<ManagementApi, 'memberships'>, parent: string) {
  const owners = new Map<string, PersonView>()
  let pageToken = ''
  do {
    const page = await api.memberships.listWorkspaceOwners({ parent, pageSize: 100, pageToken })
    for (const owner of page.owners) owners.set(owner.user, ownerView(owner).profile)
    pageToken = page.nextPageToken
  } while (pageToken)
  return (user: string) => owners.get(user) ?? null
}

const invalid = (reason: string) => new Response(reason, { status: 400, headers: privateHeaders })

// A revision number as an address or a form spells it: from 1, with no sign.
const REVISION = /^[1-9][0-9]{0,15}$/

/** What a publish dialog asks to review, from its address, in the workspace with this identifier. */
function requestedOrigin(workspace: string, query: URLSearchParams): PublishOrigin {
  const restore = query.get('restore')
  if (restore !== null) {
    if (!REVISION.test(restore)) throw invalid('Bad request')
    return { kind: 'restore', revision: Number(restore) }
  }
  const path = query.get('tool')
  if (!path) throw invalid('Bad request')
  const agent = query.get('agent')
  // Refuses an identifier that names no agent before Coral reads it.
  agentResource(workspace, agent)
  return { kind: 'private', path, agent }
}

/** The resource name of the agent with this identifier, or empty for the reader's own tools. */
function agentResource(workspace: string, agent: string | null) {
  return agent ? agentName(workspace, agent) : ''
}

/** A publish's origin as Coral reads it. */
function wireOrigin(workspace: string, origin: PublishOrigin) {
  return origin.kind === 'restore'
    ? { case: 'restoreRevision' as const, value: BigInt(origin.revision) }
    : {
        case: 'privateTool' as const,
        value: { path: origin.path, agent: agentResource(workspace, origin.agent) },
      }
}

/** The live shared tool at a path, or null when the path is free or is not a path. */
async function sharedToolAt(api: Pick<ManagementApi, 'sharedTools'>, parent: string, path: string) {
  try {
    return await api.sharedTools.getSharedTool({ parent, path })
  } catch (error) {
    if (
      error instanceof ConnectError &&
      (error.code === Code.NotFound || error.code === Code.InvalidArgument)
    )
      return null
    throw error
  }
}

/**
 * Runs the review of a publish and writes nothing. The address names what the
 * publish copies: `tool`, a private tool of the reader or of `agent`, or
 * `restore`, a revision of the shared tool at `path`. `path` is the shared
 * tool path; a private tool's own path stands in for it.
 *
 * A refusal Coral gives comes back as text, not as a failed read, so the
 * dialog shows it and the owner corrects the path there. `workspace` is the
 * workspace's identifier, as for the publish and the removal.
 */
export async function publishReview(
  api: Pick<ManagementApi, 'privateTools' | 'sharedTools' | 'memberships'>,
  workspace: string,
  query: URLSearchParams,
): Promise<PublishReviewResult> {
  const parent = workspaceName(workspace)
  const origin = requestedOrigin(workspace, query)
  const path = query.get('path') ?? (origin.kind === 'private' ? origin.path : '')
  try {
    const current = await sharedToolAt(api, parent, path)
    let content: { description: string; source: string }
    // A new revision, a restore too, starts on the policy the shared tool has
    // now: a rollback of the source does not also loosen its policy.
    const defaultPolicy = current?.currentRevision
      ? policyState(current.currentRevision.defaultAccessState)
      : null
    if (origin.kind === 'private') {
      content = await api.privateTools.getPrivateTool({
        parent,
        path: origin.path,
        agent: agentResource(workspace, origin.agent),
      })
    } else {
      if (!current) return { review: null, path, refusal: publishRefusals.gone }
      const restored = await api.sharedTools.getSharedToolRevision({
        sharedTool: current.name,
        revision: BigInt(origin.revision),
      })
      content = restored
    }
    const { review } = await api.sharedTools.publishSharedTool({
      parent,
      path,
      origin: wireOrigin(workspace, origin),
      description: content.description,
      source: content.source,
      // A review stores no default; Coral only needs one it accepts.
      defaultAccessState: ToolAccessState.APPROVAL_REQUIRED,
      expectedRevision: current?.currentRevision?.revision ?? 0n,
      validateOnly: true,
    })
    if (!review) throw new ConnectError('no review', Code.Unavailable)
    const previous = review.previousRevision
    // The owner sees whose revision the publish replaces.
    const person = previous ? await ownersByUser(api, parent) : null
    return {
      refusal: null,
      review: {
        origin,
        path,
        description: content.description,
        source: content.source,
        declaration: review.declaration,
        diagnostics: review.diagnostics.map(diagnosticView),
        diagnosticsTruncated: review.diagnosticsTruncated,
        previous:
          previous && person
            ? {
                revision: Number(previous.revision),
                description: previous.description,
                source: previous.source,
                declaration: previous.declaration,
                publisher: person(previous.publisher),
                publishedAt: isoTime(previous.publishTime),
              }
            : null,
        calledTools: review.calledTools,
        sharedTools: review.sharedTools.map((tool) => ({
          path: tool.path,
          revision: Number(tool.revision),
        })),
        runtimeBuiltPaths: review.runtimeBuiltPaths,
        defaultPolicy,
      },
    }
  } catch (error) {
    return { review: null, path, refusal: publishFailure(error)[1] }
  }
}

const publishRefusals = {
  gone: 'This shared tool no longer exists. Close this dialog.',
  moved: 'Another owner published this shared tool first. Review the changes again.',
  declaration:
    'A tool that this source calls changed after your review. Review the declaration again.',
  taken: 'Another shared tool uses this path, or a part of it. Enter another path.',
  unreadable: 'The private tool or the revision no longer exists. Close this dialog.',
  denied: 'Ask a workspace owner to publish this tool.',
}

/**
 * Why Coral refused a publish or its review, as the dialog says it. A source
 * that cannot become a tool keeps Coral's text after the instruction, because
 * that text says where the source fails.
 */
function publishFailure(error: unknown): [status: number, message: string] {
  if (error instanceof ConnectError) {
    const corrections: Record<string, string> = {
      SHARED_TOOL_PATH_INVALID:
        'Enter a path of 1 to 16 names with a dot between each, such as mail.remind. Each name is a JavaScript identifier.',
      SHARED_TOOL_RESTORE_DIFFERS:
        'This content differs from the revision. Review the revision again.',
    }
    if (error.code === Code.InvalidArgument && corrections[error.rawMessage])
      return [400, corrections[error.rawMessage]!]
    const { message } = refusalMetadata(error)
    if (error.rawMessage === 'SHARED_TOOL_SOURCE_REJECTED' && message)
      return [400, `Correct the source in the private tool. ${message}`]
    if (error.code === Code.Aborted) return [409, publishRefusals.moved]
    if (error.code === Code.PermissionDenied) return [403, publishRefusals.denied]
    if (error.code === Code.NotFound) return [404, publishRefusals.unreadable]
    const reasons: Record<string, string> = {
      SHARED_TOOL_DECLARATION_CHANGED: publishRefusals.declaration,
      SHARED_TOOL_PATH_TAKEN: publishRefusals.taken,
      SHARED_TOOL_SOURCE_UNREADABLE: publishRefusals.unreadable,
    }
    if (error.code === Code.FailedPrecondition && reasons[error.rawMessage])
      return [409, reasons[error.rawMessage]!]
  }
  return failureReason(error, 'Coral could not publish this shared tool. ')
}

/** The origin a publish form names, with the same fields as the review's address. */
function submittedOrigin(workspace: string, form: FormData): PublishOrigin {
  const query = new URLSearchParams()
  for (const name of ['tool', 'agent', 'restore'])
    if (form.has(name)) query.set(name, field(form, name))
  return requestedOrigin(workspace, query)
}

// Coral's limits on a description, a source and the declaration it emits, in
// bytes. A string holds no more characters than bytes, so these bound the text.
const REVIEWED_MAX = { description: 4 * 1024, source: 64 * 1024, declaration: 256 * 1024 }
// JSON spells a character in at most six.
const REVIEWED_FIELD_MAX =
  6 * (REVIEWED_MAX.description + REVIEWED_MAX.source + REVIEWED_MAX.declaration)

/**
 * The reviewed description, source and declaration of a publish form. A form
 * post rewrites each line break of a field as CRLF, which would change the
 * source. So the dialog sends the three as one JSON field, which holds no raw
 * line break, and they arrive as the review showed them.
 */
function reviewedContent(form: FormData) {
  let content: unknown
  try {
    content = JSON.parse(field(form, 'reviewed', REVIEWED_FIELD_MAX))
  } catch (error) {
    if (error instanceof Response) throw error
    throw invalid('Invalid form')
  }
  if (typeof content !== 'object' || content === null) throw invalid('Invalid form')
  const text = (name: keyof typeof REVIEWED_MAX) => {
    const value = (content as Record<string, unknown>)[name]
    if (typeof value !== 'string' || value.length > REVIEWED_MAX[name])
      throw invalid('Invalid form')
    return value
  }
  return {
    description: text('description'),
    source: text('source'),
    reviewedDeclaration: text('declaration'),
  }
}

/**
 * Publishes what the owner reviewed: the form holds the exact description and
 * source of the review, the declaration it showed, and the revision it
 * replaces. Then opens the shared tool. A refusal stays in the dialog.
 *
 * An agent may save the private tool again after the review. The publish
 * still stores the reviewed bytes, so it never takes the newer ones, and the
 * dialog does not say that the private tool moved: Coral keeps no signal for
 * it. The owner reviews again to publish the newer source.
 */
export async function publishSharedTool(
  context: Readonly<RouterContextProvider>,
  workspace: string,
  form: FormData,
) {
  const parent = workspaceName(workspace)
  const path = field(form, 'path')
  const defaultAccessState = accessStateOf(field(form, 'access'))
  const expected = field(form, 'expectedRevision')
  // Zero expects no shared tool at the path: the publish creates it.
  if (defaultAccessState === undefined || !(expected === '0' || REVISION.test(expected)))
    throw invalid('Invalid form')
  const request = {
    parent,
    path,
    origin: wireOrigin(workspace, submittedOrigin(workspace, form)),
    ...reviewedContent(form),
    defaultAccessState,
    expectedRevision: BigInt(expected),
  }
  let revision: bigint
  try {
    const published = await management(context).sharedTools.publishSharedTool(request)
    revision = published.sharedTool?.currentRevision?.revision ?? request.expectedRevision + 1n
  } catch (error) {
    const [status, message] = publishFailure(error)
    return data({ error: message, credential: null }, { status, headers: privateHeaders })
  }
  return redirect(
    `${sharedToolHref(resourcePath(parent), path)}?${SHARED_TOOL_PUBLISHED}=${revision}`,
    { headers: privateHeaders },
  )
}

/**
 * Removes the shared tool at the form's path, at the revision the owner saw,
 * and returns to the shared page. A refusal stays in the dialog: while another
 * shared tool calls this one, it names that tool.
 */
export async function removeSharedTool(
  context: Readonly<RouterContextProvider>,
  workspace: string,
  form: FormData,
) {
  const parent = workspaceName(workspace)
  const path = field(form, 'path')
  const expected = field(form, 'expectedRevision')
  if (!REVISION.test(expected)) throw invalid('Invalid form')
  try {
    await management(context).sharedTools.removeSharedTool({
      parent,
      path,
      expectedRevision: BigInt(expected),
    })
  } catch (error) {
    const [status, message] = removalFailure(error)
    return data({ error: message, credential: null }, { status, headers: privateHeaders })
  }
  return redirect(`${sharedToolsPath(resourcePath(parent))}?${SHARED_TOOL_REMOVED}=1`, {
    headers: privateHeaders,
  })
}

function removalFailure(error: unknown): [status: number, message: string] {
  if (error instanceof ConnectError) {
    const { dependent } = refusalMetadata(error)
    if (error.rawMessage === 'SHARED_TOOL_HAS_DEPENDENTS' && dependent)
      return [
        409,
        `The shared tool ${sharedToolCallPath(dependent)} calls this one. Change or remove that shared tool first.`,
      ]
    if (error.code === Code.Aborted)
      return [
        409,
        'Another owner published this shared tool again. Check the new revision before you remove it.',
      ]
    if (error.code === Code.PermissionDenied)
      return [403, 'Ask a workspace owner to remove this shared tool.']
  }
  return failureReason(
    error,
    'Coral could not remove this shared tool. ',
    'It no longer exists. Close this panel.',
  )
}
