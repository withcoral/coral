import { activityStream } from './activity-stream.server'
import { loadActivity, type ActivityApi } from './activity.server'
import { ACTIVITY_CHILD_WINDOW, type ActivityObservation } from './activity-live-types'
import { agentName, workspaceName, privateHeaders } from './management.server'

const invalid = () =>
  new Response('Invalid activity window', { status: 400, headers: privateHeaders })
const name = (value: unknown): value is string =>
  typeof value === 'string' &&
  value.length <= 512 &&
  /^workspaces\/[\w-]+\/(tasks|runs|invocations)\/[\w-]+(?:\/(interactions\/[\w-]+|events\/\d+|final))?$/.test(
    value,
  )
async function observation(request: Request): Promise<ActivityObservation> {
  const reader = request.body?.getReader()
  if (!reader) throw invalid()
  let text = ''
  let size = 0
  const decoder = new TextDecoder()
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      size += value.byteLength
      if (size > 262_144) {
        await reader.cancel()
        throw invalid()
      }
      text += decoder.decode(value, { stream: true })
    }
    text += decoder.decode()
    const value = JSON.parse(text) as ActivityObservation
    if (typeof value?.path === 'string')
      value.path = value.path.split('/').map(decodeURIComponent).join('/')
    if (
      !value ||
      typeof value.path !== 'string' ||
      !/^\/workspaces\/[\w-]+\/(activity|(agents|runs)\/[\w:.-]+)$/.test(value.path) ||
      typeof value.search !== 'string' ||
      value.search.length > 32768 ||
      !value.window ||
      typeof value.window.followRoot !== 'boolean' ||
      !Array.isArray(value.window.roots) ||
      value.window.roots.length > 25 ||
      !value.window.roots.every(name) ||
      !value.window.branches ||
      typeof value.window.branches !== 'object' ||
      Array.isArray(value.window.branches)
    )
      throw invalid()
    const branches = Object.entries(value.window.branches)
    if (
      branches.length > 20 ||
      branches.some(
        ([parent, viewport]) =>
          !name(parent) ||
          !viewport ||
          !Number.isSafeInteger(viewport.offset) ||
          viewport.offset < 0 ||
          !Number.isInteger(viewport.preceding) ||
          viewport.preceding < 0 ||
          viewport.preceding >= ACTIVITY_CHILD_WINDOW ||
          (viewport.anchor !== '' && !name(viewport.anchor)),
      )
    )
      throw invalid()
    return value
  } catch (error) {
    if (error instanceof Response) throw error
    throw invalid()
  } finally {
    reader.releaseLock()
  }
}

/** Read-only observation: every bounded window and selected detail crosses its owner's authorization. */
export async function observeActivity(
  request: Request,
  api: ActivityApi,
  expiresAt = Infinity,
): Promise<Response> {
  const input = await observation(request)
  const parts = input.path.split('/')
  const workspace = workspaceName(parts[2])
  const options =
    parts[3] === 'agents'
      ? { scope: { workspace, agent: agentName(parts[2], parts[4]) } }
      : parts[3] === 'runs'
        ? { scope: { workspace }, run: `${workspace}/runs/${parts[4]}` }
        : { scope: { workspace } }
  const url = new URL(input.path, request.url)
  url.search = input.search
  url.searchParams.set('tree', '1')
  for (const parent of Object.keys(input.window.branches)) {
    if (!url.searchParams.getAll('expand').includes(parent))
      url.searchParams.append('expand', parent)
  }
  return activityStream(
    request,
    api,
    async (signal) => ({
      kind: 'page',
      page: await loadActivity(new Request(url, { signal }), api, {
        ...options,
        reading: input.window,
      }),
    }),
    expiresAt,
  )
}
