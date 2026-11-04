import { activityStream, type StreamDeadlines } from './activity-stream.server'
import { loadActivity, type ActivityApi } from './activity.server'
import type { ActivityObservation } from './activity-live-types'
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
      !value.window.roots.every(name)
    )
      throw invalid()
    return {
      path: value.path,
      search: value.search,
      window: { roots: value.window.roots, followRoot: value.window.followRoot },
    }
  } catch (error) {
    if (error instanceof Response) throw error
    throw invalid()
  } finally {
    reader.releaseLock()
  }
}

/** Read-only observation: the list, the open root and the selected detail each cross their owner's authorization. */
export async function observeActivity(
  request: Request,
  api: ActivityApi,
  deadlines?: StreamDeadlines,
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
    deadlines,
  )
}
