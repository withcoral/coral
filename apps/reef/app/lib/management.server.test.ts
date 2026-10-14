import { Code, ConnectError } from '@connectrpc/connect'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { csrfTokenForRequest } from '@/auth/csrf.server'
import { requestAuthContext } from '@/auth/server-context'
import { authTestContext, localRuntimeConfig } from '@/auth/server-context.test-helper'
import { hostedRuntimeConfig, requiredAuthConfig } from '@/auth/test-config'

import { actionFailure, managementFailure, mutationForm } from './management.server'

afterEach(() => vi.restoreAllMocks())

const { publicUrl } = requiredAuthConfig

/** A POST of `fields` to `url`, with `headers`. */
function post(url: string, headers: Record<string, string>, fields: Record<string, string> = {}) {
  return new Request(url, { method: 'POST', headers, body: new URLSearchParams(fields) })
}

/** Hosted request context, and the cookie and token of its current CSRF pair. */
async function hostedCsrf() {
  const context = authTestContext('coral-token', hostedRuntimeConfig)
  const auth = context.get(requestAuthContext)
  if (auth.mode !== 'required') throw new Error('expected hosted auth')
  const { setCookie, token } = await csrfTokenForRequest(
    new Request(publicUrl),
    requiredAuthConfig,
    auth.session,
  )
  return { context, cookie: setCookie!.split(';', 1)[0], token }
}

function thrownBy(action: () => unknown): unknown {
  try {
    action()
  } catch (error) {
    return error
  }
  throw new Error('expected a throw')
}

async function refusal(promise: Promise<unknown>) {
  const thrown = await promise.then(
    () => undefined,
    (error: unknown) => error,
  )
  expect(thrown).toBeInstanceOf(Response)
  return thrown as Response
}

describe('mutationForm', () => {
  it('returns the form of a same-origin local POST', async () => {
    const form = await mutationForm(
      post('http://127.0.0.1:5173/workspaces', { origin: 'http://127.0.0.1:5173' }, { a: 'b' }),
      authTestContext(null, localRuntimeConfig),
    )
    expect(form.get('a')).toBe('b')
  })

  it.each(['GET', 'PUT', 'DELETE'])('refuses a %s with 405', async (method) => {
    const response = await refusal(
      mutationForm(
        new Request('http://127.0.0.1:5173/workspaces', {
          method,
          headers: { origin: 'http://127.0.0.1:5173' },
        }),
        authTestContext(null),
      ),
    )
    expect(response.status).toBe(405)
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
  })

  it.each([
    ['a missing origin', {}],
    ['a foreign origin', { origin: 'https://evil.example' }],
  ])('refuses %s with 403', async (_, headers) => {
    const response = await refusal(
      mutationForm(post('http://127.0.0.1:5173/workspaces', headers), authTestContext(null)),
    )
    expect(response.status).toBe(403)
    expect(await response.text()).toBe('Invalid request origin')
  })

  // Behind a proxy Reef sees its internal address; only the public URL is the browser's origin.
  it.each([
    ['the public URL', publicUrl, 200],
    ['the request address', 'http://reef.internal:3000', 403],
  ])('under required auth, accepts only %s as the origin', async (_, origin, status) => {
    const { context, cookie, token } = await hostedCsrf()
    const result = mutationForm(
      post('http://reef.internal:3000/workspaces', { origin, cookie }, { csrf: token }),
      context,
    )
    if (status === 200) await expect(result).resolves.toBeInstanceOf(FormData)
    else expect((await refusal(result)).status).toBe(status)
  })

  it.each([
    ['a missing CSRF token', (cookie: string) => ({ headers: { cookie }, fields: {} })],
    [
      'a wrong CSRF token',
      (cookie: string) => ({ headers: { cookie }, fields: { csrf: 'forged' } }),
    ],
    [
      'a CSRF token without its cookie',
      (_cookie: string, token: string) => ({ headers: {}, fields: { csrf: token } }),
    ],
  ])('refuses %s with 403 under required auth', async (_, attempt) => {
    const { context, cookie, token } = await hostedCsrf()
    const { headers, fields } = attempt(cookie, token)
    const response = await refusal(
      mutationForm(
        post(`${publicUrl}/workspaces`, { origin: publicUrl, ...headers }, fields),
        context,
      ),
    )
    expect(response.status).toBe(403)
    expect(await response.text()).toBe('Invalid CSRF token')
  })
})

const coralFailures = [
  [Code.NotFound, 404, 'This item does not exist.'],
  [
    Code.AlreadyExists,
    409,
    'This person is already a member. Use their actions menu to change their role.',
  ],
  [Code.PermissionDenied, 403, 'You do not have permission for this action.'],
  [Code.Unauthenticated, 401, 'Sign in again to continue.'],
  [
    Code.Aborted,
    409,
    'This item changed. Reload the page. Review the changes before you try again.',
  ],
  [Code.InvalidArgument, 400, 'Check the form fields.'],
  [
    Code.FailedPrecondition,
    409,
    'The current settings do not allow this change. Reload the page. Check the settings before you try again.',
  ],
  [Code.Internal, 503, 'Coral is not available.'],
  [Code.Unavailable, 503, 'Coral is not available.'],
] as const

describe('managementFailure', () => {
  it.each(coralFailures)('turns Coral code %s into %s', async (code, status, message) => {
    const response = await refusal(
      Promise.resolve().then(() => managementFailure(new ConnectError('detail', code))),
    )
    expect(response.status).toBe(status)
    expect(await response.text()).toBe(message)
    expect(response.headers.get('Cache-Control')).toBe('private, no-store')
  })

  it('reports a failure in Reef itself as unavailable, and logs it', async () => {
    const log = vi.spyOn(console, 'error').mockImplementation(() => {})
    const defect = new TypeError('undefined is not a function')
    const response = await refusal(Promise.resolve().then(() => managementFailure(defect)))
    expect(response.status).toBe(503)
    expect(await response.text()).toBe('Coral is not available.')
    expect(log).toHaveBeenCalledWith(expect.any(String), defect)
  })

  it('rethrows a route response unchanged', () => {
    const redirect = new Response(null, { status: 302, headers: { Location: '/login' } })
    expect(thrownBy(() => managementFailure(redirect))).toBe(redirect)
  })
})

describe('actionFailure', () => {
  it.each(coralFailures)('turns Coral code %s into %s', (code, status, message) => {
    const result = actionFailure(new ConnectError('detail', code), 'Coral could not add it. ')
    expect(result.init?.status).toBe(status)
    expect(result.data).toEqual({
      error: `Coral could not add it. ${message}`,
      credential: null,
    })
  })

  it('reports a failure that is not from Coral as unavailable', () => {
    const result = actionFailure(new Error('detail'))
    expect(result.init?.status).toBe(503)
    expect(result.data.error).toBe('Coral could not apply this change. Coral is not available.')
  })

  it.each([
    ['AMBIGUOUS_EMAIL', 'More than one person matches this email. Coral granted no access.'],
    ['WORKSPACE_ARCHIVED', 'This workspace is archived. Restore it before you make changes.'],
    ['LAST_WORKSPACE_OWNER', 'This workspace must keep one owner. Assign another owner first.'],
    [
      'OWNER_TRANSFER_REQUIRED',
      'This person is the only owner of a workspace. Choose a person to take over their workspaces.',
    ],
    [
      'OWNER_TARGET_NOT_ENABLED',
      'This person does not have an enabled organization membership. Choose another person.',
    ],
    [
      'REQUEST_ALREADY_APPLIED',
      'Coral already applied this request. Check the current list before you start another action.',
    ],
  ])('words the known reason %s instead of its code', (reason, message) => {
    const result = actionFailure(
      new ConnectError(reason, Code.FailedPrecondition),
      'Coral could not add it. ',
    )
    expect(result.init?.status).toBe(409)
    expect(result.data.error).toBe(message)
  })

  it('names what was not found when the caller says', () => {
    const result = actionFailure(
      new ConnectError('detail', Code.NotFound),
      'Coral could not add it. ',
      'The workspace was removed.',
    )
    expect(result.init?.status).toBe(404)
    expect(result.data.error).toBe('Coral could not add it. The workspace was removed.')
    // Other codes keep their own message.
    expect(
      actionFailure(new ConnectError('detail', Code.Aborted), '', 'The workspace was removed.').data
        .error,
    ).toMatch(/^This item changed/)
  })

  it('rethrows a route response unchanged', () => {
    const response = new Response('Invalid form', { status: 400 })
    expect(thrownBy(() => actionFailure(response))).toBe(response)
  })
})
