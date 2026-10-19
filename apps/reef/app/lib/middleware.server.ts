import { data, type RouterContextProvider } from 'react-router'
import {
  MiddlewareDiagnosticCategory,
  MiddlewareMoveDirection,
  type Middleware,
} from '@/generated/coral/management/v1/middleware_pb'
import type { MiddlewareDetailView, MiddlewareView } from '@/views/middleware/types'
import { field, formContext, privateHeaders, resourcePath } from './management.server'
import { isoTime } from './management-views.server'
import { authorizedWorkspace } from './workspace-route.server'

const categories = {
  [MiddlewareDiagnosticCategory.ERROR]: 'error',
  [MiddlewareDiagnosticCategory.WARNING]: 'warning',
  [MiddlewareDiagnosticCategory.SUGGESTION]: 'suggestion',
  [MiddlewareDiagnosticCategory.MESSAGE]: 'message',
  [MiddlewareDiagnosticCategory.UNSPECIFIED]: 'message',
} as const

export function middlewareView(value: Middleware): MiddlewareView {
  return {
    name: value.name,
    path: resourcePath(value.name),
    displayName: value.displayName,
    description: value.description,
    enabled: value.enabled,
    scopes: {
      run: value.scopes?.run ?? false,
      function: value.scopes?.function ?? false,
      operation: value.scopes?.operation ?? false,
    },
    position: value.position,
    updatedAt: isoTime(value.updateTime),
  }
}

export function middlewareDetailView(value: Middleware): MiddlewareDetailView {
  return {
    ...middlewareView(value),
    source: value.source,
    diagnostics: value.diagnostics.map((d) => ({
      category: categories[d.category] ?? 'message',
      code: Number(d.code),
      message: d.message,
      line: d.line === undefined ? null : Number(d.line),
      column: d.column === undefined ? null : Number(d.column),
    })),
  }
}

/** The Owner-only chain, outermost first. */
export async function loadMiddleware(
  request: Request,
  context: Readonly<RouterContextProvider>,
  workspace: string,
) {
  const { api, workspace: w } = await authorizedWorkspace(request, context, workspace, {
    ownerOnly: true,
  })
  const list = await api.middleware.listMiddleware({ parent: w.name })
  return {
    workspace: w,
    middleware: list.middleware.map(middlewareView),
    canChange: w.state === 'active',
    form: formContext(context),
  }
}

const messages = {
  enabled: 'Middleware enabled. New executions use it.',
  disabled: 'Middleware disabled. New executions do not use it.',
  scopes: 'Scopes saved. Coral disabled the middleware. Review it, then enable it again.',
  move: 'Order saved. Coral disabled the moved middleware. Review it, then enable it again.',
  delete: 'Middleware deleted. Running executions keep the middleware they started with.',
} as const

/** Applies one Owner change. Coral, not Reef, decides who can make it. */
export async function middlewareAction(
  request: Request,
  context: Readonly<RouterContextProvider>,
  workspace: string,
  form: FormData,
) {
  const { api, workspace: w } = await authorizedWorkspace(request, context, workspace, {
    ownerOnly: true,
  })
  const name = field(form, 'name')
  if (!name.startsWith(`${w.name}/middleware/`) || name.split('/').length !== 4)
    throw new Response('Invalid middleware', { status: 400, headers: privateHeaders })
  let message: string
  switch (field(form, 'intent')) {
    case 'enable': {
      const enabled = field(form, 'enabled') === 'true'
      await api.middleware.setMiddlewareEnabled({ name, enabled })
      message = enabled ? messages.enabled : messages.disabled
      break
    }
    case 'scopes':
      await api.middleware.updateMiddlewareScopes({
        name,
        scopes: {
          run: form.get('run') === 'on',
          function: form.get('function') === 'on',
          operation: form.get('operation') === 'on',
        },
      })
      message = messages.scopes
      break
    case 'move': {
      const direction = field(form, 'direction')
      if (direction !== 'up' && direction !== 'down')
        throw new Response('Invalid direction', { status: 400, headers: privateHeaders })
      await api.middleware.moveMiddleware({
        name,
        direction: direction === 'up' ? MiddlewareMoveDirection.UP : MiddlewareMoveDirection.DOWN,
      })
      message = messages.move
      break
    }
    case 'delete':
      await api.middleware.deleteMiddleware({ name })
      message = messages.delete
      break
    default:
      throw new Response('Invalid action', { status: 400, headers: privateHeaders })
  }
  return data({ error: null, credential: null, message }, { headers: privateHeaders })
}
