import { create } from '@bufbuild/protobuf'
import { redirect } from 'react-router'

import type { Route } from './+types/functions'

import {
  AddFunctionRequestSchema,
  DeleteFunctionRequestSchema,
  GetFunctionRequestSchema,
  ListFunctionsRequestSchema,
} from '@/generated/coral/v1/functions_pb'
import {
  formatFunctionArtifact,
  parseFunctionArtifact,
  type FunctionArtifact,
} from '@/lib/function-artifact.server'
import { functionClientForRequest } from '@/lib/coral-request.server'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'
import { routePath } from '@/routing/routemap'

export interface FunctionSummary {
  arguments: { dataType: string; name: string }[]
  description: string
  error: string | null
  name: string
  schema: string | null
  status: 'invalid' | 'ready'
}

export type FunctionEditor =
  | { artifact: FunctionArtifact; loadError: string | null; mode: 'edit' | 'new' }
  | { mode: 'delete'; name: string }
  | null

export interface FunctionsRouteData {
  editor: FunctionEditor
  functions: FunctionSummary[]
  loadError: string | null
}

export type FunctionsActionData =
  | { intent: 'delete' | 'save'; message: string; status: 'error' }
  | undefined

export async function loader({ params, request }: Route.LoaderArgs): Promise<FunctionsRouteData> {
  const workspace = workspaceFromParams(params)
  const client = functionClientForRequest(request)
  const url = new URL(request.url)

  let functions: FunctionSummary[] = []
  let loadError: string | null = null
  try {
    const response = await client.listFunctions(create(ListFunctionsRequestSchema, { workspace }))
    functions = response.functions.map(summarizeFunction)
  } catch (error) {
    loadError = errorMessage(error)
  }

  const editName = url.searchParams.get('edit')
  if (editName) {
    try {
      const response = await client.getFunction(
        create(GetFunctionRequestSchema, { name: editName, workspace }),
      )
      const artifact = parseFunctionArtifact(response.sql)
      if (artifact.name !== editName) {
        throw new Error(
          `Function artifact declares '${artifact.name}' but inventory entry is '${editName}'`,
        )
      }
      return {
        editor: { artifact, loadError: null, mode: 'edit' },
        functions,
        loadError,
      }
    } catch (error) {
      return {
        editor: {
          artifact: { description: '', name: editName, schema: '', sql: '' },
          loadError: errorMessage(error),
          mode: 'edit',
        },
        functions,
        loadError,
      }
    }
  }

  const deleteName = url.searchParams.get('delete')
  if (deleteName) return { editor: { mode: 'delete', name: deleteName }, functions, loadError }

  const editor = url.searchParams.has('new')
    ? {
        artifact: { description: '', name: '', schema: '', sql: '' },
        loadError: null,
        mode: 'new' as const,
      }
    : null
  return { editor, functions, loadError }
}

export async function action({ params, request }: Route.ActionArgs) {
  const workspace = workspaceFromParams(params)
  const formData = await request.formData()
  const intent = formValue(formData, '_intent')
  const path = routePath('workspaceFunctions', { workspaceId: workspace.name })

  try {
    if (intent === 'delete') {
      const name = requiredFormValue(formData, 'name')
      await functionClientForRequest(request).deleteFunction(
        create(DeleteFunctionRequestSchema, { name, workspace }),
      )
      return redirect(path)
    }
    if (intent !== 'save') return actionError('save', 'Unknown function action')

    const artifact = artifactFromForm(formData)
    const originalName = formValue(formData, 'originalName')
    if (originalName && originalName !== artifact.name) {
      return actionError('save', 'Function names cannot be changed after creation')
    }
    await functionClientForRequest(request).addFunction(
      create(AddFunctionRequestSchema, {
        failIfExists: !originalName,
        sql: formatFunctionArtifact(artifact),
        workspace,
      }),
    )
    return redirect(path)
  } catch (error) {
    return actionError(intent === 'delete' ? 'delete' : 'save', errorMessage(error))
  }
}

function summarizeFunction(fn: {
  name: string
  runtime: {
    case: 'invalid' | 'ready' | undefined
    value?: {
      arguments?: { dataType: string; name: string }[]
      description?: string
      reason?: string
      tableFunction?: { schemaName: string }
    }
  }
}): FunctionSummary {
  if (fn.runtime.case === 'ready') {
    const ready = fn.runtime.value
    return {
      arguments: ready?.arguments ?? [],
      description: ready?.description ?? '',
      error: null,
      name: fn.name,
      schema: ready?.tableFunction?.schemaName ?? null,
      status: 'ready',
    }
  }
  return {
    arguments: [],
    description: '',
    error: fn.runtime.case === 'invalid' ? (fn.runtime.value?.reason ?? 'Validation failed') : null,
    name: fn.name,
    schema: null,
    status: 'invalid',
  }
}

function artifactFromForm(formData: FormData): FunctionArtifact {
  return {
    description: rawFormValue(formData, 'description'),
    name: requiredFormValue(formData, 'name'),
    schema: requiredFormValue(formData, 'schema'),
    sql: requiredFormValue(formData, 'sql'),
  }
}

function formValue(formData: FormData, key: string): string {
  return rawFormValue(formData, key).trim()
}

function rawFormValue(formData: FormData, key: string): string {
  const value = formData.get(key)
  return typeof value === 'string' ? value : ''
}

function requiredFormValue(formData: FormData, key: string): string {
  const value = formValue(formData, key)
  if (!value) throw new Error(`${key === 'sql' ? 'SQL' : key} is required`)
  return value
}

function actionError(intent: 'delete' | 'save', message: string): FunctionsActionData {
  return { intent, message, status: 'error' }
}
