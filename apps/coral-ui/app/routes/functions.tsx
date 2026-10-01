import { create } from '@bufbuild/protobuf'

import type { Route } from './+types/functions'

import { requestAuthContext } from '@/auth/server-context'
import type { FunctionDetailsProps } from '@/components/functions'
import {
  DeleteFunctionRequestSchema,
  ListFunctionsRequestSchema,
  type Function,
} from '@/generated/coral/v1/functions_pb'
import { functionClientForRequest } from '@/lib/coral-request.server'
import { errorMessage } from '@/lib/utils'
import { workspaceFromParams } from '@/lib/workspace-routing'
import { FunctionsIndex } from '@/views/functions/functions-index'

export interface FunctionsRouteData {
  functions: FunctionDetailsProps[]
  loadError: string | null
}

export type FunctionsActionData =
  | { message: string; name: string; status: 'error' }
  | { name: string; status: 'success' }

export async function action({
  context,
  params,
  request,
}: Route.ActionArgs): Promise<FunctionsActionData> {
  const formData = await request.formData()
  const nameValue = formData.get('name')
  const name = typeof nameValue === 'string' ? nameValue : ''
  if (!name) return { message: 'Missing function name', name, status: 'error' }

  try {
    const workspace = workspaceFromParams(params)
    await functionClientForRequest(
      request,
      context.get(requestAuthContext).accessToken,
    ).deleteFunction(create(DeleteFunctionRequestSchema, { name, workspace }), {
      signal: request.signal,
    })
    return { name, status: 'success' }
  } catch (error) {
    return { message: errorMessage(error), name, status: 'error' }
  }
}

export async function loader({
  context,
  params,
  request,
}: Route.LoaderArgs): Promise<FunctionsRouteData> {
  try {
    const workspace = workspaceFromParams(params)
    const response = await functionClientForRequest(
      request,
      context.get(requestAuthContext).accessToken,
    ).listFunctions(create(ListFunctionsRequestSchema, { workspace }), { signal: request.signal })
    return {
      functions: response.functions
        .map(toFunctionDetails)
        .filter((fn): fn is FunctionDetailsProps => fn !== null)
        .toSorted((left, right) => left.name.localeCompare(right.name)),
      loadError: null,
    }
  } catch (error) {
    return { functions: [], loadError: errorMessage(error) }
  }
}

export default function FunctionsRoute({ loaderData }: Route.ComponentProps) {
  return <FunctionsIndex {...loaderData} />
}

export function toFunctionDetails(fn: Function): FunctionDetailsProps | null {
  if (fn.runtime.case !== 'ready') return null
  const ready = fn.runtime.value
  if (!ready.tableFunction?.schemaName) return null
  return {
    arguments: ready.arguments.map(({ dataType, name }) => ({ dataType, name })),
    body: ready.sqlBody,
    description: ready.description || ready.tableFunction?.description || '',
    name: fn.name,
    namespace: ready.tableFunction.schemaName,
    resultColumns: ready.resultColumns.map(({ dataType, name, nullable }) => ({
      dataType,
      name,
      nullable,
    })),
    sources: ready.sourceNames,
  }
}
