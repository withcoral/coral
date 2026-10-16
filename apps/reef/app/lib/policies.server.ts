import { data, type RouterContextProvider } from 'react-router'
import { OperationAccessState } from '@/generated/coral/management/v1/policy_pb'
import { operationCallPath } from './call-root'
import { formContext, privateHeaders, field } from './management.server'
import { providerName, listAllOperations } from './providers.server'
import { authorizedWorkspace } from './workspace-route.server'
import type { PolicyView, AccessState } from '@/views/accounts/policy-page'

const wireState: Record<AccessState, OperationAccessState> = {
  enabled: OperationAccessState.ENABLED,
  approval_required: OperationAccessState.APPROVAL_REQUIRED,
  disabled: OperationAccessState.DISABLED,
}
function state(value: OperationAccessState): AccessState {
  const found = (Object.keys(wireState) as AccessState[]).find((key) => wireState[key] === value)
  if (!found)
    throw new Response('Unrecognized policy state', { status: 503, headers: privateHeaders })
  return found
}
export async function loadPolicy(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; provider: string },
): Promise<PolicyView> {
  const { api, workspace } = await authorizedWorkspace(request, context, params.workspace)
  const name = providerName(workspace.name, params.provider)
  const provider = await api.providers.getProvider({ name })
  const url = new URL(request.url)
  const exact = url.searchParams.get('operation')
  let operations
  let next = ''
  if (exact !== null) {
    operations = (await listAllOperations(api, provider.name)).filter(
      (o) => o.relativePath === exact,
    )
    if (operations.length !== 1)
      throw new Response('Not found', { status: 404, headers: privateHeaders })
  } else {
    const page = await api.providers.listOperations({
      parent: provider.name,
      pageSize: 100,
      pageToken: url.searchParams.get('cursor') ?? '',
    })
    operations = page.operations
    next = page.nextPageToken
  }
  const policies = operations.length
    ? (
        await api.policies.getOperationPolicies({
          parent: name,
          operations: operations.map((o) => o.name),
        })
      ).policies
    : []
  const byName = new Map(policies.map((p) => [p.operation, p]))
  return {
    workspace,
    parents: [
      { label: 'Providers', to: `${workspace.path}/providers` },
      { label: provider.displayName, to: `${workspace.path}/providers/${params.provider}` },
    ],
    canChange: workspace.effectiveRole === 'owner' && workspace.state === 'active',
    operations: operations.map((o) => {
      const policy = byName.get(o.name)
      if (!policy)
        throw new Response('Policy unavailable', { status: 503, headers: privateHeaders })
      return {
        name: o.name,
        path: operationCallPath(provider.callRoot, o.relativePath),
        state: state(policy.accessState),
        explicit: policy.explicit,
        available: o.available,
      }
    }),
    next: next ? `${url.pathname}?cursor=${encodeURIComponent(next)}` : '',
    form: formContext(context),
  }
}
export async function policyAction(
  request: Request,
  context: Readonly<RouterContextProvider>,
  params: { workspace: string; provider: string },
  form: FormData,
) {
  const { api, workspace } = await authorizedWorkspace(request, context, params.workspace, {
    ownerOnly: true,
  })
  const selected = form.getAll('operation')
  const access = field(form, 'access')
  if (
    !Object.hasOwn(wireState, access) ||
    selected.length === 0 ||
    selected.length > 100 ||
    new Set(selected).size !== selected.length ||
    selected.some((s) => typeof s !== 'string' || s.length > 4096)
  )
    throw new Response('Select 1–100 distinct operations and an access state.', {
      status: 400,
      headers: privateHeaders,
    })
  await api.policies.updateOperationPolicies({
    parent: providerName(workspace.name, params.provider),
    policies: selected.map((name) => ({
      operation: name as string,
      accessState: wireState[access as AccessState],
    })),
  })
  return data(
    { error: null, message: 'Operation policy saved.', credential: null },
    { headers: privateHeaders },
  )
}
