import { beforeEach, describe, expect, it, vi } from 'vitest'

const { addFunction, deleteFunction, getFunction, listFunctions } = vi.hoisted(() => ({
  addFunction: vi.fn(),
  deleteFunction: vi.fn(),
  getFunction: vi.fn(),
  listFunctions: vi.fn(),
}))

vi.mock('@/lib/coral-request.server', () => ({
  functionClientForRequest: () => ({ addFunction, deleteFunction, getFunction, listFunctions }),
}))

import { action, loader } from './functions.server'

describe('functions route server boundary', () => {
  beforeEach(() => {
    addFunction.mockReset().mockResolvedValue({})
    deleteFunction.mockReset().mockResolvedValue({})
    getFunction.mockReset()
    listFunctions.mockReset().mockResolvedValue({ functions: [] })
  })

  it('loads function status for the route workspace', async () => {
    listFunctions.mockResolvedValue({
      functions: [
        {
          name: 'retrieve_pull_requests',
          runtime: {
            case: 'ready',
            value: {
              arguments: [{ dataType: 'Utf8', name: 'owner' }],
              description: 'Retrieve pull requests',
              tableFunction: { schemaName: 'github' },
            },
          },
        },
      ],
    })

    const result = await loader({
      params: { workspaceId: 'analytics' },
      request: new Request('http://reef.test/workspaces/analytics/functions'),
    } as never)

    expect(listFunctions).toHaveBeenCalledWith(
      expect.objectContaining({ workspace: expect.objectContaining({ name: 'analytics' }) }),
    )
    expect(result.functions).toEqual([
      expect.objectContaining({
        name: 'retrieve_pull_requests',
        schema: 'github',
        status: 'ready',
      }),
    ])
  })

  it('serializes editor fields and saves through add-as-upsert', async () => {
    const request = new Request('http://reef.test/workspaces/analytics/functions?new', {
      body: new URLSearchParams({
        _intent: 'save',
        description: 'Retrieve pull requests',
        name: 'retrieve_pull_requests',
        schema: 'github',
        sql: 'select * from github.pulls(owner => $owner)',
      }),
      method: 'POST',
    })

    const response = await action({ params: { workspaceId: 'analytics' }, request } as never)

    expect(addFunction).toHaveBeenCalledWith(
      expect.objectContaining({
        sql: expect.stringContaining('name: retrieve_pull_requests'),
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
    )
    expect(response).toBeInstanceOf(Response)
    expect((response as Response).headers.get('location')).toBe('/workspaces/analytics/functions')
  })

  it('does not let the new-function flow replace an existing function', async () => {
    addFunction.mockRejectedValue(new Error("function 'existing' already exists"))
    const request = new Request('http://reef.test/workspaces/analytics/functions?new', {
      body: new URLSearchParams({
        _intent: 'save',
        description: '',
        name: 'existing',
        schema: 'github',
        sql: 'select 1',
      }),
      method: 'POST',
    })

    await expect(
      action({ params: { workspaceId: 'analytics' }, request } as never),
    ).resolves.toMatchObject({ message: "function 'existing' already exists", status: 'error' })
    expect(addFunction).toHaveBeenCalledWith(expect.objectContaining({ failIfExists: true }))
  })

  it('refuses to edit an artifact whose name drifted from its inventory identity', async () => {
    getFunction.mockResolvedValue({
      sql: '/*\nname: other\nschema: github\ndescription: drifted\n*/\n\nselect 1',
    })

    const result = await loader({
      params: { workspaceId: 'analytics' },
      request: new Request('http://reef.test/workspaces/analytics/functions?edit=selected'),
    } as never)

    expect(result.editor).toMatchObject({
      artifact: { name: 'selected' },
      loadError: expect.stringContaining("declares 'other'"),
      mode: 'edit',
    })
  })

  it('rejects renaming an existing function before calling Coral', async () => {
    const request = new Request('http://reef.test/workspaces/analytics/functions?edit=old_name', {
      body: new URLSearchParams({
        _intent: 'save',
        description: '',
        name: 'new_name',
        originalName: 'old_name',
        schema: 'github',
        sql: 'select 1',
      }),
      method: 'POST',
    })

    await expect(
      action({ params: { workspaceId: 'analytics' }, request } as never),
    ).resolves.toMatchObject({
      status: 'error',
      message: expect.stringContaining('cannot be changed'),
    })
    expect(addFunction).not.toHaveBeenCalled()
  })

  it('deletes from the route workspace', async () => {
    const request = new Request('http://reef.test/workspaces/analytics/functions?delete=test', {
      body: new URLSearchParams({ _intent: 'delete', name: 'test' }),
      method: 'POST',
    })

    await action({ params: { workspaceId: 'analytics' }, request } as never)

    expect(deleteFunction).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'test',
        workspace: expect.objectContaining({ name: 'analytics' }),
      }),
    )
  })
})
