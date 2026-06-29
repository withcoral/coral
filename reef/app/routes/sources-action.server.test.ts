import { afterEach, describe, expect, it, vi } from 'vitest'

import type { Source, SourceInfo } from '@/generated/coral/v1/sources_pb'
import { SourceOrigin } from '@/generated/coral/v1/sources_pb'
import { sourceServiceForRequest } from '@/lib/source-service.server'

import {
  action,
  editBindingsFromForm,
  firstMissingRequiredInput,
  installBindingsFromForm,
} from './sources-action.server'

vi.mock('@/lib/source-service.server', () => ({
  sourceServiceForRequest: vi.fn(),
}))

describe('sources action form mapping', () => {
  it('maps install variable and secret inputs to source bindings', () => {
    const form = new FormData()
    form.set('var:region', ' us-east-1 ')
    form.set('sec:api_key', ' token ')

    expect(installBindingsFromForm(sourceInfo(), form)).toEqual([
      { key: 'region', secret: false, value: 'us-east-1' },
      { key: 'api_key', secret: true, value: 'token' },
    ])
  })

  it('reports the first missing required source-config secret', () => {
    const form = new FormData()
    form.set('var:region', 'us-east-1')

    expect(firstMissingRequiredInput(sourceInfo(), form)).toBe('api_key')
  })

  it('keeps existing edit variables and omits unchanged secrets', () => {
    const form = new FormData()
    form.set('var:region', 'eu-west-1')
    form.set('sec:api_key', '')

    expect(editBindingsFromForm(source(), sourceInfo(), form)).toEqual([
      { key: 'region', secret: false, value: 'eu-west-1' },
    ])
  })

  it('omits absent edit secret fields as unchanged', () => {
    const form = new FormData()
    form.set('var:region', 'eu-west-1')

    expect(editBindingsFromForm(source(), sourceInfo(), form)).toEqual([
      { key: 'region', secret: false, value: 'eu-west-1' },
    ])
  })

  it('falls back to installed bindings when source info is unavailable', () => {
    const form = new FormData()
    form.set('var:region', 'ap-south-1')
    form.set('sec:api_key', 'new-token')

    expect(editBindingsFromForm(source(), null, form)).toEqual([
      { key: 'region', secret: false, value: 'ap-south-1' },
      { key: 'api_key', secret: true, value: 'new-token' },
    ])
  })
})

describe('sources install action guards', () => {
  afterEach(() => {
    vi.mocked(sourceServiceForRequest).mockReset()
  })

  it('blocks installing an imported source that is already installed', async () => {
    const createBundledSource = vi.fn()
    stubService({
      createBundledSource,
      getSourceInfo: vi.fn(async () => ({
        info: {
          inputs: [],
          installed: true,
          name: 'github',
          origin: SourceOrigin.IMPORTED,
        } as unknown as SourceInfo,
      })),
    })

    const result = await action(actionArgs(installForm('github')))

    expect(result).toMatchObject({ intent: 'install', name: 'github', status: 'error' })
    expect((result as { message: string }).message).toMatch(/Imported sources/)
    expect(createBundledSource).not.toHaveBeenCalled()
  })

  it('blocks installing when the selected credential method is OAuth', async () => {
    const createBundledSource = vi.fn()
    stubService({
      createBundledSource,
      getSourceInfo: vi.fn(async () => ({ info: oauthSourceInfo() })),
    })

    const result = await action(actionArgs(installForm('slack')))

    expect(result).toMatchObject({ intent: 'install', name: 'slack', status: 'error' })
    expect((result as { message: string }).message).toMatch(/OAuth install is not available/)
    expect(createBundledSource).not.toHaveBeenCalled()
  })
})

function stubService(overrides: Partial<Record<string, unknown>>): void {
  const service = {
    createBundledSource: vi.fn(),
    deleteSource: vi.fn(),
    getInstalledSource: vi.fn(),
    getSourceInfo: vi.fn(),
    listCatalog: vi.fn(),
    ...overrides,
  }
  vi.mocked(sourceServiceForRequest).mockReturnValue(
    service as unknown as ReturnType<typeof sourceServiceForRequest>,
  )
}

function installForm(name: string): FormData {
  const form = new FormData()
  form.set('_intent', 'install')
  form.set('name', name)
  return form
}

function actionArgs(form: FormData): Parameters<typeof action>[0] {
  const request = new Request('http://localhost/sources', { body: form, method: 'POST' })
  return { request } as Parameters<typeof action>[0]
}

function oauthSourceInfo(): SourceInfo {
  return {
    inputs: [
      {
        hint: '',
        input: {
          case: 'secret',
          value: {
            credential: {
              methods: [
                { description: '', hint: '', label: '', method: { case: 'oauth', value: {} } },
              ],
            },
          },
        },
        key: 'token',
        required: true,
      },
    ],
    installed: false,
    name: 'slack',
    origin: SourceOrigin.BUNDLED,
  } as SourceInfo
}

function sourceInfo(): SourceInfo {
  return {
    inputs: [
      {
        hint: '',
        input: { case: 'variable', value: { defaultValue: 'us-west-2' } },
        key: 'region',
        required: true,
      },
      {
        hint: '',
        input: { case: 'secret', value: { credential: undefined } },
        key: 'api_key',
        required: true,
      },
    ],
  } as SourceInfo
}

function source(): Source {
  return {
    name: 'demo',
    secrets: [{ key: 'api_key', value: '' }],
    variables: [{ key: 'region', value: 'us-west-2' }],
    version: '1.0.0',
  } as Source
}
