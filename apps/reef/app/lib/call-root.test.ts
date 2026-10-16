import { describe, expect, it } from 'vitest'
import { operationOwner } from './call-root'

const github = { callRoot: 'coral.providers.github' }
const builtin = { callRoot: 'coral.workspace' }
const ownerWorkspace = { callRoot: 'coral.providers.workspace' }
const providers = [github, builtin, ownerWorkspace]

describe('operationOwner', () => {
  it.each([
    ['coral.providers.github.repos.get', github, 'repos.get'],
    ['coral.workspace.createProvider', builtin, 'createProvider'],
    ['coral.providers.workspace.createProvider', ownerWorkspace, 'createProvider'],
  ])('gives %s to the provider whose Call Root starts it', (callPath, provider, path) => {
    expect(operationOwner(callPath, providers)).toEqual({ provider, relativePath: path })
  })

  it.each([
    'coral.providers.github',
    'coral.providers.github.',
    'coral.providers.githubber.repos',
    'coral.functions.github.repos',
  ])('leaves %s without an owner', (callPath) => {
    expect(operationOwner(callPath, providers)).toBeNull()
  })
})
