import { describe, expect, it } from 'vitest'
import { toolOwner } from './call-root'

const github = { callRoot: 'coral.providers.github' }
const builtin = { callRoot: 'coral.workspace' }
const ownerWorkspace = { callRoot: 'coral.providers.workspace' }
const providers = [github, builtin, ownerWorkspace]

describe('toolOwner', () => {
  it.each([
    ['coral.providers.github.repos.get', github, 'repos.get'],
    ['coral.workspace.addProvider', builtin, 'addProvider'],
    ['coral.providers.workspace.addProvider', ownerWorkspace, 'addProvider'],
  ])('gives %s to the provider whose Call Root starts it', (callPath, provider, path) => {
    expect(toolOwner(callPath, providers)).toEqual({ provider, relativePath: path })
  })

  it.each([
    'coral.providers.github',
    'coral.providers.github.',
    'coral.providers.githubber.repos',
    'coral.functions.github.repos',
  ])('leaves %s without an owner', (callPath) => {
    expect(toolOwner(callPath, providers)).toBeNull()
  })
})
