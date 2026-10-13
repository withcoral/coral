import { describe, expect, it } from 'vitest'
import { operationOwner } from './call-root'

const github = { callRoot: 'coral.providers.github' }
const admin = { callRoot: 'coral.admin' }
const ownerAdmin = { callRoot: 'coral.providers.admin' }
const providers = [github, admin, ownerAdmin]

describe('operationOwner', () => {
  it.each([
    ['coral.providers.github.repos.get', github, 'repos.get'],
    ['coral.admin.createProvider', admin, 'createProvider'],
    ['coral.providers.admin.createProvider', ownerAdmin, 'createProvider'],
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
