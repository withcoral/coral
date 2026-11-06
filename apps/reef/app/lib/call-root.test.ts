import { describe, expect, it } from 'vitest'
import { privateToolPath, toolOwner } from './call-root'

const github = { callRoot: 'coral.providers.github' }
const builtin = { callRoot: 'coral' }
const ownerWorkspace = { callRoot: 'coral.providers.workspace' }
const providers = [builtin, github, ownerWorkspace]

describe('toolOwner', () => {
  it.each([
    ['coral.providers.github.repos.get', github, 'repos.get'],
    ['coral.addProvider', builtin, 'addProvider'],
    // A retained Run keeps the Built-in Provider's old Call Root.
    ['coral.workspace.addProvider', builtin, 'addProvider'],
    ['coral.providers.workspace.addProvider', ownerWorkspace, 'addProvider'],
  ])('gives %s to the provider whose Call Root owns it', (callPath, provider, path) => {
    expect(toolOwner(callPath, providers)).toEqual({ provider, relativePath: path })
  })

  it.each([
    'coral.providers.github',
    'coral.providers.github.',
    'coral.providers.githubber.repos',
    'coral.private.github.repos',
    'coral.functions.github.repos',
    'coral.search.paths',
    'coral.emit',
    'coral.',
  ])('leaves %s without an owner', (callPath) => {
    expect(toolOwner(callPath, providers)).toBeNull()
  })
})

describe('privateToolPath', () => {
  it.each([
    ['coral.private.github.repos', 'github.repos'],
    // A retained Run keeps the old root of Saved Functions.
    ['coral.functions.github.repos', 'github.repos'],
    ['coral.private.', null],
    ['coral.providers.github.repos', null],
    ['coral.search', null],
  ])('reads %s as %s', (callPath, path) => {
    expect(privateToolPath(callPath)).toBe(path)
  })
})
