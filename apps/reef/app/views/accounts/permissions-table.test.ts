import { expect, it } from 'vitest'
import { permissionRows, permissionSummary } from './permissions-table'

it.each([
  ['spaces', 'read write'],
  ['commas', 'read,write'],
  ['commas and spaces', 'read, write'],
])('reads a grant separated by %s', (_, grant) => {
  expect(permissionRows(['write', 'read'], grant)).toEqual([
    { name: 'read', status: 'granted' },
    { name: 'write', status: 'granted' },
  ])
})

it('puts missing permissions first and sorts extras among the granted ones', () => {
  const rows = permissionRows(['read', 'write', 'admin'], 'read,extra')
  expect(rows).toEqual([
    { name: 'admin', status: 'missing' },
    { name: 'write', status: 'missing' },
    { name: 'extra', status: 'extra' },
    { name: 'read', status: 'granted' },
  ])
  expect(permissionSummary(rows)).toBe('The service granted 1 of 3 requested permissions.')
})

it('lists only the request when no grant exists', () => {
  const rows = permissionRows(['write', 'read', 'read'])
  expect(rows).toEqual([{ name: 'read' }, { name: 'write' }])
  expect(permissionSummary(rows)).toBe('Coral asks for 2 permissions.')
})
