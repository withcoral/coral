import { describe, expect, it } from 'vitest'

import { settle } from './policy-mutations'
import { selectionKey } from './selection'

const a = { provider: 'prov-a', tool: 'providers/prov-a/tools/one' }
const b = { provider: 'prov-b', tool: 'providers/prov-b/tools/two' }

describe('settle', () => {
  // Catches: a mixed result clearing the whole selection, which loses the
  // failed tools the reader needs to retry.
  it('confirms only the tools Coral updated', () => {
    const outcome = settle([a, b], {
      error: null,
      credential: null,
      message: 'Tool policy saved for 1 tool. 1 tool not saved: denied',
      tools: [
        { ...a, status: 'updated' },
        { ...b, status: 'failed', reason: 'denied' },
      ],
    })
    expect(outcome.confirmed).toEqual([selectionKey(a.provider, a.tool)])
    expect([...outcome.failures]).toEqual([
      [selectionKey(b.provider, b.tool), { ...b, status: 'failed', reason: 'denied' }],
    ])
  })

  it('confirms every tool when the shared result succeeded', () => {
    const outcome = settle([a, b], {
      error: null,
      credential: null,
      message: 'Tool policy saved for 2 tools.',
    })
    expect(outcome.confirmed).toHaveLength(2)
    expect(outcome.failures.size).toBe(0)
  })

  it('fails every tool when Coral refused the request', () => {
    const refused = settle([a, b], { error: 'Coral refused.', credential: null })
    expect(refused.confirmed).toEqual([])
    expect([...refused.failures.values()]).toEqual([
      { ...a, status: 'failed', reason: 'Coral refused.' },
      { ...b, status: 'failed', reason: 'Coral refused.' },
    ])
  })

  // Catches: a lost response reported as a failure, or worse as a success,
  // when the policy may or may not have been saved.
  it('leaves every tool unconfirmed when no response came back', () => {
    const missing = settle([a, b], undefined)
    expect(missing.confirmed).toEqual([])
    const failures = [...missing.failures.values()]
    expect(failures.map((f) => f.status)).toEqual(['unconfirmed', 'unconfirmed'])
    expect(failures[0]?.reason).toContain('lost')
  })
})
