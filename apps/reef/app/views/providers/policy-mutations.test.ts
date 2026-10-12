import { describe, expect, it } from 'vitest'

import { settle } from './policy-mutations'
import { selectionKey } from './selection'

const a = { provider: 'prov-a', operation: 'providers/prov-a/operations/one' }
const b = { provider: 'prov-b', operation: 'providers/prov-b/operations/two' }

describe('settle', () => {
  // Catches: a mixed result clearing the whole selection, which loses the
  // failed operations the reader needs to retry.
  it('confirms only the operations Coral updated', () => {
    const outcome = settle([a, b], {
      error: null,
      credential: null,
      message: 'Operation policy saved for 1 operation. 1 operation not saved: denied',
      operations: [
        { ...a, status: 'updated' },
        { ...b, status: 'failed', reason: 'denied' },
      ],
    })
    expect(outcome.confirmed).toEqual([selectionKey(a.provider, a.operation)])
    expect([...outcome.failures]).toEqual([
      [selectionKey(b.provider, b.operation), { ...b, status: 'failed', reason: 'denied' }],
    ])
  })

  it('confirms every operation when the shared result succeeded', () => {
    const outcome = settle([a, b], {
      error: null,
      credential: null,
      message: 'Operation policy saved for 2 operations.',
    })
    expect(outcome.confirmed).toHaveLength(2)
    expect(outcome.failures.size).toBe(0)
  })

  it('fails every operation when Coral refused the request', () => {
    const refused = settle([a, b], { error: 'Coral refused.', credential: null })
    expect(refused.confirmed).toEqual([])
    expect([...refused.failures.values()]).toEqual([
      { ...a, status: 'failed', reason: 'Coral refused.' },
      { ...b, status: 'failed', reason: 'Coral refused.' },
    ])
  })

  // Catches: a lost response reported as a failure, or worse as a success,
  // when the policy may or may not have been saved.
  it('leaves every operation unconfirmed when no response came back', () => {
    const missing = settle([a, b], undefined)
    expect(missing.confirmed).toEqual([])
    const failures = [...missing.failures.values()]
    expect(failures.map((f) => f.status)).toEqual(['unconfirmed', 'unconfirmed'])
    expect(failures[0]?.reason).toContain('lost')
  })
})
