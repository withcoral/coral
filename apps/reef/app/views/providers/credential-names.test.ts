import { describe, expect, it } from 'vitest'

import { nameFor, unusedFieldName } from './credential-names'

describe('nameFor', () => {
  it.each([
    ['API key', 'api_key'],
    ['  Secret -- token!  ', 'secret_token'],
    // A name starts with a letter.
    ['2FA code', 'field_2fa_code'],
    ['x'.repeat(200), 'x'.repeat(50)],
  ])('turns the label %j into a name Coral accepts', (label, name) => {
    expect(nameFor(label, [])).toBe(name)
  })

  it('gives no name to a label with nothing to keep', () => {
    expect(nameFor('Ключ', [])).toBeNull()
  })

  it('never takes a name another credential holds', () => {
    expect(nameFor('Key', ['key', 'key_2'])).toBe('key_3')
  })
})

describe('unusedFieldName', () => {
  it('numbers past the credentials there are, and skips a number in use', () => {
    expect(unusedFieldName([])).toBe('field_1')
    expect(unusedFieldName(['api_key', 'field_2'])).toBe('field_3')
  })
})
