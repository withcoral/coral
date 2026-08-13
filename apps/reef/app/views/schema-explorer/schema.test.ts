import { describe, expect, it } from 'vitest'

import { schemaTreeChildrenId } from './schema'

describe('schemaTreeChildrenId', () => {
  it('encodes catalog metadata into an unambiguous HTML id', () => {
    const id = schemaTreeChildrenId('catalogSchema', 'GitHub / Enterprise', 'Issue Tracking')

    expect(id).not.toMatch(/[\s/]/)
    expect(id).not.toBe(
      schemaTreeChildrenId('catalogSchema', 'GitHub', 'Enterprise/Issue Tracking'),
    )
  })
})
