import { describe, expect, it } from 'vitest'

import { formatFunctionArtifact, parseFunctionArtifact } from './function-artifact.server'

describe('function artifacts', () => {
  it('round-trips individual editor fields through YAML frontmatter', () => {
    const artifact = {
      description: 'Pull requests:\n  ready for review\n',
      name: 'retrieve_pull_requests',
      schema: 'github',
      sql: 'select * from github.pulls(owner => $owner, repo => $repo)',
    }

    expect(parseFunctionArtifact(formatFunctionArtifact(artifact))).toEqual(artifact)
  })

  it('rejects frontmatter values that terminate the SQL comment', () => {
    expect(() =>
      formatFunctionArtifact({
        description: 'Computes */ safely',
        name: 'unsafe_comment',
        schema: 'github',
        sql: 'select 1',
      }),
    ).toThrow("description cannot contain '*/'")
  })

  it('reports malformed frontmatter before opening it in the editor', () => {
    expect(() => parseFunctionArtifact('/*\nname: [broken\n*/\nselect 1')).toThrow()
  })
})
