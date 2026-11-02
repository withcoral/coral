import { expect, test } from 'vitest'
import { filledSourceUrl, recipeNamespace, type SourceChoice } from './choice'

// Recipes told apart by a source label read different APIs of one service, so a
// workspace can add them side by side without their namespaces colliding.
test.each<[Partial<SourceChoice>, string]>([
  [{ id: 'datadog_mcp', service: 'datadog_v1', kind: 'mcp' }, 'datadog_v1'],
  [{ id: 'datadog_v1', sourceLabel: 'V1' }, 'datadog_v1'],
  [{ id: 'datadog_v2', service: 'datadog_v1', sourceLabel: 'V2' }, 'datadog_v2'],
])('offers %o the namespace %s', (fields, expected) => {
  const recipe = {
    id: '',
    displayName: 'Datadog',
    description: '',
    kind: 'openapi',
    url: 'https://example.test',
    ...fields,
  } as SourceChoice
  expect(recipeNamespace(recipe)).toBe(expected)
})

test.each([
  ['https://gitlab.example.test', 'https://gitlab.example.test/api/v4/mcp'],
  ['https://gitlab.example.test/', 'https://gitlab.example.test/api/v4/mcp'],
  ['https://gitlab.example.test/gitlab/', 'https://gitlab.example.test/gitlab/api/v4/mcp'],
])('appends an MCP path to instance URL %s without duplicate slashes', (answer, expected) => {
  const recipe: SourceChoice = {
    id: 'gitlab',
    displayName: 'GitLab',
    description: '',
    kind: 'mcp',
    url: '{{input.instance}}/api/v4/mcp',
    sourceInputs: [
      {
        name: 'instance',
        label: 'Instance',
        description: '',
        type: 'url',
        defaultValue: '',
        choices: [],
      },
    ],
  }
  expect(filledSourceUrl(recipe, { instance: answer })).toBe(expected)
})
