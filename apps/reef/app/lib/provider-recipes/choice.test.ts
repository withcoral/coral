import { expect, test } from 'vitest'
import { filledSourceUrl, type SourceChoice } from './choice'

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
