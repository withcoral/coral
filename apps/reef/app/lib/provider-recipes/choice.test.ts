import { expect, test } from 'vitest'
import { filledSourceUrl, recipeNamespace, type SourceChoice } from './choice'

// A free-text answer in the query must stay one value, or `&`, `#` and `?` would
// add parameters or cut the URL short.
test.each([
  ['all', 'https://mcp.example.test/v1/mcp?toolsets=all'],
  ['all,apm', 'https://mcp.example.test/v1/mcp?toolsets=all,apm'],
  ['all&omit_tools=x', 'https://mcp.example.test/v1/mcp?toolsets=all%26omit_tools%3Dx'],
  ['all#apm', 'https://mcp.example.test/v1/mcp?toolsets=all%23apm'],
  ['all apm', 'https://mcp.example.test/v1/mcp?toolsets=all%20apm'],
])('keeps query answer %j one value', (answer, expected) => {
  const recipe: SourceChoice = {
    id: 'example_mcp',
    displayName: 'Example',
    description: '',
    kind: 'mcp',
    url: 'https://{{input.host}}/v1/mcp?toolsets={{input.toolsets}}',
    sourceInputs: [
      {
        name: 'host',
        label: 'Host',
        description: '',
        type: 'hostname',
        defaultValue: '',
        choices: [],
      },
      {
        name: 'toolsets',
        label: 'Toolsets',
        description: '',
        type: 'string',
        defaultValue: 'all',
        choices: [],
      },
    ],
  }
  expect(filledSourceUrl(recipe, { host: 'mcp.example.test', toolsets: answer })).toBe(expected)
})

// An MCP recipe offers its service's namespace, as an API recipe without a
// button label does: a workspace adds one or the other. Labelled recipes read
// different APIs of the service, so V1 and V2 can sit side by side.
test.each<[Partial<SourceChoice>, string]>([
  [{ id: 'datadog_mcp', service: 'datadog_v1', kind: 'mcp' }, 'datadog_v1'],
  [{ id: 'datadog_v1', buttonLabel: 'V1' }, 'datadog_v1'],
  [{ id: 'datadog_v2', service: 'datadog_v1', buttonLabel: 'V2' }, 'datadog_v2'],
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
