import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { join } from 'node:path'
import test from 'node:test'
import { detectHost, hosts, noHostMessage, openPanes } from './panes.mjs'

const root = '/repo with spaces'
const reef = "'/repo with spaces/dev' reef"

// Each row fakes one host. `answer` replies to the host's CLI for a tab
// holding `panes` panes, the first the caller's (none: the host can't find
// the caller), running `agent` when set. `opens`
// lists every call a successful open makes, in order.
const rows = [
  {
    name: 'herdr',
    env: { HERDR_ENV: '1' },
    agentRefused: true,
    answer: (bin, args, { panes, agent }) => {
      if (args[1] === 'current') return { result: { pane: { pane_id: 'L', agent } } }
      if (args[1] === 'layout') return { result: { layout: { panes: Array(panes).fill({}) } } }
      if (args[1] === 'split') return { result: { pane: { pane_id: 'R' } } }
      return ''
    },
    opens: [
      ['herdr', 'pane', 'current', '--current'],
      ['herdr', 'pane', 'layout', '--current'],
      ['herdr', 'pane', 'split', '--current', '--direction', 'right', '--cwd', root, '--no-focus'],
      ['herdr', 'pane', 'rename', 'R', 'Reef'],
      ['herdr', 'pane', 'rename', 'L', 'Coral'],
      ['herdr', 'pane', 'run', 'R', reef],
    ],
  },
  {
    name: 'rex',
    env: { REX_SESSION: 's:1', REX_BLOCK: 'L' },
    answer: (bin, args, { panes }) => {
      if (args[0] === 'session')
        return {
          windows: [
            { layers: [{ blocks: [{ block_id: 'other' }] }] },
            {
              layers: [
                {
                  blocks: Array.from({ length: panes }, (_, i) => ({ block_id: i ? 'X' : 'L' })),
                },
              ],
            },
          ],
        }
      if (args[0] === 'split') return { block_ids: ['R'] }
      return ''
    },
    opens: [
      ['rex', 'session', 'inspect', 's:1', '--json'],
      [
        'rex',
        'split',
        '-s',
        's:1',
        '-b',
        'L',
        '--split',
        'right',
        '-c',
        root,
        '--focus=false',
        '--json',
      ],
      ['rex', 'block', 'rename', '-s', 's:1', 'R', 'Reef'],
      ['rex', 'block', 'rename', '-s', 's:1', 'L', 'Coral'],
      ['rex', 'send', '-s', 's:1', '-b', 'R', reef],
      ['rex', 'send-key', '-s', 's:1', '-b', 'R', 'enter'],
    ],
  },
  {
    name: 'herdr-app',
    env: { HC_TERMINAL_ID: 't1' },
    agentRefused: true,
    answer: (bin, args, { panes, agent }) => {
      if (args[0] === 'ls')
        return {
          spaces: [
            { tabs: [{ terminals: [{ terminal: 't9', pane: 'other' }] }] },
            {
              tabs: [
                {
                  terminals: Array.from({ length: panes }, (_, i) =>
                    i ? { terminal: 't2', pane: 'X' } : { terminal: 't1', pane: 'L', app: agent },
                  ),
                },
              ],
            },
          ],
        }
      if (args[1] === 'terminal.open') return { pane: 'R', terminal: 'tR' }
      return ''
    },
    opens: [
      ['herdr-app', 'ls'],
      ['herdr-app', 'do', 'pane.focus', '--pane', 'L'],
      ['herdr-app', 'do', 'terminal.open', '--place', 'right', '--folder', root],
      ['herdr-app', 'do', 'pane.focus', '--pane', 'L'],
      ['hc', 'send', 'tR', reef],
    ],
  },
]

function open(row, tab) {
  const calls = []
  const cli = (bin, args) => {
    calls.push([bin, ...args])
    return row.answer(bin, args, tab)
  }
  try {
    openPanes(detectHost(row.env), cli, root)
    return { calls }
  } catch (error) {
    return { calls, error }
  }
}

test('no pane host: names every host and the two-terminal fallback', () => {
  assert.throws(() => detectHost({}), { message: noHostMessage })
  for (const host of Object.values(hosts)) assert.ok(noHostMessage.includes(host.name))
  assert.match(noHostMessage, /\.\/dev coral and \.\/dev reef/)
})

test('a multiplexer inside a terminal app wins', () => {
  const env = Object.assign({}, ...rows.map((row) => row.env))
  assert.equal(detectHost(env).key, 'herdr')
})

test('every host has a test row', () => {
  assert.deepEqual(
    rows.map((row) => row.name),
    Object.keys(hosts),
  )
})

for (const row of rows) {
  test(`${row.name}: detected from its environment`, () => {
    assert.equal(detectHost(row.env).key, row.name)
  })

  test(`${row.name}: refuses a tab that is not fresh, before splitting`, () => {
    const tabs = [{ panes: 2 }, { panes: 0 }]
    if (row.agentRefused) tabs.push({ panes: 1, agent: 'pi' })
    for (const tab of tabs) {
      const { calls, error } = open(row, tab)
      assert.match(error?.message ?? '', /from the only pane/, JSON.stringify(tab))
      assert.deepEqual(calls, row.opens.slice(0, calls.length))
      assert.ok(calls.length < row.opens.length)
    }
  })

  test(`${row.name}: splits right at the root, keeps focus and starts Reef`, () => {
    const { calls, error } = open(row, { panes: 1 })
    assert.equal(error, undefined)
    assert.deepEqual(calls, row.opens)
  })
}

test('./dev run --split outside any pane host says what to do before any other check', () => {
  const env = { ...process.env }
  for (const row of rows) for (const key of Object.keys(row.env)) delete env[key]
  const result = spawnSync(
    process.execPath,
    [join(import.meta.dirname, 'cli.mjs'), 'run', '--split'],
    { encoding: 'utf8', env },
  )
  assert.ok(result.stderr.includes(noHostMessage), result.stderr)
  assert.equal(result.status, 1)
})
