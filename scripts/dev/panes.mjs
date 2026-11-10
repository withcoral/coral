import { join } from 'node:path'
import { shellQuote } from './config.mjs'

// A pane host is a terminal app or multiplexer that `./dev run --split`
// can split: Coral stays in the caller's pane on the left, Reef starts on the
// right.
//
// Each host says how to find the caller's pane and how to split, name and type
// into panes. `openPanes` does the rest the same way for every host. Hooks get
// `cli(bin, args, { json })`, which runs a host's binary and returns its
// output, parsed when `json` is set.
//
// The first host whose `detect` matches wins. A multiplexer usually runs inside
// a terminal app and inherits its environment, so list multiplexers first.
//
// To add a host: add an entry here and a row to the table test in
// panes.test.mjs.
export const hosts = {
  herdr: {
    name: 'Herdr',
    where: 'a fresh Herdr shell workspace',
    detect: (env) => (env.HERDR_ENV === '1' ? {} : undefined),
    fresh(cli, ctx) {
      const pane = cli('herdr', ['pane', 'current', '--current'], { json: true }).result?.pane
      const layout = cli('herdr', ['pane', 'layout', '--current'], { json: true }).result?.layout
      ctx.pane = pane?.pane_id
      return Boolean(ctx.pane && !pane.agent && layout?.panes?.length === 1)
    },
    split(cli, ctx, { cwd, title }) {
      const right = cli(
        'herdr',
        ['pane', 'split', '--current', '--direction', 'right', '--cwd', cwd, '--no-focus'],
        { json: true },
      ).result?.pane?.pane_id
      if (right) cli('herdr', ['pane', 'rename', right, title])
      return right
    },
    rename: (cli, ctx, pane, title) => cli('herdr', ['pane', 'rename', pane, title]),
    send: (cli, ctx, pane, command) => cli('herdr', ['pane', 'run', pane, command]),
  },

  rex: {
    name: 'Rex',
    where: 'a fresh Rex shell tab',
    detect: (env) =>
      env.REX_SESSION && env.REX_BLOCK
        ? { session: env.REX_SESSION, pane: env.REX_BLOCK }
        : undefined,
    fresh(cli, ctx) {
      const window = cli('rex', ['session', 'inspect', ctx.session, '--json'], {
        json: true,
      }).windows?.find((window) =>
        window.layers?.some((layer) => layer.blocks?.some((b) => b.block_id === ctx.pane)),
      )
      return window?.layers?.flatMap((layer) => layer.blocks ?? []).length === 1
    },
    split: (cli, ctx, { cwd, title }) =>
      cli(
        'rex',
        [
          'split',
          '-s',
          ctx.session,
          '-b',
          ctx.pane,
          '--split',
          'right',
          '-c',
          cwd,
          '--label',
          title,
          '--focus=false',
          '--json',
        ],
        { json: true },
      ).block_ids?.[0],
    rename: (cli, ctx, pane, title) =>
      cli('rex', ['block', 'rename', '-s', ctx.session, pane, title]),
    send(cli, ctx, pane, command) {
      cli('rex', ['send', '-s', ctx.session, '-b', pane, command])
      cli('rex', ['send-key', '-s', ctx.session, '-b', pane, 'enter'])
    },
  },

  'herdr-app': {
    name: 'the Herdr app',
    where: 'a fresh Herdr app tab',
    detect: (env) => (env.HC_TERMINAL_ID ? { terminal: env.HC_TERMINAL_ID } : undefined),
    fresh(cli, ctx) {
      const tabs = cli('herdr-app', ['ls'], { json: true }).spaces?.flatMap((s) => s.tabs ?? [])
      const tab = tabs?.find((t) => t.terminals?.some((t) => t.terminal === ctx.terminal))
      ctx.pane = tab?.terminals.find((t) => t.terminal === ctx.terminal).pane
      return Boolean(ctx.pane && tab.terminals.length === 1)
    },
    split(cli, ctx, { cwd }) {
      // terminal.open splits the focused pane and takes focus, so pin both ends.
      cli('herdr-app', ['do', 'pane.focus', '--pane', ctx.pane])
      const opened = cli(
        'herdr-app',
        ['do', 'terminal.open', '--place', 'right', '--folder', cwd],
        { json: true },
      )
      cli('herdr-app', ['do', 'pane.focus', '--pane', ctx.pane])
      return opened.terminal
    },
    // The Herdr app names panes after what they run.
    send: (cli, ctx, terminal, command) => cli('hc', ['send', terminal, command]),
  },
}

export const noHostMessage = `./dev run --split found no pane host. Run it in a fresh tab of one of: ${Object.values(
  hosts,
)
  .map((host) => host.name)
  .join(', ')}. Or run ./dev coral and ./dev reef in two terminals.`

// Returns the pane host the caller is in, with its context, or throws.
export function detectHost(env) {
  for (const [key, host] of Object.entries(hosts)) {
    const ctx = host.detect(env)
    if (ctx) return { key, host, ctx }
  }
  throw new Error(noHostMessage)
}

export function openPanes({ host, ctx }, cli, root) {
  if (!host.fresh(cli, ctx))
    throw new Error(
      `Start ./dev run --split from the only pane, an ordinary shell, in ${host.where}`,
    )
  const right = host.split(cli, ctx, { cwd: root, title: 'Reef' })
  if (!right) throw new Error(`${host.name} did not return the new pane ID`)
  host.rename?.(cli, ctx, ctx.pane, 'Coral')
  host.send(cli, ctx, right, `${shellQuote(join(root, 'dev'))} reef`)
}
