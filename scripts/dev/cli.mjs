import { execFileSync, spawnSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { configure, initialize, load, ports, proxyPlan, urls, validateField } from './config.mjs'
import {
  assertPortsFree,
  commands,
  configureHerdrPanes,
  configureRexPanes,
  supervise,
  waitForReady,
} from './runtime.mjs'

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..')
const helper = join(root, 'scripts/dev/cli.mjs')
const help = `Usage: ./dev setup              Interactive WorkOS + Tailscale setup
       ./dev run                Run Coral and Reef together; Ctrl-C stops both
       ./dev run --herdr        In a fresh Herdr shell: Coral left, Reef right
       ./dev run --rex          In a fresh Rex tab: Coral left, Reef right
       ./dev coral              Run just the backend (rebuilds Rust first)
       ./dev reef               Run just the UI (hot reload)
       ./dev doctor             Check prerequisites, proxy mappings and readiness
       ./dev proxy [--sudo]     Add missing Serve mappings and replace the earlier per-port layout
       ./dev test               Test the development tools without live services

Configuration and disposable data live in this checkout's ignored .dev/.
Setup never overwrites an unrelated apps/reef/.env or conflicting Tailscale mapping.
Linux/macOS are supported; on Windows use WSL. See docs/development.md.`

function output(command, args) {
  try {
    return execFileSync(command, args, {
      cwd: root,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout: 30_000,
    })
  } catch {
    throw new Error(
      `${command} ${args.join(' ')} failed. Check that it is installed and available to this user.`,
    )
  }
}

function jsonCommand(command, args) {
  try {
    return JSON.parse(output(command, args))
  } catch {
    throw new Error(`Could not read ${command} status; check its installation and permissions`)
  }
}

function hostname() {
  const status = jsonCommand('tailscale', ['status', '--json'])
  if (status.BackendState !== 'Running')
    throw new Error('Tailscale is not connected. Sign this machine into your tailnet first.')
  return validateField(
    'TAILNET_HOST',
    (status.Self?.DNSName ?? '').replace(/\.$/, '').toLowerCase(),
  )
}

function preflight() {
  if (!['linux', 'darwin'].includes(process.platform))
    throw new Error('Use Linux/macOS, or run these tools inside WSL')
  const required = readFileSync(join(root, 'apps/reef/.node-version'), 'utf8')
    .trim()
    .split('.')
    .map(Number)
  const actual = process.versions.node.split('.').map(Number)
  if (
    actual[0] !== required[0] ||
    actual[1] < required[1] ||
    (actual[1] === required[1] && actual[2] < required[2])
  ) {
    throw new Error(
      `Use Node ${required.join('.')} or a newer patch/minor in the same major version`,
    )
  }
  output('git', ['--version'])
  output('cargo', ['--version'])
  if (process.platform === 'linux') {
    output('clang', ['--version'])
    output('mold', ['--version'])
  }
  output('npm', ['--version'])
  return hostname()
}

function checkHost(config) {
  if (hostname() !== config.input.TAILNET_HOST)
    throw new Error(
      "This machine has a different Tailscale hostname. Run ./dev setup again rather than copying another machine's credentials.",
    )
}

function planFor(config) {
  checkHost(config)
  return proxyPlan(
    config.input.TAILNET_HOST,
    jsonCommand('tailscale', ['serve', 'status', '--json']),
  )
}

function verifyProxy(config) {
  const plan = planFor(config)
  if (plan.some((mapping) => mapping.state !== 'ready'))
    throw new Error(
      'Tailscale HTTPS mappings are missing or use the earlier per-port layout. Run ./dev proxy (or ./dev proxy --sudo).',
    )
  for (const { authority, surface, funnel } of plan) {
    if (!funnel) continue
    console.warn(
      surface === 'Reef'
        ? `Warning: Reef is public through Funnel at ${authority}. Vite serves development source, not just authenticated pages.`
        : `Note: ${surface} are public through Funnel at ${authority}.`,
    )
  }
}

function applyProxy(config, sudo) {
  const plan = planFor(config) // Validate ALL ports before changing any mapping.
  for (const { commands } of plan) {
    for (const args of commands) {
      console.log(`Configuring: tailscale ${args.join(' ')}`)
      const result = spawnSync(sudo ? 'sudo' : 'tailscale', sudo ? ['tailscale', ...args] : args, {
        stdio: 'inherit',
      })
      if (result.status !== 0)
        throw new Error(
          'Tailscale Serve did not complete. Follow any HTTPS-enablement link it printed, or retry ./dev proxy --sudo if permission was denied.',
        )
    }
  }
  verifyProxy(config)
  console.log(
    'HTTPS mappings are ready. New ports and replaced mappings are tailnet-only; other ports keep their visibility.',
  )
  if (plan.some((mapping) => mapping.state === 'legacy' && mapping.funnel))
    console.log(
      'A replaced mapping used Funnel. To make Coral auth and MCP public again, see "Optional public access with Funnel" in docs/development.md.',
    )
}

function healthUrls(config, names) {
  const selected = []
  // Auth and MCP share one origin, where /readyz reaches MCP; check auth directly.
  if (names.includes('coral'))
    selected.push(`http://127.0.0.1:${ports.auth}/readyz`, `${config.urls.mcp}/readyz`)
  if (names.includes('reef')) selected.push(`${config.urls.web}/readyz`)
  return selected
}

function herdr(args) {
  if (['current', 'layout', 'split'].includes(args[1])) return jsonCommand('herdr', args)
  output('herdr', args)
  return {}
}

function rex(args) {
  if (args.includes('--json')) return jsonCommand('rex', args)
  output('rex', args)
  return {}
}

async function run(names, panes) {
  if (panes === 'herdr' && process.env.HERDR_ENV !== '1')
    throw new Error('Run --herdr only inside a fresh Herdr shell workspace')
  if (panes === 'rex' && !(process.env.REX_SESSION && process.env.REX_BLOCK))
    throw new Error('Run --rex only inside a fresh Rex shell tab')
  preflight()
  const config = load(root)
  if (!existsSync(join(root, 'apps/reef/node_modules/.bin/react-router')))
    throw new Error('Reef dependencies are missing. Run ./dev setup first.')
  verifyProxy(config)
  const selectedPorts = names.includes('coral') ? [ports.grpc, ports.auth, ports.mcp] : []
  if (names.includes('reef')) selectedPorts.push(ports.web)
  await assertPortsFree(selectedPorts)
  const specs = commands(root, config, process.env)
  if (panes === 'herdr') configureHerdrPanes(root, herdr, process.env.HERDR_ENV === '1')
  if (panes === 'rex') configureRexPanes(root, rex, process.env.REX_SESSION, process.env.REX_BLOCK)
  if (panes) {
    // The caller is currently executing this command, so run Coral directly
    // here rather than injecting another command into its busy shell.
    names = ['coral']
  }
  console.log(
    `Starting ${names.join(' + ')}. Ctrl-C stops ${panes ? 'this pane; stop Reef separately in its pane' : 'the launched process groups'}.`,
  )
  process.exitCode = await supervise(
    names.map((name) => specs[name]),
    {
      cwd: root,
      readyUrls: healthUrls(config, panes ? ['coral', 'reef'] : names),
      onReady: () =>
        console.log(
          names.includes('reef') || panes
            ? `Ready: ${config.urls.web}`
            : 'Coral is ready. Start ./dev reef for the web interface.',
        ),
    },
  )
}

async function main(args) {
  const [command, ...options] = args
  if (!command || command === '--help' || command === 'help') {
    console.log(help)
    return
  }
  if (command === 'test' && !options.length) {
    const result = spawnSync(
      process.execPath,
      [
        '--test',
        join(root, 'scripts/dev/config.test.mjs'),
        join(root, 'scripts/dev/runtime.test.mjs'),
        join(root, 'scripts/dev/fs-security.test.mjs'),
        join(root, 'scripts/postgres.test.mjs'),
        join(root, 'scripts/check-migrations.test.mjs'),
        join(root, 'scripts/load.test.mjs'),
      ],
      { cwd: root, stdio: 'inherit' },
    )
    process.exitCode = result.status ?? 1
    return
  }
  if (command === 'run' && !options.length) return run(['coral', 'reef'])
  if (command === 'run' && options.length === 1 && ['--herdr', '--rex'].includes(options[0]))
    return run(['coral', 'reef'], options[0].slice(2))
  if (['coral', 'reef'].includes(command) && !options.length) return run([command])
  if (command === 'proxy' && (!options.length || (options.length === 1 && options[0] === '--sudo')))
    return applyProxy(load(root), options[0] === '--sudo')
  if (command === 'doctor' && !options.length) {
    preflight()
    const config = load(root)
    verifyProxy(config)
    await waitForReady(healthUrls(config, ['coral', 'reef']), AbortSignal.timeout(15_000))
    console.log(`Ready: ${config.urls.web}`)
    return
  }
  // Private helpers used by the wizard; values from stdin are never evaluated
  // as shell code and secrets are never command-line arguments.
  if (command === '_prepare' && !options.length) {
    const host = preflight()
    initialize(root)
    console.log(host)
    return
  }
  if (command === '_validate' && options.length === 1) {
    validateField(options[0], readFileSync(0, 'utf8'))
    return
  }
  if (command === '_urls' && !options.length) {
    const publicUrls = urls(hostname())
    console.log(
      `Web: ${publicUrls.web}\nCoral auth: ${publicUrls.auth}\nMCP: ${publicUrls.mcp}/mcp\nWorkOS Connect callback: ${publicUrls.auth}/auth/oidc/callback`,
    )
    return
  }
  if (command === '_configure' && !options.length) {
    configure(root)
    console.log('Private development configuration generated; keys and existing state preserved.')
    return
  }
  if (command === '_install' && !options.length) {
    const result = spawnSync(
      'npm',
      ['ci', '--prefix', join(root, 'apps/reef'), '--no-audit', '--no-fund'],
      { cwd: root, stdio: 'inherit' },
    )
    if (result.status !== 0)
      throw new Error('npm ci failed; fix the dependency installation and rerun setup')
    const build = spawnSync('cargo', ['build', '--locked', '-p', 'coral', '--bin', 'coral'], {
      cwd: root,
      stdio: 'inherit',
    })
    if (build.status !== 0)
      throw new Error('Coral build failed; fix the native build prerequisites and rerun setup')
    return
  }
  throw new Error(help)
}

if (process.argv[1] && resolve(process.argv[1]) === helper) {
  main(process.argv.slice(2)).catch((error) => {
    // Runtime errors must not dump configuration objects or token responses.
    console.error(`dev: ${error.message}`)
    process.exitCode = 1
  })
}
