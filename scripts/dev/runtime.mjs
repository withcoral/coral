import { spawn } from 'node:child_process'
import { createServer } from 'node:net'
import { setTimeout as delay } from 'node:timers/promises'
import { join } from 'node:path'
import { ports, shellQuote } from './config.mjs'

export function configureHerdrPanes(root, invoke, insideHerdr) {
  if (!insideHerdr) throw new Error('Run --herdr only inside a fresh Herdr shell workspace')
  const current = invoke(['pane', 'current', '--current']).result?.pane
  const layout = invoke(['pane', 'layout', '--current']).result?.layout
  if (!current?.pane_id || current.agent || layout?.panes?.length !== 1)
    throw new Error('Start ./dev run --herdr from one ordinary shell pane in a fresh workspace')
  const split = invoke([
    'pane',
    'split',
    '--current',
    '--direction',
    'right',
    '--cwd',
    root,
    '--no-focus',
  ])
  const right = split.result?.pane?.pane_id
  if (!right) throw new Error('Herdr did not return the new pane ID')
  invoke(['pane', 'rename', current.pane_id, 'Coral'])
  invoke(['pane', 'rename', right, 'Reef'])
  invoke(['pane', 'run', right, `${shellQuote(join(root, 'dev'))} reef`])
}

export function configureRexPanes(root, invoke, session, block) {
  if (!session || !block) throw new Error('Run --rex only inside a fresh Rex shell tab')
  const window = invoke(['session', 'inspect', session, '--json']).windows?.find((window) =>
    window.layers?.some((layer) => layer.blocks?.some((b) => b.block_id === block)),
  )
  const blocks = window?.layers?.flatMap((layer) => layer.blocks ?? []) ?? []
  if (blocks.length !== 1)
    throw new Error('Start ./dev run --rex from the only pane in a fresh tab')
  const split = invoke([
    'split',
    '-s',
    session,
    '-b',
    block,
    '--split',
    'right',
    '-c',
    root,
    '--label',
    'Reef',
    '--focus=false',
    '--json',
  ])
  const right = split.block_ids?.[0]
  if (!right) throw new Error('Rex did not return the new pane ID')
  invoke(['block', 'rename', '-s', session, block, 'Coral'])
  invoke(['send', '-s', session, '-b', right, `${shellQuote(join(root, 'dev'))} reef`])
  invoke(['send-key', '-s', session, '-b', right, 'enter'])
}

export function commands(root, config, ambient) {
  return {
    coral: {
      command: 'cargo',
      args: ['run', '--locked', '-p', 'coral', '--bin', 'coral'],
      env: {
        ...ambient,
        CORAL_CONFIG_DIR: join(root, '.dev/state'),
        WORKOS_CONNECT_CLIENT_SECRET: config.input.WORKOS_CONNECT_CLIENT_SECRET,
      },
    },
    reef: {
      command: 'npm',
      args: [
        'run',
        'dev',
        '--prefix',
        join(root, 'apps/reef'),
        '--',
        '--host',
        '127.0.0.1',
        '--port',
        String(ports.web),
        '--strictPort',
      ],
      env: {
        ...ambient,
        ...config.webEnv,
        NODE_ENV: 'development',
        __VITE_ADDITIONAL_SERVER_ALLOWED_HOSTS: config.input.TAILNET_HOST,
      },
    },
  }
}

export async function assertPortsFree(selectedPorts) {
  for (const port of selectedPorts) {
    await new Promise((resolve, reject) => {
      const probe = createServer()
      probe.once('error', () =>
        reject(
          new Error(
            `Loopback port ${port} is already occupied. Stop the existing process yourself; nothing was killed.`,
          ),
        ),
      )
      probe.listen({ host: '127.0.0.1', port, exclusive: true }, () => probe.close(resolve))
    })
  }
}

export async function waitForReady(
  urls,
  signal,
  { timeout = 300_000, interval = 500, requestTimeout = 2000, request = fetch } = {},
) {
  const deadline = Date.now() + timeout
  while (!signal.aborted && Date.now() < deadline) {
    const statuses = await Promise.all(
      urls.map(async (url) => {
        try {
          const response = await request(url, {
            redirect: 'error',
            signal: AbortSignal.any([signal, AbortSignal.timeout(requestTimeout)]),
          })
          await response.body?.cancel()
          return response.ok
        } catch {
          return false
        }
      }),
    )
    if (signal.aborted) break
    if (statuses.every(Boolean)) return
    await delay(interval, undefined, { signal }).catch(() => {})
  }
  signal.throwIfAborted()
  throw new Error(
    'Development listeners did not become ready before the startup deadline. Inspect the process output and run ./dev doctor.',
  )
}

// Each server gets its own process group. Stop descendants too (npm/Vite and
// Cargo/compiler/runtime), but never signal another developer's existing group.
export async function supervise(
  specs,
  { cwd, readyUrls = [], onReady = () => {}, signals = process, grace = 5000 } = {},
) {
  const children = []
  const controller = new AbortController()
  let stopping = false
  let exitCode = 0
  let killTimer
  let stoppedAt
  const signalGroup = (child, signal) => {
    if (!child.pid) return
    try {
      process.kill(-child.pid, signal)
    } catch (error) {
      if (error.code !== 'ESRCH') throw error
    }
  }
  const stop = (code, signal = 'SIGTERM') => {
    if (stopping) return
    stopping = true
    stoppedAt = Date.now()
    exitCode = code
    controller.abort()
    for (const child of children) signalGroup(child, signal)
    killTimer = setTimeout(() => {
      for (const child of children) signalGroup(child, 'SIGKILL')
    }, grace)
    killTimer.unref()
  }
  const handlers = {
    SIGINT: () => stop(130, 'SIGINT'),
    SIGTERM: () => stop(143),
    SIGHUP: () => stop(129),
  }
  for (const [signal, handler] of Object.entries(handlers)) signals.on(signal, handler)
  try {
    const closed = specs.map(
      ({ command, args, env }) =>
        new Promise((resolve) => {
          const child = spawn(command, args, {
            cwd,
            env,
            detached: true,
            stdio: ['ignore', 'inherit', 'inherit'],
          })
          children.push(child)
          child.once('error', () => {
            console.error(`Could not start ${command}. Check ./dev doctor and your prerequisites.`)
            stop(1)
          })
          child.once('close', (code) => {
            if (!stopping) {
              console.error(`${command} exited; stopping the development stack.`)
              stop(code || 1)
            }
            resolve()
          })
        }),
    )
    const readiness = waitForReady(readyUrls, controller.signal)
      .then(() => {
        if (!stopping) onReady()
      })
      .catch((error) => {
        if (!stopping) {
          console.error(error.message)
          stop(1)
        }
      })
    await Promise.all(closed)
    controller.abort()
    await readiness
    // A parent can exit before its descendants. Keep the supervisor alive
    // through the grace period rather than abandoning an unref'ed kill timer.
    const groupAlive = (child) => {
      if (!child.pid) return false
      try {
        process.kill(-child.pid, 0)
        return true
      } catch (error) {
        if (error.code === 'ESRCH') return false
        throw error
      }
    }
    while (children.some(groupAlive) && Date.now() - stoppedAt < grace) await delay(25)
    for (const child of children) if (groupAlive(child)) signalGroup(child, 'SIGKILL')
    return exitCode
  } finally {
    clearTimeout(killTimer)
    for (const [signal, handler] of Object.entries(handlers))
      signals.removeListener(signal, handler)
  }
}
