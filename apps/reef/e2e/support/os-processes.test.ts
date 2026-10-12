import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { createServer, type AddressInfo, type Server } from 'node:net'
import { test } from 'node:test'

import { descendantProcesses, tcpListeners } from './os-processes.ts'

test('descendants include grandchildren with their command lines, and never the root', async (t) => {
  const marker = `reel-descendant-${crypto.randomUUID()}`
  // A child that starts a grandchild, then reports the grandchild's pid.
  const child = spawn(
    process.execPath,
    [
      '-e',
      `const { spawn } = require('node:child_process')
       const grandchild = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)', '${marker}'], { stdio: 'ignore' })
       console.log(grandchild.pid)
       setInterval(() => {}, 1000)`,
    ],
    { stdio: ['ignore', 'pipe', 'inherit'] },
  )
  const [line] = (await once(child.stdout!, 'data')) as [Buffer]
  const grandchild = Number(line.toString().trim())
  t.after(() => {
    process.kill(grandchild)
    child.kill()
  })

  const descendants = await descendantProcesses(process.pid)
  const pids = descendants.map((row) => row.pid)
  assert.ok(pids.includes(child.pid!), 'the child')
  assert.ok(pids.includes(grandchild), 'the grandchild')
  assert.ok(!pids.includes(process.pid), 'not the root')
  assert.match(descendants.find((row) => row.pid === grandchild)!.args, new RegExp(marker))
})

const byPort = (a: { port: number }, b: { port: number }) => a.port - b.port

test('listeners report each listening socket of the given processes with its address', async (t) => {
  const servers: Server[] = []
  t.after(() => servers.forEach((server) => server.close()))
  const expected = []
  // The address the OS prints for each bind; IPv6 only where the host has it.
  for (const host of ['127.0.0.1', '0.0.0.0', '::1']) {
    const server = createServer()
    servers.push(server)
    server.listen(0, host)
    try {
      // Rejects when the server emits 'error' instead.
      await once(server, 'listening')
    } catch {
      t.diagnostic(`This host cannot listen on ${host}`)
      continue
    }
    expected.push({ pid: process.pid, address: host, port: (server.address() as AddressInfo).port })
  }

  const ports = expected.map(({ port }) => port)
  const ours = (await tcpListeners([process.pid])).filter(({ port }) => ports.includes(port))
  assert.deepEqual(ours.toSorted(byPort), expected.toSorted(byPort))
  assert.deepEqual(await tcpListeners([]), [])
})
