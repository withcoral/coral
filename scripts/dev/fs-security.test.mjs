import assert from 'node:assert/strict'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import test from 'node:test'
import reefConfig from '../../apps/reef/vite.config.ts'

const require = createRequire(new URL('../../apps/reef/package.json', import.meta.url))
const { createServer, resolveConfig } = await import(pathToFileURL(require.resolve('vite')).href)

test('Reef keeps Vite default protections and blocks .dev credentials and database over HTTP', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'coral-dev-http-'))
  t.after(() => rm(root, { recursive: true, force: true }))
  await mkdir(join(root, '.dev/state'), { recursive: true })
  await writeFile(join(root, 'allowed.txt'), 'public fixture')
  for (const file of ['.dev/setup.env', '.dev/state/coral.db', '.env']) {
    await writeFile(join(root, file), 'private-canary-do-not-serve', { mode: 0o600 })
  }
  const defaults = await resolveConfig({ configFile: false, root, logLevel: 'silent' }, 'serve')
  for (const pattern of defaults.server.fs.deny)
    assert.ok(reefConfig.server.fs.deny.includes(pattern), `keep Vite's ${pattern} protection`)
  const server = await createServer({
    configFile: false,
    root,
    logLevel: 'silent',
    optimizeDeps: { noDiscovery: true },
    server: {
      host: '127.0.0.1',
      port: 0,
      strictPort: true,
      hmr: false,
      fs: { allow: [root], deny: reefConfig.server.fs.deny },
    },
  })
  t.after(() => server.close())
  await server.listen()
  const origin = `http://127.0.0.1:${server.httpServer.address().port}`
  assert.equal(await (await fetch(`${origin}/allowed.txt`)).text(), 'public fixture')
  for (const file of ['.dev/setup.env', '.dev/state/coral.db', '.env']) {
    for (const pathname of [`/${file}`, `/@fs${join(root, file)}`]) {
      const response = await fetch(origin + pathname)
      assert.equal(response.status, 403, pathname)
      assert.ok(!(await response.text()).includes('private-canary-do-not-serve'))
    }
  }
})
