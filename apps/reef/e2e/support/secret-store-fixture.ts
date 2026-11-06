import { randomBytes } from 'node:crypto'
import { createServer } from 'node:http'
import type { FixtureAddress } from './config.ts'
import { listen } from './provider-fixtures.ts'

/** What the fixture holds, for a scenario to name and to expect upstream. */
export const SECRET_STORE = {
  name: 'reel-store',
  path: 'coral/forecast',
  key: 'token',
  value: 'reel-store-value-canary',
} as const

/**
 * A secret store speaking the part of the KV version 2 and AppRole API Coral uses: sign in,
 * read an entry. Every Coral in the stack shares it, so it holds fixed entries.
 */
export async function startSecretStoreFixture(address: FixtureAddress) {
  const roleId = 'reel-role'
  const secretId = randomBytes(16).toString('hex')
  const token = randomBytes(16).toString('hex')
  const server = createServer((request, response) => {
    const answer = (status: number, body: unknown) =>
      response.writeHead(status, { 'content-type': 'application/json' }).end(JSON.stringify(body))
    const path = new URL(request.url ?? '/', 'http://store').pathname
    if (request.method === 'POST' && path === '/v1/auth/approle/login') {
      let text = ''
      request.on('data', (chunk) => (text += chunk))
      request.on('end', () => {
        const login = JSON.parse(text) as { role_id?: string; secret_id?: string }
        if (login.role_id !== roleId || login.secret_id !== secretId)
          return answer(400, { errors: ['invalid role or secret ID'] })
        answer(200, { auth: { client_token: token, lease_duration: 3600, renewable: true } })
      })
      return
    }
    if (request.headers['x-vault-token'] !== token) return answer(403, { errors: ['denied'] })
    if (request.method === 'GET' && path === `/v1/secret/data/${SECRET_STORE.path}`)
      return answer(200, {
        data: { data: { [SECRET_STORE.key]: SECRET_STORE.value }, metadata: { version: 1 } },
      })
    answer(404, { errors: [] })
  })
  const closers: (() => void | Promise<void>)[] = []
  const { origin } = await listen(
    { onTestFinished: (close: () => void | Promise<void>) => closers.push(close) } as never,
    server,
    address,
  )
  return {
    /** The `[[secret_stores]]` table that reaches this fixture. */
    config: (secretIdEnv: string) => `
[[secret_stores]]
name = ${JSON.stringify(SECRET_STORE.name)}
kind = "kv-v2"
address = ${JSON.stringify(origin)}
mount = "secret"
[secret_stores.auth]
method = "approle"
role_id = ${JSON.stringify(roleId)}
secret_id_env = ${JSON.stringify(secretIdEnv)}
`,
    secretId,
    close: () => Promise.all(closers.map((close) => close())),
  }
}
