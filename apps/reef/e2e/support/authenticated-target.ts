import { generateKeyPairSync, randomBytes } from 'node:crypto'
import { writeFile } from 'node:fs/promises'
import { createServer } from 'node:net'
import { join } from 'node:path'

import { startIdentityProvider, type AuthenticationTarget } from './oidc-fixture.ts'

// OAuth origins must be fixed before Coral starts. Reserve ephemeral ports,
// release immediately before the owning child binds, and let a collision fail
// startup. Never attach to a server that happens to occupy a port.
export async function reservePort() {
  const server = createServer()
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const port = (server.address() as { port: number }).port
  let closing: Promise<void> | undefined
  return {
    port,
    url: `http://127.0.0.1:${port}`,
    release() {
      closing ??= new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      )
      return closing
    },
  }
}

export async function prepareAuthenticatedTarget(stateDir: string) {
  const reservations: Awaited<ReturnType<typeof reservePort>>[] = []
  let provider: Awaited<ReturnType<typeof startIdentityProvider>> | undefined
  const close = async () => {
    const outcomes = await Promise.allSettled([
      ...reservations.map((reservation) => reservation.release()),
      ...(provider ? [provider.close()] : []),
    ])
    const failures = outcomes.filter((outcome) => outcome.status === 'rejected')
    if (failures.length)
      throw new AggregateError(
        failures.map((failure) => failure.reason),
        'Authenticated fixture cleanup failed',
      )
  }
  try {
    const auth = await reservePort()
    reservations.push(auth)
    const web = await reservePort()
    reservations.push(web)
    const mcp = await reservePort()
    reservations.push(mcp)
    provider = await startIdentityProvider(`${auth.url}/auth/oidc/callback`, web.url)
    const { privateKey } = generateKeyPairSync('ec', {
      namedCurve: 'prime256v1',
      privateKeyEncoding: { format: 'der', type: 'pkcs8' },
      publicKeyEncoding: { format: 'pem', type: 'spki' },
    })
    await writeFile(join(stateDir, 'session.pk8'), privateKey, { mode: 0o600 })
    const q = JSON.stringify
    const config = `
[listeners.grpc]
bind = '127.0.0.1:0'
[listeners.auth]
bind = '127.0.0.1:${auth.port}'
public_url = ${q(auth.url)}
[listeners.mcp]
bind = '127.0.0.1:${mcp.port}'
public_url = ${q(mcp.url)}
[listeners.web]
public_url = ${q(web.url)}
[auth]
# Loopback web navigation does not confer HTTPS metadata client trust.
# OAuth resources and trusted clients remain explicit.
allowed_audiences = [${q(web.url)}, ${q(provider.origin)}]
trusted_clients = [${q(`${web.url}/.well-known/oauth-client`)}, ${q(`${provider.origin}/.well-known/oauth-client`)}]
[auth.session]
signing_key_file = 'session.pk8'
access_token_ttl_seconds = 3600
[auth.provider]
issuer = ${q(provider.origin)}
client_id = ${q(provider.clientId)}
client_secret_env = 'E2E_OIDC_CLIENT_SECRET'
bootstrap_owner_subject = ${q(provider.owner.subject)}
`
    const authentication: AuthenticationTarget = {
      issuerURL: auth.url,
      oidcURL: provider.origin,
      owner: provider.owner,
    }
    return {
      config,
      authentication,
      coralEnv: { E2E_OIDC_CLIENT_SECRET: provider.clientSecret },
      webEnv: {
        PORT: String(web.port),
        CORAL_WEB_AUTH_MODE: 'required',
        CORAL_WEB_PUBLIC_URL: web.url,
        CORAL_WEB_AUTH_ISSUER: auth.url,
        CORAL_WEB_SESSION_SECRET: randomBytes(32).toString('hex'),
      },
      releaseCoralPorts: () => Promise.all([auth.release(), mcp.release()]),
      releaseWebPort: () => web.release(),
      close,
    }
  } catch (error) {
    try {
      await close()
    } catch (cleanup) {
      console.warn(cleanup)
    }
    throw error
  }
}
