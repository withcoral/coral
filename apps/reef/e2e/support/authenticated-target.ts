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

/**
 * Prepares sign-in for the Coral Instances whose state roots are `stateDirs`.
 * Each gets the same session key and configuration; only its auth listener's
 * port differs. Behind a proxy, `front` names the public auth and MCP origins
 * every instance advertises, and each binds MCP to a free port of its own.
 */
export async function prepareAuthenticatedTarget(
  stateDirs: string[],
  front?: { authURL: string; mcpURL: string },
) {
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
  const reserve = async () => {
    const reservation = await reservePort()
    reservations.push(reservation)
    return reservation
  }
  try {
    const auth: Awaited<ReturnType<typeof reservePort>>[] = []
    while (auth.length < stateDirs.length) auth.push(await reserve())
    const web = await reserve()
    const mcp = front ? undefined : await reserve()
    const authURL = front?.authURL ?? auth[0].url
    const mcpURL = front?.mcpURL ?? mcp!.url
    const idp = (provider = await startIdentityProvider(`${authURL}/auth/oidc/callback`, web.url))
    const { privateKey } = generateKeyPairSync('ec', {
      namedCurve: 'prime256v1',
      privateKeyEncoding: { format: 'der', type: 'pkcs8' },
      publicKeyEncoding: { format: 'pem', type: 'spki' },
    })
    for (const stateDir of stateDirs)
      await writeFile(join(stateDir, 'session.pk8'), privateKey, { mode: 0o600 })
    const q = JSON.stringify
    const config = (index: number) => `
[listeners.grpc]
bind = '127.0.0.1:0'
[listeners.auth]
bind = '127.0.0.1:${auth[index].port}'
public_url = ${q(authURL)}
[listeners.mcp]
bind = '127.0.0.1:${mcp?.port ?? 0}'
public_url = ${q(mcpURL)}
[listeners.web]
public_url = ${q(web.url)}
[auth]
# Loopback web navigation does not confer HTTPS metadata client trust.
# OAuth resources and trusted clients remain explicit.
allowed_audiences = [${q(web.url)}, ${q(idp.origin)}]
trusted_clients = [${q(`${web.url}/.well-known/oauth-client`)}, ${q(`${idp.origin}/.well-known/oauth-client`)}]
[auth.session]
signing_key_file = 'session.pk8'
access_token_ttl_seconds = 3600
[auth.provider]
issuer = ${q(idp.origin)}
client_id = ${q(idp.clientId)}
client_secret_env = 'E2E_OIDC_CLIENT_SECRET'
bootstrap_owner_subject = ${q(idp.owner.subject)}
`
    const authentication: AuthenticationTarget = {
      issuerURL: authURL,
      oidcURL: idp.origin,
      owner: idp.owner,
    }
    return {
      config,
      authentication,
      coralEnv: { E2E_OIDC_CLIENT_SECRET: idp.clientSecret },
      webEnv: {
        PORT: String(web.port),
        CORAL_WEB_AUTH_MODE: 'required',
        CORAL_WEB_PUBLIC_URL: web.url,
        CORAL_WEB_AUTH_ISSUER: authURL,
        CORAL_WEB_SESSION_SECRET: randomBytes(32).toString('hex'),
      },
      /** Each instance's own auth listener origin, which the proxy forwards to. */
      authOrigins: auth.map((reservation) => reservation.url),
      releaseCoralPorts: (index: number) =>
        Promise.all([auth[index].release(), ...(mcp ? [mcp.release()] : [])]),
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
