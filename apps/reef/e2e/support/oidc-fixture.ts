// A test-only identity provider built on the maintained oidc-provider protocol
// implementation. No Coral code, database access, or Coral token signing here.
import { generateKeyPairSync, randomBytes, randomUUID } from 'node:crypto'
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
  type RequestListener,
} from 'node:http'
import Provider from 'oidc-provider'

import { escape } from './artifacts.ts'

export interface SyntheticUser {
  subject: string
  label: string
  email: string
}

export interface AuthenticationTarget {
  issuerURL: string
  oidcURL: string
  owner: SyntheticUser
}

const starting: RequestListener = (_request, response) => {
  response.writeHead(503).end()
}

export async function startIdentityProvider(coralCallback: string, reefOrigin: string) {
  let handler = starting
  const server = createServer((request, response) => handler(request, response))
  // Coral reuses its connection for every sign-in. Node closes an idle one after
  // five seconds, and a request sent as it closes fails the sign-in.
  server.keepAliveTimeout = 0
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`
  const accounts = new Map<string, SyntheticUser & { emailVerified: boolean }>()
  const register = (label: string, emailVerified = true, email?: string): SyntheticUser => {
    const subject = `reel-${randomUUID()}`
    const account = { subject, label, email: email ?? `${subject}@example.test`, emailVerified }
    accounts.set(subject, account)
    return { subject, label, email: account.email }
  }
  const owner = register('Organization Owner')
  const clientId = 'coral-reel'
  const clientSecret = randomBytes(32).toString('hex')
  let closing: Promise<void> | undefined
  const close = () => {
    closing ??= new Promise<void>((resolve, reject) => {
      server.closeAllConnections()
      server.close((error) => (error ? reject(error) : resolve()))
    })
    return closing
  }
  try {
    const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 })
    const provider = new Provider(origin, {
      clients: [
        {
          client_id: clientId,
          client_secret: clientSecret,
          redirect_uris: [coralCallback],
          response_types: ['code'],
          grant_types: ['authorization_code'],
          token_endpoint_auth_method: 'client_secret_post',
          id_token_signed_response_alg: 'RS256',
        },
      ],
      jwks: {
        keys: [
          { ...privateKey.export({ format: 'jwk' }), kid: randomUUID(), alg: 'RS256', use: 'sig' },
        ],
      },
      cookies: { keys: [randomBytes(32).toString('hex')] },
      features: { devInteractions: { enabled: false } },
      ttl: {
        Interaction: 300,
        Session: 3600,
        Grant: 3600,
        AccessToken: 300,
        IdToken: 300,
        AuthorizationCode: 60,
      },
      pkce: { required: () => true },
      // Coral consumes ID-token identity claims, not a UserInfo side channel.
      conformIdTokenClaims: false,
      claims: { openid: ['sub'], email: ['email', 'email_verified'], profile: ['name'] },
      findAccount: (_context, subject) => {
        const account = accounts.get(subject)
        if (!account) return undefined
        return {
          accountId: subject,
          claims: () => ({
            sub: subject,
            name: account.label,
            email: account.email,
            email_verified: account.emailVerified,
          }),
        }
      },
      interactions: { url: (_context, interaction) => `/interaction/${interaction.uid}` },
    })
    const callback = provider.callback()
    handler = (request, response) => {
      void (async () => {
        if (request.headers.host !== new URL(origin).host) {
          response.writeHead(403).end()
          return
        }
        response.setHeader('Cache-Control', 'no-store')
        const url = new URL(request.url!, origin)
        if (url.pathname === '/.well-known/oauth-client' && request.method === 'GET') {
          sendJSON(response, {
            client_id: `${origin}/.well-known/oauth-client`,
            client_name: 'Reel fixture',
            redirect_uris: [`${origin}/fixture/callback`],
            grant_types: ['authorization_code'],
            response_types: ['code'],
            token_endpoint_auth_method: 'none',
          })
          return
        }
        if (url.pathname === '/fixture/callback' && request.method === 'GET') {
          response.setHeader('Content-Type', 'text/html; charset=utf-8')
          response.end(
            '<!doctype html><title>Fixture callback</title><h1>Fixture authorization received</h1>',
          )
          return
        }
        if (url.pathname === '/fixture/accounts' && request.method === 'POST') {
          if (
            !validOrigin(request, origin) ||
            request.headers['content-type'] !== 'application/json'
          ) {
            response.writeHead(403).end()
            return
          }
          const body = JSON.parse(await readBody(request))
          if (
            typeof body.label !== 'string' ||
            !body.label.trim() ||
            body.label.length > 80 ||
            (body.emailVerified !== undefined && typeof body.emailVerified !== 'boolean') ||
            (body.email !== undefined &&
              (typeof body.email !== 'string' || body.email.length > 320))
          ) {
            response.writeHead(400).end()
            return
          }
          sendJSON(response, register(body.label, body.emailVerified, body.email))
          return
        }
        if (url.pathname.startsWith('/fixture/accounts/') && request.method === 'PATCH') {
          if (
            !validOrigin(request, origin) ||
            request.headers['content-type'] !== 'application/json'
          ) {
            response.writeHead(403).end()
            return
          }
          const subject = url.pathname.slice('/fixture/accounts/'.length)
          const account = accounts.get(subject)
          if (!account || subject === owner.subject) {
            response.writeHead(404).end()
            return
          }
          const body = JSON.parse(await readBody(request))
          if (typeof body.email !== 'string' || !body.email || body.email.length > 320) {
            response.writeHead(400).end()
            return
          }
          account.email = body.email
          response.writeHead(204).end()
          return
        }
        if (url.pathname.startsWith('/fixture/accounts/') && request.method === 'DELETE') {
          if (!validOrigin(request, origin)) {
            response.writeHead(403).end()
            return
          }
          const subject = url.pathname.slice('/fixture/accounts/'.length)
          if (subject === owner.subject) {
            response.writeHead(403).end()
            return
          }
          accounts.delete(subject)
          response.writeHead(204).end()
          return
        }
        if (url.pathname.startsWith('/interaction/')) {
          const details = await provider.interactionDetails(request, response)
          if (details.prompt.name === 'consent') {
            if (!details.session) throw new Error('Consent without an identity')
            const grant = new provider.Grant({ accountId: details.session.accountId, clientId })
            grant.addOIDCScope('openid email profile')
            await provider.interactionFinished(
              request,
              response,
              { consent: { grantId: await grant.save() } },
              { mergeWithLastSubmission: true },
            )
            return
          }
          if (details.prompt.name !== 'login') throw new Error('Unsupported fixture interaction')
          if (request.method === 'POST') {
            if (!validOrigin(request, origin)) {
              response.writeHead(403).end()
              return
            }
            const subject = new URLSearchParams(await readBody(request)).get('account')
            if (!subject || !accounts.has(subject)) {
              response.writeHead(400).end()
              return
            }
            await provider.interactionFinished(
              request,
              response,
              { login: { accountId: subject } },
              { mergeWithLastSubmission: false },
            )
            return
          }
          if (request.method !== 'GET') {
            response.writeHead(405).end()
            return
          }
          response.setHeader('Content-Type', 'text/html; charset=utf-8')
          response.setHeader(
            'Content-Security-Policy',
            `default-src 'none'; form-action 'self' ${new URL(coralCallback).origin} ${reefOrigin}; frame-ancestors 'none'; base-uri 'none'`,
          )
          response.end(
            `<!doctype html><html lang="en"><title>Synthetic identity provider</title><h1>Synthetic identity provider</h1><p>Disposable test accounts only. This is not Coral's login UI.</p><form method="post" action="/interaction/${encodeURIComponent(details.uid)}">${[...accounts.values()].map((account) => `<p><button name="account" value="${escape(account.subject)}">${escape(account.label)}</button></p>`).join('')}</form></html>`,
          )
          return
        }
        callback(request, response)
      })().catch(() => {
        // Do not print URLs, token bodies, client secrets or interaction state.
        if (!response.headersSent) response.writeHead(500).end('Identity fixture request failed')
        else response.destroy()
      })
    }
    return { origin, owner, clientId, clientSecret, close }
  } catch (error) {
    await close()
    throw error
  }
}

function validOrigin(request: IncomingMessage, origin: string) {
  return request.headers.origin === origin
}

async function readBody(request: IncomingMessage) {
  let body = ''
  for await (const chunk of request) {
    body += chunk.toString()
    if (Buffer.byteLength(body) > 4096) throw new Error('Fixture request too large')
  }
  return body
}

function sendJSON(response: ServerResponse, value: unknown) {
  response.setHeader('Content-Type', 'application/json')
  response.end(JSON.stringify(value))
}
