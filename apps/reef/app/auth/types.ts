export type AuthMode = 'disabled' | 'required'

export interface DisabledAuthConfig {
  mode: 'disabled'
}

export interface RequiredAuthConfig {
  cookieName: string
  issuer: string
  mode: 'required'
  publicUrl: string
  sessionMaxAgeSeconds: number
  sessionSecret: string
}

export type AuthConfig = DisabledAuthConfig | RequiredAuthConfig

/**
 * One request's view of a signed-in web session. Coral holds the session and
 * its grant; Reef keeps only the handle, in the session cookie.
 */
export interface AuthSession {
  /** A Reef access token Coral minted for this request. */
  accessToken: string
  /** When `accessToken` expires, in Unix seconds. */
  expiresAt: number
  /** The session's opaque handle. It is a bearer secret and never leaves the server. */
  handle: string
  /** When the session itself ends, in Unix seconds. It never moves later. */
  sessionExpiresAt: number
}

export type BrowserAuth = { mode: 'disabled' } | { csrfToken: string; mode: 'required' }

export type RequestAuth =
  | { accessToken: null; mode: 'disabled' }
  | { accessToken: string; csrfToken: string; mode: 'required'; session: AuthSession }
