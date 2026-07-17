export type AuthMode = 'disabled' | 'required'

export interface DisabledAuthConfig {
  mode: 'disabled'
}

export interface RequiredAuthConfig {
  clientId: string | null
  cookieName: string
  cookieSecure: boolean
  issuer: string
  mode: 'required'
  redirectUri: string | null
  scope: string | null
  sessionMaxAgeSeconds: number
  sessionSecret: string
}

export type AuthConfig = DisabledAuthConfig | RequiredAuthConfig

export interface AuthSession {
  accessToken: string
  expiresAt: number
  tokenType: string
}
