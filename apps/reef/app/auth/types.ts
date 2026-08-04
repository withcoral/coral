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

export interface AuthSession {
  accessToken: string
  expiresAt: number
  tokenType: string
}
