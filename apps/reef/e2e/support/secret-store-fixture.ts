/**
 * An `env` store, which every build has, and the variable it holds. A reference names
 * the whole `variable`, which starts with `prefix`, and a scenario expects `value` upstream.
 */
export const ENV_SECRET_STORE = {
  name: 'reel-env',
  prefix: 'E2E_SECRET_STORE_ENV_',
  variable: 'E2E_SECRET_STORE_ENV_FORECAST_TOKEN',
  value: 'reel-env-value-canary',
} as const

/** The `[[secret_stores]]` table and the environment for {@link ENV_SECRET_STORE}. */
export const envSecretStore = {
  config: `
[[secret_stores]]
name = ${JSON.stringify(ENV_SECRET_STORE.name)}
kind = "env"
prefix = ${JSON.stringify(ENV_SECRET_STORE.prefix)}
`,
  env: { [ENV_SECRET_STORE.variable]: ENV_SECRET_STORE.value },
}
