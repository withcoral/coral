// What the install suite's host-side setup (install/cluster.ts) and its test
// process agree on: the Install's hosts, the identity provider's, and how the
// container learns the rest. Both sides import this file and nothing else from
// each other.

/** The test-only domain. `.test` never resolves on a real network. */
export const INSTALL_DOMAIN = 'reel.test'

/** The Install's three hosts, which the chart's HTTPRoutes serve, and the identity provider's. */
export const INSTALL_HOSTS = {
  web: `coral.${INSTALL_DOMAIN}`,
  auth: `auth.coral.${INSTALL_DOMAIN}`,
  mcp: `mcp.coral.${INSTALL_DOMAIN}`,
  idp: `idp.${INSTALL_DOMAIN}`,
} as const

/**
 * Where the container finds the install's CA and the identity provider's
 * certificate and key, mounted read-only from the host.
 */
export const INSTALL_TLS_DIRECTORY = '/run/reel-install'

/** The files in that folder: the CA, and the identity provider's certificate and key. */
export const INSTALL_TLS_FILES = { ca: 'ca.pem', cert: 'idp.pem', key: 'idp.key' } as const

/** The per-run secrets the host makes, before the chart is installed. */
export interface InstallSecrets {
  /** The owner's OIDC subject, which the chart names as `bootstrap_owner_subject`. */
  ownerSubject: string
  /** The client secret, which the chart reads from its Secret. */
  clientSecret: string
}

/** The secrets, and where the container finds the TLS files. */
export interface InstallSettings extends InstallSecrets {
  /** The CA that signs the Gateway's, the identity provider's and Postgres's certificates. */
  caFile: string
  certFile: string
  keyFile: string
}

/** Reads the settings the host passed, or fails naming what is missing. */
export function installSettings(env: NodeJS.ProcessEnv): InstallSettings {
  const ownerSubject = env.E2E_INSTALL_OWNER_SUBJECT
  const clientSecret = env.E2E_INSTALL_CLIENT_SECRET
  if (!ownerSubject || !/^reel-[0-9a-f-]{36}$/.test(ownerSubject) || !clientSecret)
    throw new Error(
      'The install suite runs in the Reel container, against a cluster that install/cluster.ts sets up',
    )
  return {
    ownerSubject,
    clientSecret,
    caFile: `${INSTALL_TLS_DIRECTORY}/${INSTALL_TLS_FILES.ca}`,
    certFile: `${INSTALL_TLS_DIRECTORY}/${INSTALL_TLS_FILES.cert}`,
    keyFile: `${INSTALL_TLS_DIRECTORY}/${INSTALL_TLS_FILES.key}`,
  }
}
