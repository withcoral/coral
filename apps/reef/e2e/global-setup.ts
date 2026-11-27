import { constants } from 'node:fs'
import { access, mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import type { TestProject } from 'vitest/node'

import { json, renderRun } from './support/artifacts.ts'
import { ready, startStack } from './support/stack.ts'
import { loopbackFixtures, type DesktopApp } from './support/config.ts'
import { checkDesktopProfiles } from './support/desktop-profile.ts'
import { INSTALL_HOSTS, type InstallSettings } from './support/install.ts'
import { startIdentityProvider } from './support/oidc-fixture.ts'
import { validateFixtureTLS } from './support/provider-fixtures.ts'

export default async function setup(project: TestProject) {
  const config = project.getProvidedContext().reelBootstrap
  if (!config) throw new Error('Reel requires the run-level reporter bootstrap')
  const name = config.attached ? 'attached-local' : project.name
  const multiInstance = config.suite === 'multi-instance'
  const authenticated = (config.suite === 'main' || multiInstance) && !config.probes
  // Authenticated Coral denies loopback, so the container advertises a private
  // host. Local Coral reaches loopback services, as Desktop's users run them.
  // An Install in a cluster reaches only the container's private host.
  const fixtures =
    authenticated || config.install ? (config.fixtures ?? loopbackFixtures) : loopbackFixtures
  const targetDir = join(config.runDir, name)
  await mkdir(targetDir, { recursive: true })
  let stack: Awaited<ReturnType<typeof startStack>> | undefined
  let install: Awaited<ReturnType<typeof startInstallTarget>> | undefined
  let closed: Promise<void> | undefined
  const metadata: {
    name: string
    state: 'starting' | 'ready' | 'closed' | 'failed'
    startedAt: string
    pids: number[]
    error?: string
  } = { name, state: 'starting', startedAt: new Date().toISOString(), pids: [] }
  const save = () => json(join(targetDir, 'target.json'), metadata)
  const close = () => {
    closed ??= (async () => {
      try {
        await stack?.stop()
        await install?.close()
        if (metadata.state !== 'failed') metadata.state = 'closed'
      } catch (error) {
        metadata.state = 'failed'
        metadata.error = [metadata.error, `Target cleanup failed: ${String(error)}`]
          .filter(Boolean)
          .join('\n')
        throw error
      } finally {
        await save()
        await renderRun(config.runDir)
      }
    })()
    return closed
  }
  try {
    await save()
    await validateFixtureTLS(fixtures)
    // Missing prerequisites fail setup, not each scenario.
    if (config.desktop) await desktopPrerequisites(config.desktop)
    // Each Desktop scenario launches its own app, which chooses its origin.
    const target = config.desktop
      ? { name: 'desktop' as const, baseURL: '' }
      : config.install
        ? (install = await startInstallTarget(config.install)).target
        : config.attached
          ? { name: 'attached-local' as const, baseURL: config.attached.replace(/\/$/, '') }
          : (stack = await startStack({
              ...config,
              // State is already private per stack; logs must also stay target-owned.
              runDir: targetDir,
              authenticated,
              // Product fixtures are explicitly trusted; ordinary stack builders
              // keep deny/deny so independent default-policy tests stay meaningful.
              privateNetworkAccess: authenticated ? 'http-and-https' : undefined,
              postgresURL: config.postgresURL,
              fixtures,
              probes: config.probes,
              multiInstance,
            })).target
    if (config.attached) await ready(target.baseURL)
    metadata.state = 'ready'
    metadata.pids = stack?.pids.filter((pid) => pid !== undefined) ?? []
    await save()
    project.provide('reel', {
      target,
      runDir: config.runDir,
      film: config.film,
      ffmpegBinary: config.ffmpegBinary,
      fixtures,
      tlsDirectory: config.tlsDirectory,
      desktop: config.desktop,
    })
    return close
  } catch (error) {
    metadata.state = 'failed'
    metadata.error = `Target setup failed: ${String(error)}`
    await save()
    try {
      await close()
    } catch (cleanup) {
      console.warn(cleanup)
    }
    throw error
  }
}

/**
 * The Install that install/cluster.ts set up before this container started,
 * and the identity provider it trusts, which runs here.
 */
async function startInstallTarget(settings: InstallSettings) {
  const web = `https://${INSTALL_HOSTS.web}`
  const auth = `https://${INSTALL_HOSTS.auth}`
  const idp = await startIdentityProvider(`${auth}/auth/oidc/callback`, web, {
    host: INSTALL_HOSTS.idp,
    ...settings,
  })
  try {
    await ready(web)
    return {
      target: {
        name: 'helm-install' as const,
        baseURL: web,
        mcpURL: `https://${INSTALL_HOSTS.mcp}`,
        publicOrigins: [web, auth, `https://${INSTALL_HOSTS.mcp}`],
        authentication: { issuerURL: auth, oidcURL: idp.origin, owner: idp.owner },
      },
      close: idp.close,
    }
  } catch (error) {
    await idp.close()
    throw error
  }
}

async function desktopPrerequisites(app: DesktopApp) {
  const { executable, env } = app
  try {
    await access(executable, constants.X_OK)
  } catch {
    throw new Error(
      `No Desktop app at ${executable}; run Reel's desktop suite, or check E2E_DESKTOP_EXECUTABLE`,
    )
  }
  if (process.platform === 'linux' && !env.DISPLAY && !env.WAYLAND_DISPLAY)
    throw new Error('The desktop suite needs a display; the Reel container provides Xvfb')
  checkDesktopProfiles(app)
}
