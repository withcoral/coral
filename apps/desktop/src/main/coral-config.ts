import { mkdir, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

export interface DesktopCoralConfigOptions {
  bindAddr?: string
  directory?: string
}

export function desktopRuntimeCoralConfigOptions(
  isPackaged: boolean,
  env: NodeJS.ProcessEnv = process.env,
): DesktopCoralConfigOptions {
  if (isPackaged) return {}

  const devPort = env.CORAL_DEV_SIDECAR_PORT || '8778'
  return {
    bindAddr: `127.0.0.1:${devPort}`,
    directory: `coral-dev-${devPort}`,
  }
}

export function desktopCoralConfigDir(userDataDir: string, directory = 'coral'): string {
  return join(userDataDir, directory)
}

/** Creates Desktop's isolated Coral state only on first launch. */
export async function ensureDesktopCoralConfig(
  userDataDir: string,
  { bindAddr = '127.0.0.1:0', directory = 'coral' }: DesktopCoralConfigOptions = {},
): Promise<string> {
  const configDir = desktopCoralConfigDir(userDataDir, directory)
  await mkdir(configDir, { recursive: true })
  try {
    await writeFile(join(configDir, 'config.toml'), desktopCoralConfig(bindAddr), {
      encoding: 'utf8',
      flag: 'wx',
    })
  } catch (error: unknown) {
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error
  }
  return configDir
}

function desktopCoralConfig(bindAddr: string): string {
  return `version = 1\n\n[server]\nbind_addr = ${JSON.stringify(bindAddr)}\n`
}
