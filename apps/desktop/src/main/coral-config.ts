import { mkdir, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

const DESKTOP_CORAL_CONFIG = 'version = 1\n\n[server]\nbind_addr = "127.0.0.1:0"\n'

export function desktopCoralConfigDir(userDataDir: string): string {
  return join(userDataDir, 'coral')
}

/** Creates Desktop's isolated Coral state only on first launch. */
export async function ensureDesktopCoralConfig(userDataDir: string): Promise<string> {
  const configDir = desktopCoralConfigDir(userDataDir)
  await mkdir(configDir, { recursive: true })
  try {
    await writeFile(join(configDir, 'config.toml'), DESKTOP_CORAL_CONFIG, { encoding: 'utf8', flag: 'wx' })
  } catch (error: unknown) {
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error
  }
  return configDir
}
