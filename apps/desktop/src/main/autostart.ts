// Start at login on Linux. Electron's login item API does not cover Linux, so
// Desktop writes an XDG autostart entry that names the AppImage.
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

import { BACKGROUND_ARG, type LoginItem } from './background'

// Matches package.json `desktopName`, so a session manager knows the app.
const ENTRY = 'coral-app.desktop'

/**
 * The login item that `directory`, the person's XDG autostart folder, holds
 * for the image at `appImage`. An update can give the image another name, so
 * an entry that names another image is rewritten to name this one.
 */
export function autostartLoginItem(directory: string, appImage: string): LoginItem {
  const file = join(directory, ENTRY)
  const enabled = () => existsSync(file)
  const write = () => {
    const content = entry(appImage)
    mkdirSync(directory, { recursive: true })
    writeFileSync(file, content, { encoding: 'utf8', mode: 0o644 })
  }
  try {
    if (enabled() && readFileSync(file, 'utf8') !== entry(appImage)) write()
  } catch {
    // The entry stays as it is. The person can turn the setting off and on.
  }
  return {
    enabled,
    set: (enable) => (enable ? write() : rmSync(file, { force: true })),
  }
}

function entry(appImage: string): string {
  if (/[\r\n]/.test(appImage)) throw new Error('The AppImage path has a line break')
  // The Desktop Entry specification quotes an argument and escapes these
  // characters in it. Its string escaping then doubles every backslash, and
  // `%%` is a literal percent sign.
  const quoted = `"${appImage.replace(/["`$\\]/g, '\\$&')}"`
  const exec = `${quoted} ${BACKGROUND_ARG}`.replace(/\\/g, '\\\\').replace(/%/g, '%%')
  return [
    '[Desktop Entry]',
    'Type=Application',
    'Name=Coral',
    `Exec=${exec}`,
    'Terminal=false',
    '',
  ].join('\n')
}
