import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'

import { autostartLoginItem } from './autostart'

describe('the XDG autostart entry', () => {
  let config: string
  let entry: string
  beforeEach(() => {
    config = mkdtempSync(join(tmpdir(), 'coral-autostart-'))
    // The autostart folder does not exist in a new profile.
    entry = join(config, 'autostart', 'coral-app.desktop')
  })
  afterEach(() => rmSync(config, { recursive: true, force: true }))
  const item = (appImage: string) => autostartLoginItem(join(config, 'autostart'), appImage)

  it('starts the AppImage with no window, and goes when the person turns it off', () => {
    const login = item('/home/ada/Apps/coral-app-linux-x86_64.AppImage')
    expect(login.enabled()).toBe(false)

    login.set(true)

    expect(login.enabled()).toBe(true)
    expect(readFileSync(entry, 'utf8')).toBe(
      [
        '[Desktop Entry]',
        'Type=Application',
        'Name=Coral',
        'Exec="/home/ada/Apps/coral-app-linux-x86_64.AppImage" --background',
        'Terminal=false',
        '',
      ].join('\n'),
    )

    login.set(false)
    expect(login.enabled()).toBe(false)
    // Turning it off twice is not an error.
    login.set(false)
  })

  it.each([
    ['a space', '/opt/My Apps/coral.AppImage', 'Exec="/opt/My Apps/coral.AppImage" --background'],
    // Quoting escapes these for the launcher, and the entry's string escaping
    // then doubles each backslash.
    ['a quote', '/opt/a"b.AppImage', 'Exec="/opt/a\\\\"b.AppImage" --background'],
    ['a dollar sign', '/opt/$HOME.AppImage', 'Exec="/opt/\\\\$HOME.AppImage" --background'],
    ['a backslash', '/opt/a\\b.AppImage', 'Exec="/opt/a\\\\\\\\b.AppImage" --background'],
    // A lone percent sign would start a field code.
    ['a percent sign', '/opt/100%.AppImage', 'Exec="/opt/100%%.AppImage" --background'],
  ])('escapes %s in the path', (_name, appImage, exec) => {
    item(appImage).set(true)

    expect(readFileSync(entry, 'utf8').split('\n')).toContain(exec)
  })

  it('refuses a path that would add lines to the entry', () => {
    const login = item('/opt/coral.AppImage\nExec=/bin/evil')

    expect(() => login.set(true)).toThrow(/line break/)
    expect(login.enabled()).toBe(false)
  })

  it('names this image again after an update moved it', () => {
    item('/opt/coral-1.AppImage').set(true)

    const login = item('/opt/coral-2.AppImage')

    expect(login.enabled()).toBe(true)
    expect(readFileSync(entry, 'utf8')).toContain('Exec="/opt/coral-2.AppImage" --background')
  })

  it('leaves the folder alone while the login item is off', () => {
    item('/opt/coral.AppImage')

    expect(() => readFileSync(entry)).toThrow(/ENOENT/)
  })

  it('keeps an entry it cannot rewrite', () => {
    item('/opt/coral-1.AppImage').set(true)
    const before = readFileSync(entry, 'utf8')

    const login = item('/opt/coral\n2.AppImage')

    expect(login.enabled()).toBe(true)
    expect(readFileSync(entry, 'utf8')).toBe(before)
  })
})
