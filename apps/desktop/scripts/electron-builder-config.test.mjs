import assert from 'node:assert/strict'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test, { after } from 'node:test'

import { createConfig } from '../electron-builder.config.ts'
import { RELEASE_PLATFORMS, releaseTarget } from '../src/shared/release-targets.ts'

const tempDir = mkdtempSync(join(tmpdir(), 'coral-desktop-signing-config-'))
const apiKeyPath = join(tempDir, 'AuthKey_TEST.p8')
writeFileSync(apiKeyPath, 'test App Store Connect private key')
after(() => rmSync(tempDir, { recursive: true, force: true }))

const apiKeyCredentials = {
  APPLE_API_KEY: apiKeyPath,
  APPLE_API_KEY_ID: 'TESTKEY123',
  APPLE_API_ISSUER: '00000000-0000-0000-0000-000000000000',
}

test('desktop package and lockfile versions match', () => {
  const packageJson = JSON.parse(
    readFileSync(new URL('../package.json', import.meta.url), 'utf8'),
  )
  const packageLock = JSON.parse(
    readFileSync(new URL('../package-lock.json', import.meta.url), 'utf8'),
  )

  assert.equal(packageLock.version, packageJson.version)
  assert.equal(packageLock.packages[''].version, packageJson.version)
})

test('non-release packages are explicitly unsigned', () => {
  const config = createConfig({})

  assert.equal(config.forceCodeSigning, false)
  assert.equal(config.mac?.identity, null)
  assert.equal(config.mac?.hardenedRuntime, false)
  assert.equal(config.mac?.entitlements, null)
  assert.equal(config.mac?.entitlementsInherit, null)
  assert.equal(config.mac?.notarize, false)
})

test('release packages enable strict signing and notarization', () => {
  const config = createConfig(
    {
      CORAL_DESKTOP_RELEASE: '1',
      ...apiKeyCredentials,
    },
    'darwin',
  )

  assert.equal(config.forceCodeSigning, true)
  assert.equal(config.mac?.identity, undefined)
  assert.equal(config.mac?.hardenedRuntime, true)
  assert.equal(config.mac?.entitlements, 'resources/entitlements.mac.plist')
  assert.equal(config.mac?.entitlementsInherit, 'resources/entitlements.mac.inherit.plist')
  assert.equal(config.mac?.notarize, true)
})

test('release packages reject every missing or blank notarization input', () => {
  for (const name of Object.keys(apiKeyCredentials)) {
    const missing = { ...apiKeyCredentials }
    delete missing[name]
    assert.throws(
      () => createConfig({ CORAL_DESKTOP_RELEASE: '1', ...missing }, 'darwin'),
      new RegExp(`missing ${name}`),
    )

    assert.throws(
      () =>
        createConfig(
          {
            CORAL_DESKTOP_RELEASE: '1',
            ...apiKeyCredentials,
            [name]: ' \t ',
          },
          'darwin',
        ),
      new RegExp(`missing ${name}`),
    )
  }
})

test('release packages require a readable, non-empty API key file', async () => {
  const missingPath = join(tempDir, 'missing.p8')
  const emptyPath = join(tempDir, 'empty.p8')
  await writeFile(emptyPath, '')

  for (const invalidPath of [missingPath, emptyPath, tempDir]) {
    assert.throws(
      () =>
        createConfig(
          {
            CORAL_DESKTOP_RELEASE: '1',
            ...apiKeyCredentials,
            APPLE_API_KEY: invalidPath,
          },
          'darwin',
        ),
      /APPLE_API_KEY must point to a readable, non-empty regular file/,
    )
  }
})

test('release mode is rejected off a release platform before any preflight', () => {
  assert.equal(releaseTarget('win32'), null)
  assert.throws(
    () => createConfig({ CORAL_DESKTOP_RELEASE: '1', ...apiKeyCredentials }, 'win32'),
    /CORAL_DESKTOP_RELEASE=1 supports darwin, linux hosts only, not win32/,
  )
})

test('every release platform demands Apple credentials only if it signs', () => {
  for (const platform of RELEASE_PLATFORMS) {
    const signs = releaseTarget(platform).appleSigning
    // A platform that signs nothing must package without credentials in the
    // environment; one that signs must refuse to package without them.
    const build = () => createConfig({ CORAL_DESKTOP_RELEASE: '1' }, platform)
    if (signs) {
      assert.throws(build, /App Store Connect API key credential set/)
    } else {
      assert.equal(build().forceCodeSigning, false)
      assert.equal(build().mac?.notarize, false)
    }
  }
})

test('non-release packaging config is identical on every host platform', () => {
  const linuxHost = createConfig({}, 'linux')

  assert.deepEqual(linuxHost, createConfig({}, 'darwin'))
  assert.deepEqual(linuxHost, createConfig({}, 'win32'))
})

test('linux packages target AppImage and deb, and publish an update feed', () => {
  const { linux, deb, publish } = createConfig({}, 'linux')

  assert.deepEqual(linux?.target, [
    { target: 'AppImage', arch: ['x64'] },
    { target: 'deb', arch: ['x64'] },
  ])
  // The AppImage updater reads latest-linux.yml and the app-update.yml the
  // package embeds, so linux must inherit the GitHub publish config rather
  // than override it.
  assert.equal(linux?.publish, undefined)
  assert.equal(publish?.[0]?.provider, 'github')
  // Neither the executable nor the deb may claim the `coral` name; the deb
  // symlinks its executable into /usr/bin and would shadow the CLI.
  assert.equal(linux?.executableName, 'coral-desktop')
  assert.equal(deb?.packageName, 'coral-desktop')
  // fpm refuses to build a deb without a maintainer contact.
  assert.match(linux?.maintainer ?? '', /^.+ <.+@.+>$/)
  // A directory, so electron-builder generates a multi-size hicolor set;
  // naming a single .png would install only the 1024x1024 source.
  assert.equal(linux?.icon, 'resources/icons')
})

test('deb metadata inputs that live in package.json are present', () => {
  const packageJson = JSON.parse(
    readFileSync(new URL('../package.json', import.meta.url), 'utf8'),
  )

  // fpm requires a project URL and reads the license from package metadata.
  assert.match(packageJson.homepage, /^https:\/\//)
  assert.equal(packageJson.license, 'Apache-2.0')
})
