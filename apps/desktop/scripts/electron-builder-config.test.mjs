import assert from 'node:assert/strict'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import test, { after } from 'node:test'

import { createConfig } from '../electron-builder.config.ts'

const tempDir = mkdtempSync(join(tmpdir(), 'coral-desktop-signing-config-'))
const apiKeyPath = join(tempDir, 'AuthKey_TEST.p8')
writeFileSync(apiKeyPath, 'test App Store Connect private key')
after(() => rmSync(tempDir, { recursive: true, force: true }))

const apiKeyCredentials = {
  APPLE_API_KEY: apiKeyPath,
  APPLE_API_KEY_ID: 'TESTKEY123',
  APPLE_API_ISSUER: '00000000-0000-0000-0000-000000000000',
}

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
  const config = createConfig({
    CORAL_DESKTOP_RELEASE: '1',
    ...apiKeyCredentials,
  })

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
      () => createConfig({ CORAL_DESKTOP_RELEASE: '1', ...missing }),
      new RegExp(`missing ${name}`),
    )

    assert.throws(
      () =>
        createConfig({
          CORAL_DESKTOP_RELEASE: '1',
          ...apiKeyCredentials,
          [name]: ' \t ',
        }),
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
        createConfig({
          CORAL_DESKTOP_RELEASE: '1',
          ...apiKeyCredentials,
          APPLE_API_KEY: invalidPath,
        }),
      /APPLE_API_KEY must point to a readable, non-empty regular file/,
    )
  }
})
