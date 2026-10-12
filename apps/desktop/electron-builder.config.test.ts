import { describe, expect, it } from 'vitest'

import { createConfig } from './electron-builder.config.ts'

describe('a package built without the release flag', () => {
  it.each(['darwin', 'linux', 'win32'] as const)(
    'publishes no update feed and signs nothing on %s',
    (platform) => {
      const config = createConfig({}, platform)

      expect(config.publish).toBeNull()
      expect(config.forceCodeSigning).toBe(false)
      expect(config.mac?.identity).toBeNull()
      expect(config.mac?.notarize).toBe(false)
      expect(config.win?.azureSignOptions).toBeUndefined()
    },
  )
})

// A complete credential set for each platform's release build.
const RELEASE_ENV: Record<'darwin' | 'linux' | 'win32', NodeJS.ProcessEnv> = {
  darwin: {
    CORAL_DESKTOP_RELEASE: '1',
    CSC_NAME: 'With Coral (TEAMID)',
    APPLE_API_KEY: import.meta.filename,
    APPLE_API_KEY_ID: 'KEYID',
    APPLE_API_ISSUER: 'issuer',
  },
  linux: { CORAL_DESKTOP_RELEASE: '1' },
  win32: {
    CORAL_DESKTOP_RELEASE: '1',
    AZURE_TENANT_ID: 'tenant',
    AZURE_CLIENT_ID: 'client',
    AZURE_CLIENT_SECRET: 'secret',
    AZURE_TRUSTED_SIGNING_ENDPOINT: 'https://weu.codesigning.azure.net',
    AZURE_TRUSTED_SIGNING_ACCOUNT_NAME: 'withcoral',
    AZURE_TRUSTED_SIGNING_CERTIFICATE_PROFILE_NAME: 'coral-app',
    AZURE_TRUSTED_SIGNING_PUBLISHER_NAME: 'With Coral Ltd',
  },
}

describe('a release package', () => {
  it.each(['darwin', 'linux', 'win32'] as const)(
    'updates from the v1 feed only on %s',
    (platform) => {
      const config = createConfig(RELEASE_ENV[platform], platform)

      // Never withcoral/coral, whose releases the 0.x app polls.
      expect(config.publish).toEqual([
        { provider: 'github', owner: 'withcoral', repo: 'coral-releases' },
      ])
    },
  )
})

describe('release signing', () => {
  it('signs macOS with hardened runtime, notarizes, and grants JIT to every binary', () => {
    const config = createConfig(RELEASE_ENV.darwin, 'darwin')

    expect(config.forceCodeSigning).toBe(true)
    expect(config.mac).toMatchObject({
      identity: 'With Coral (TEAMID)',
      hardenedRuntime: true,
      notarize: true,
      entitlements: 'resources/entitlements.mac.plist',
      // Nested binaries inherit JIT. The runtime needs it too: it embeds V8,
      // which aborts at startup without it.
      entitlementsInherit: 'resources/entitlements.mac.plist',
    })
    // Every binary is signed, the runtime included.
    expect(config.mac?.signIgnore ?? []).toEqual([])
  })

  it('signs Windows with Azure Trusted Signing under the publisher the updater checks', () => {
    const config = createConfig(RELEASE_ENV.win32, 'win32')

    expect(config.forceCodeSigning).toBe(true)
    expect(config.win?.azureSignOptions).toEqual({
      publisherName: 'With Coral Ltd',
      endpoint: 'https://weu.codesigning.azure.net',
      codeSigningAccountName: 'withcoral',
      certificateProfileName: 'coral-app',
    })
  })

  it('leaves the Linux AppImage unsigned', () => {
    expect(createConfig(RELEASE_ENV.linux, 'linux').forceCodeSigning).toBe(false)
  })

  it.each([
    ['darwin', 'CSC_NAME'],
    ['darwin', 'APPLE_API_KEY'],
    ['darwin', 'APPLE_API_KEY_ID'],
    ['darwin', 'APPLE_API_ISSUER'],
    ['win32', 'AZURE_TENANT_ID'],
    ['win32', 'AZURE_CLIENT_ID'],
    ['win32', 'AZURE_CLIENT_SECRET'],
    ['win32', 'AZURE_TRUSTED_SIGNING_ENDPOINT'],
    ['win32', 'AZURE_TRUSTED_SIGNING_ACCOUNT_NAME'],
    ['win32', 'AZURE_TRUSTED_SIGNING_CERTIFICATE_PROFILE_NAME'],
    ['win32', 'AZURE_TRUSTED_SIGNING_PUBLISHER_NAME'],
  ] as const)('fails on %s when %s is empty', (platform, name) => {
    // A missing GitHub secret arrives as an empty string.
    const env = { ...RELEASE_ENV[platform], [name]: ' ' }

    expect(() => createConfig(env, platform)).toThrow(name)
  })

  it('fails when the App Store Connect key is not a readable file', () => {
    const env = { ...RELEASE_ENV.darwin, APPLE_API_KEY: '/nonexistent/AuthKey.p8' }

    expect(() => createConfig(env, 'darwin')).toThrow('APPLE_API_KEY')
  })
})
