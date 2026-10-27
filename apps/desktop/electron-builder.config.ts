import { accessSync, constants, existsSync, statSync } from 'node:fs'
import { join } from 'node:path'

import type { Configuration } from 'electron-builder'

import { APP_ID } from './src/shared/app-id.ts'

// `npm run stage` writes these; a package without them cannot start.
const STAGED = [
  join('resources', 'reef', 'server.mjs'),
  join('resources', 'reef', 'build', 'client'),
]

// The v1 update feed: GitHub releases in a repository of its own. It must
// never be withcoral/coral, whose releases the 0.x app polls: that app
// would update itself into this one. Only a release package names a feed, so
// only a release package can update itself.
//
// electron-builder names the updater's download cache after package.json
// `name`. `coral-app` keeps it apart from the 0.x `@withcoral/desktop`.
export const UPDATE_FEED = {
  provider: 'github',
  owner: 'withcoral',
  repo: 'coral-releases',
} as const

/**
 * The package configuration. `CORAL_DESKTOP_RELEASE=1` makes a release
 * package: it names the v1 feed, and on macOS and Windows it must be signed.
 * A release build fails before packaging when a credential is missing, because
 * electron-builder skips notarization with only a warning.
 */
export function createConfig(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
): Configuration {
  const release = env.CORAL_DESKTOP_RELEASE === '1'
  const macRelease = release && platform === 'darwin'
  const winRelease = release && platform === 'win32'
  if (release && !['darwin', 'linux', 'win32'].includes(platform))
    throw new Error(`A release package builds on macOS, Linux or Windows, not ${platform}`)
  if (macRelease) requireMacCredentials(env)
  const azure = winRelease ? requireAzureCredentials(env) : undefined
  return {
    appId: APP_ID,
    productName: 'Coral',
    artifactName: 'coral-app-${os}-${arch}.${ext}',
    publish: release ? [UPDATE_FEED] : null,
    // The AppImage is unsigned; Linux has no platform signature to check.
    forceCodeSigning: macRelease || winRelease,
    directories: {
      output: 'dist',
      buildResources: 'resources',
    },
    files: ['out/**/*', 'package.json'],
    extraResources: [
      { from: 'resources/coral/', to: 'coral/' },
      { from: 'resources/reef/', to: 'reef/' },
      {
        from: 'resources/icons/',
        to: 'icons/',
        filter: ['icon.png', 'icon.ico', 'trayTemplate*.png'],
      },
    ],
    beforePack: async ({ electronPlatformName }) => {
      const runtime = join(
        'resources',
        'coral',
        electronPlatformName === 'win32' ? 'coral.exe' : 'coral',
      )
      const missing = [runtime, ...STAGED].filter(
        (path) => !existsSync(join(import.meta.dirname, path)),
      )
      if (missing.length)
        throw new Error(`Run \`npm run stage\` first. Missing: ${missing.join(', ')}`)
    },
    electronFuses: {
      runAsNode: false,
      enableNodeOptionsEnvironmentVariable: false,
      enableNodeCliInspectArguments: false,
      enableEmbeddedAsarIntegrityValidation: true,
      onlyLoadAppFromAsar: true,
      // Flipping fuses rewrites Electron's binaries and breaks their ad-hoc
      // signature, and Apple Silicon kills an app whose signature is invalid.
      // Re-sign ad-hoc, so an unsigned package runs. A release package is then
      // signed with Developer ID.
      resetAdHocDarwinSignature: true,
    },
    mac: {
      category: 'public.app-category.developer-tools',
      icon: 'resources/icons/icon.icns',
      // null keeps local packages unsigned instead of signing them with any
      // Developer ID certificate found in the keychain. xtask imports the
      // release identity into a keychain of its own, named by CSC_KEYCHAIN.
      identity: macRelease ? env.CSC_NAME : null,
      hardenedRuntime: true,
      notarize: macRelease,
      // Apple silicon only. `npm run stage` builds an arm64 runtime to match.
      target: [
        { target: 'dmg', arch: ['arm64'] },
        { target: 'zip', arch: ['arm64'] },
      ],
      // V8 needs JIT under the hardened runtime: Electron's, and the runtime's,
      // which runs Programs in V8 and aborts at startup without it. Every
      // nested binary, the runtime included, gets `entitlementsInherit`.
      entitlements: 'resources/entitlements.mac.plist',
      entitlementsInherit: 'resources/entitlements.mac.plist',
    },
    dmg: {
      // The mounted volume's name; the file name carries the version.
      title: 'Coral',
      // background@2x.png beside it serves Retina displays. The window takes
      // the 1x image's size, 640 × 420, including Finder's window bars.
      // Render both from background.svg with
      // scripts/render-dmg-background.mjs.
      background: 'resources/dmg/background.png',
      iconSize: 96,
      // Finder's bars leave about 360 points of visible background. Center
      // the whole row, including its labels, in that area. The arrow matches.
      contents: [
        { x: 200, y: 165, type: 'file' },
        { x: 440, y: 165, type: 'link', path: '/Applications' },
      ],
    },
    linux: {
      category: 'Development',
      // `coral` is the runtime binary's name, and `coral-desktop` is the 0.x
      // package, which apt would upgrade to this app.
      executableName: 'coral-app',
      // A directory, so electron-builder generates every hicolor icon size.
      icon: 'resources/icons',
      maintainer: 'Coral <support@withcoral.com>',
      synopsis: 'Coral desktop app',
      // Matches StartupWMClass to the WM_CLASS that Electron takes from
      // package.json `desktopName`, so the launcher claims its own window.
      syncDesktopName: true,
      target: [{ target: 'AppImage', arch: ['x64'] }],
    },
    deb: { packageName: 'coral-app' },
    win: {
      icon: 'resources/icons/icon.ico',
      target: [{ target: 'nsis', arch: ['x64'] }],
      // electron-builder signs Coral.exe, the runtime and the installer. The
      // publisher name also goes into app-update.yml: the updater installs
      // only an installer signed by that publisher.
      azureSignOptions: azure,
    },
    // Per user, with no UAC prompt, into %LOCALAPPDATA%\Programs\Coral. The
    // 0.x app installs there too, so users uninstall it first, as on macOS,
    // where both are Coral.app. The two apps still keep separate data folders.
    // The Start Menu entry is "Coral".
    nsis: {
      oneClick: false,
      perMachine: false,
      // The install mode page still offers all users; this disables that choice.
      allowElevation: false,
    },
  }
}

/** The values of `names`; throws naming the first that is empty or unset. */
function required(env: NodeJS.ProcessEnv, names: string[]): string[] {
  return names.map((name) => {
    const value = env[name]?.trim()
    // A missing GitHub secret arrives as an empty string.
    if (!value) throw new Error(`A release package needs ${name}`)
    return value
  })
}

// Azure Trusted Signing authenticates with a service principal from the
// environment, and signs with the account and profile the options name.
function requireAzureCredentials(env: NodeJS.ProcessEnv) {
  required(env, ['AZURE_TENANT_ID', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET'])
  const [publisherName, endpoint, codeSigningAccountName, certificateProfileName] = required(env, [
    'AZURE_TRUSTED_SIGNING_PUBLISHER_NAME',
    'AZURE_TRUSTED_SIGNING_ENDPOINT',
    'AZURE_TRUSTED_SIGNING_ACCOUNT_NAME',
    'AZURE_TRUSTED_SIGNING_CERTIFICATE_PROFILE_NAME',
  ])
  return { publisherName, endpoint, codeSigningAccountName, certificateProfileName }
}

// Notarization uses an App Store Connect API key. APPLE_API_KEY is the path
// to its .p8 file, which xtask decodes from a secret.
function requireMacCredentials(env: NodeJS.ProcessEnv) {
  const [keyPath] = required(env, [
    'APPLE_API_KEY',
    'APPLE_API_KEY_ID',
    'APPLE_API_ISSUER',
    'CSC_NAME',
  ])
  try {
    accessSync(keyPath, constants.R_OK)
    const key = statSync(keyPath)
    if (key.isFile() && key.size > 0) return
  } catch {}
  throw new Error('APPLE_API_KEY must name a readable, non-empty file')
}

export default createConfig()
