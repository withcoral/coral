import { accessSync, constants, statSync } from 'node:fs'

import type { Configuration } from 'electron-builder'

const API_KEY_NOTARIZATION_ENV = [
  'APPLE_API_KEY',
  'APPLE_API_KEY_ID',
  'APPLE_API_ISSUER',
] as const

function hasValue(env: NodeJS.ProcessEnv, name: string): boolean {
  return Boolean(env[name]?.trim())
}

function requireNotarizationCredentials(env: NodeJS.ProcessEnv): void {
  // app-builder-lib only warns and returns successfully when `notarize: true`
  // has no usable credentials. Coral's release path intentionally uses its
  // App Store Connect API key mode, so reject an incomplete setup here.
  const missing = API_KEY_NOTARIZATION_ENV.filter((name) => !hasValue(env, name))
  if (missing.length > 0) {
    throw new Error(
      `CORAL_DESKTOP_RELEASE=1 requires a complete App Store Connect API key credential set; missing ${missing.join(', ')}`,
    )
  }

  const keyPath = env.APPLE_API_KEY ?? ''
  let keyMetadata
  try {
    keyMetadata = statSync(keyPath)
    accessSync(keyPath, constants.R_OK)
  } catch {
    throw new Error('APPLE_API_KEY must point to a readable, non-empty regular file')
  }
  if (!keyMetadata.isFile() || keyMetadata.size === 0) {
    throw new Error('APPLE_API_KEY must point to a readable, non-empty regular file')
  }
}

export function createConfig(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
): Configuration {
  const releaseBuild = env.CORAL_DESKTOP_RELEASE === '1'
  if (releaseBuild) {
    // Release mode is the Apple signing and notarization path, and macOS is the
    // only platform with an update feed. Reject it on any other host so the
    // build fails on the flag rather than on a credential preflight it could
    // never satisfy.
    if (platform !== 'darwin') {
      throw new Error('CORAL_DESKTOP_RELEASE=1 is macOS-only; it drives Apple signing and notarization')
    }
    requireNotarizationCredentials(env)
  }

  return {
    appId: 'com.withcoral.desktop',
    productName: 'Coral',
    artifactName: 'coral-desktop-${os}-${arch}.${ext}',
    forceCodeSigning: releaseBuild,
    publish: [
      {
        provider: 'github',
        owner: 'withcoral',
        repo: 'coral',
      },
    ],
    directories: {
      output: 'dist',
      buildResources: 'resources',
    },
    files: ['out/**/*', 'package.json'],
    extraResources: [
      {
        from: 'resources/coral/',
        to: 'coral/',
        filter: ['**/*'],
      },
      {
        from: 'resources/icons/',
        to: 'icons/',
        filter: [
          'icon.icns',
          'icon-dark.icns',
          'icon.ico',
          'icon-dark.ico',
          'icon.png',
          'icon-dark.png',
          'icon-mac.png',
          'icon-dark-mac.png',
          'icon.svg',
        ],
      },
      {
        from: '../coral-ui/build/client/',
        to: 'app/',
        filter: ['**/*'],
      },
    ],
    mac: {
      category: 'public.app-category.developer-tools',
      entitlements: releaseBuild ? 'resources/entitlements.mac.plist' : null,
      entitlementsInherit: releaseBuild ? 'resources/entitlements.mac.inherit.plist' : null,
      hardenedRuntime: releaseBuild,
      icon: 'resources/icons/icon.icns',
      // identity null makes non-release builds deterministically unsigned;
      // otherwise electron-builder auto-discovers a Developer ID certificate
      // from the developer's keychain and signs local packages.
      identity: releaseBuild ? undefined : null,
      notarize: releaseBuild,
      target: ['dmg', 'zip'],
    },
    linux: {
      category: 'Development',
      // `coral` is the CLI's name. Claiming it for the Electron executable would
      // put a /usr/bin/coral symlink to the desktop app in the deb payload and
      // shadow the CLI the user installed.
      executableName: 'coral-desktop',
      // The directory, not icon.png. electron-builder returns a lone .png
      // source verbatim (iconConverter.js `set: source is already a .png`), so
      // naming the file installs a single 1024x1024 icon — a size hicolor's
      // index.theme does not declare, leaving launchers with no icon at all.
      // A directory routes icon.png through the generator and yields a real set.
      icon: 'resources/icons',
      // Debian requires a contact address for the Maintainer field, and fpm
      // refuses to build without one — `author` carries no email.
      maintainer: 'Coral Eng Team <eng@withcoral.com>',
      // Linux has no updater (see desktopUpdatesSupported in src/main/auto-update.ts).
      // `null` keeps electron-builder from writing a latest-linux.yml feed nobody
      // serves and from embedding app-update.yml in the package.
      publish: null,
      synopsis: 'Coral desktop app',
      // Electron sets the window's WM_CLASS from `desktopName` in package.json,
      // and electron-builder writes StartupWMClass from the same value. Without
      // it the two disagree — WM_CLASS is `withcoral-desktop`, derived from the
      // package name, while StartupWMClass would be `Coral` — and no desktop
      // environment can link a running window to the installed launcher.
      syncDesktopName: true,
      target: [
        { target: 'AppImage', arch: ['x64'] },
        { target: 'deb', arch: ['x64'] },
      ],
    },
    deb: {
      // Without this the package name falls back to the product name, `coral`,
      // which would collide with a future CLI package.
      packageName: 'coral-desktop',
    },
  }
}

const config = createConfig()

export default config
