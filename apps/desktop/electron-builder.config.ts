import { accessSync, constants, statSync } from 'node:fs'

import type { Configuration } from 'electron-builder'

// Release mode ships an active updater, so it only applies where the app can
// replace itself: the macOS app and the Linux AppImage. Windows has no updater
// and no signing story yet.
const RELEASE_PLATFORMS: NodeJS.Platform[] = ['darwin', 'linux']

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
  // Reject the flag up front, so the build fails on the flag rather than on a
  // preflight it could never satisfy.
  const releaseBuild = env.CORAL_DESKTOP_RELEASE === '1'
  if (releaseBuild && !RELEASE_PLATFORMS.includes(platform)) {
    throw new Error(
      `CORAL_DESKTOP_RELEASE=1 supports ${RELEASE_PLATFORMS.join(', ')} hosts only, not ${platform}`,
    )
  }
  // Signing and notarization are the macOS half of release mode; the AppImage
  // updates itself unsigned.
  const appleRelease = releaseBuild && platform === 'darwin'
  if (appleRelease) requireNotarizationCredentials(env)

  return {
    appId: 'com.withcoral.desktop',
    productName: 'Coral',
    artifactName: 'coral-desktop-${os}-${arch}.${ext}',
    forceCodeSigning: appleRelease,
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
      entitlements: appleRelease ? 'resources/entitlements.mac.plist' : null,
      entitlementsInherit: appleRelease ? 'resources/entitlements.mac.inherit.plist' : null,
      hardenedRuntime: appleRelease,
      icon: 'resources/icons/icon.icns',
      // identity null makes non-release builds deterministically unsigned;
      // otherwise electron-builder auto-discovers a Developer ID certificate
      // from the developer's keychain and signs local packages.
      identity: appleRelease ? undefined : null,
      notarize: appleRelease,
      target: ['dmg', 'zip'],
    },
    linux: {
      category: 'Development',
      // `coral` is the CLI's name. Claiming it for the Electron executable would
      // put a /usr/bin/coral symlink to the desktop app in the deb payload and
      // shadow the CLI the user installed.
      executableName: 'coral-desktop',
      // The directory, not icon.png: electron-builder passes a lone .png through
      // verbatim, installing one 1024x1024 icon at a size hicolor's index.theme
      // does not declare. A directory routes it through the size generator.
      icon: 'resources/icons',
      // Debian requires a contact address for the Maintainer field, and fpm
      // refuses to build without one — `author` carries no email.
      maintainer: 'Coral Eng Team <eng@withcoral.com>',
      synopsis: 'Coral desktop app',
      // Makes StartupWMClass match the WM_CLASS Electron takes from `desktopName`
      // in package.json. Without it the launcher cannot claim its own window.
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
      // Keeps the deb out of latest-linux.yml. FpmTarget reports the deb with
      // `isWriteUpdateInfo: true` whenever a publish config resolves, and
      // writeUpdateInfoFiles() merges every reported artifact into the one feed.
      // The sort there breaks ties by zip-vs-non-zip and then arch, which the
      // deb and the AppImage tie on, so the top-level `path:` the updater reads
      // would come from whichever target hashed first. A target-level null
      // short-circuits getPublishConfigs() for the artifact event, so the feed
      // names the AppImage and nothing else.
      //
      // It does not stop FpmTarget writing `resources/package-type` — that call
      // passes the target options through but resolves against `linux.publish`
      // and the global config only. auto-update.ts names the updater class for
      // that reason.
      publish: null,
    },
    win: {
      // The file, not the directory. The .png trap the linux block works around
      // is PNG-only: electron-builder validates that an .ico carries a >=256
      // entry and then uses it as is, and resources/icons/icon.ico carries
      // 16/24/32/48/64/128/256.
      icon: 'resources/icons/icon.ico',
      // Windows ships no updater, so desktopUpdatesSupported() is false there.
      // `null` keeps electron-builder from writing a latest.yml feed nobody
      // serves and from embedding app-update.yml in the package.
      // getPublishConfigs reads this platform block before the top-level
      // `publish`, so null short-circuits it.
      publish: null,
      // No executableName: the deb symlinks its executable into /usr/bin and
      // would shadow the `coral` CLI, which is why linux renames it. The
      // installer writes Coral.exe into its own directory, so there is nothing
      // to collide with.
      target: [{ target: 'nsis', arch: ['x64'] }],
    },
    nsis: {
      // An assisted installer, so the user can pick a directory.
      allowToChangeInstallationDirectory: true,
      // A blockmap is differential-update metadata. NsisTarget gates it on
      // `!isPortable && differentialPackage !== false` — not on `publish` — so
      // without this a coral-desktop-win-x64.exe.blockmap lands in dist even
      // with publish: null, and release.yml's `coral-*.blockmap` glob would
      // sweep it into the GitHub release.
      differentialPackage: false,
      oneClick: false,
      // Per-user, under %LOCALAPPDATA%. A per-machine install puts
      // resourcesPath under C:\Program Files, which a standard user cannot
      // write to, and costs a UAC prompt for a developer tool that needs none.
      perMachine: false,
    },
  }
}

const config = createConfig()

export default config
