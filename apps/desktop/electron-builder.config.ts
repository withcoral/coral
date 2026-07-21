import { execFileSync } from 'node:child_process'
import { accessSync, constants, statSync } from 'node:fs'
import { join } from 'node:path'

import type { AfterPackContext, Configuration } from 'electron-builder'

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

export function verifyUniversalReleaseSidecar(context: AfterPackContext): void {
  const appName = `${context.packager.appInfo.productFilename}.app`
  const sidecarPath = join(
    context.appOutDir,
    appName,
    'Contents',
    'Resources',
    'coral',
    'coral',
  )

  let sidecarMetadata
  try {
    sidecarMetadata = statSync(sidecarPath)
    accessSync(sidecarPath, constants.R_OK | constants.X_OK)
  } catch {
    throw new Error(`release desktop sidecar must be a readable, executable file: ${sidecarPath}`)
  }
  if (!sidecarMetadata.isFile() || sidecarMetadata.size === 0) {
    throw new Error(`release desktop sidecar must be a readable, executable file: ${sidecarPath}`)
  }

  const architectures = execFileSync('lipo', ['-archs', sidecarPath], {
    encoding: 'utf8',
  })
    .trim()
    .split(/\s+/)
    .sort()
  if (
    architectures.length !== 2 ||
    architectures[0] !== 'arm64' ||
    architectures[1] !== 'x86_64'
  ) {
    throw new Error(
      `release desktop sidecar must contain exactly arm64 and x86_64; lipo reported '${architectures.join(' ')}'`,
    )
  }

  console.info(
    `[desktop-package] verified pre-sign sidecar architectures: ${architectures.join(' ')}`,
  )
}

export function createConfig(env: NodeJS.ProcessEnv = process.env): Configuration {
  const releaseBuild = env.CORAL_DESKTOP_RELEASE === '1'
  if (releaseBuild) requireNotarizationCredentials(env)

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
    // afterPack runs after Coral.app is assembled but before electron-builder
    // signs it, so release packaging fails before signing begins if the staged
    // sidecar lost either architecture.
    afterPack: releaseBuild ? verifyUniversalReleaseSidecar : undefined,
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
        from: '../reef/build/client/',
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
  }
}

const config = createConfig()

export default config
