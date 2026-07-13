import type { Configuration } from 'electron-builder'

const releaseBuild = process.env.CORAL_DESKTOP_RELEASE === '1'

const config: Configuration = {
  appId: 'com.withcoral.desktop',
  productName: 'Coral',
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
      from: '../reef/build/client/',
      to: 'app/',
      filter: ['**/*'],
    },
  ],
  mac: {
    artifactName: 'coral-desktop-mac-${arch}.${ext}',
    category: 'public.app-category.developer-tools',
    entitlements: 'resources/entitlements.mac.plist',
    entitlementsInherit: 'resources/entitlements.mac.inherit.plist',
    hardenedRuntime: true,
    icon: 'resources/icons/icon.icns',
    notarize: releaseBuild,
    target: ['dmg', 'zip'],
  },
}

export default config
