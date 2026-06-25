import type { Configuration } from 'electron-builder'

const config: Configuration = {
  appId: 'com.withcoral.desktop',
  productName: 'Coral',
  artifactName: 'coral-desktop-${os}-${arch}.${ext}',
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
      from: 'resources/bin/',
      to: 'bin/',
      filter: ['**/*'],
    },
    {
      from: 'resources/icons/',
      to: 'icons/',
      filter: ['**/*'],
    },
  ],
  mac: {
    category: 'public.app-category.developer-tools',
    icon: 'resources/icons/icon.icns',
    target: ['dmg', 'zip', 'pkg'],
  },
  pkg: {
    allowAnywhere: false,
    allowCurrentUserHome: false,
    allowRootDirectory: true,
    installLocation: '/Applications',
  },
  win: {
    icon: 'resources/icons/icon.ico',
    target: ['nsis'],
  },
  nsis: {
    oneClick: true,
    perMachine: false,
  },
  linux: {
    category: 'Development',
    icon: 'resources/icons/icon.png',
    target: ['AppImage', 'deb', 'rpm'],
  },
}

export default config
