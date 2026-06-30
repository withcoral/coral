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
      from: 'resources/icons/',
      to: 'icons/',
      filter: ['icon.icns', 'icon.ico', 'icon.png', 'icon-dark.png', 'icon.svg'],
    },
    {
      from: '../reef/build/client/',
      to: 'reef/',
      filter: ['**/*'],
    },
  ],
  mac: {
    category: 'public.app-category.developer-tools',
    icon: 'resources/icons/icon.icns',
    target: ['dmg'],
  },
}

export default config
