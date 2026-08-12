// This file has been automatically migrated to valid ESM format by Storybook.
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import type { PluginOption } from 'vite'
import type { StorybookConfig } from '@storybook/react-vite'

import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'

const __dirname = dirname(fileURLToPath(import.meta.url))

const config: StorybookConfig = {
  stories: [
    '../app/wax/**/*.stories.@(js|jsx|mjs|ts|tsx)',
    '../app/components/**/*.stories.@(js|jsx|mjs|ts|tsx)',
  ],
  addons: ['@chromatic-com/storybook', '@storybook/addon-docs', '@storybook/addon-a11y'],
  framework: {
    name: '@storybook/react-vite',
    options: {},
  },
  viteFinal: (config) => {
    const flattenPlugins = (plugins: PluginOption[] = []): PluginOption[] =>
      plugins.flatMap((plugin) => (Array.isArray(plugin) ? flattenPlugins(plugin) : [plugin]))

    config.plugins = flattenPlugins(config.plugins as PluginOption[]).filter((plugin) => {
      const name =
        typeof plugin === 'object' && plugin && !Array.isArray(plugin) && 'name' in plugin
          ? plugin.name
          : undefined
      // React Router's framework-mode Vite plugins require the React Router dev/build
      // runtime and fail inside Storybook's separate Vite preview server. Storybook only
      // needs React + the Wax styling plugin here.
      return typeof name !== 'string' || !name.startsWith('react-router')
    })
    config.plugins.push(vanillaExtractPlugin())

    config.resolve ??= {}
    const existingAliases = Array.isArray(config.resolve.alias)
      ? config.resolve.alias
      : Object.entries(config.resolve.alias ?? {}).map(([find, replacement]) => ({
          find,
          replacement,
        }))

    config.resolve.alias = [
      // Use array format to ensure specific aliases are matched first.
      { find: '@', replacement: resolve(__dirname, '../app') },
      { find: '~', replacement: resolve(__dirname, '../app') },
      ...existingAliases,
    ]

    config.optimizeDeps ??= {}
    const existingOptimizeIncludes = config.optimizeDeps.include ?? []
    config.optimizeDeps.include = [
      ...new Set([
        ...existingOptimizeIncludes,
        '@bufbuild/protobuf',
        '@bufbuild/protobuf/codegenv2',
      ]),
    ]

    return config
  },
}
export default config
