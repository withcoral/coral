// Plain JavaScript: Node loads this file from an app's Vite config, and Node does
// not strip types from a file in node_modules.

import { readFileSync } from 'node:fs'

const manifest = JSON.parse(readFileSync(new URL('./package.json', import.meta.url), 'utf8'))
const peers = Object.keys(manifest.peerDependencies)
const isPeer = (specifier) =>
  peers.some((peer) => specifier === peer || specifier.startsWith(`${peer}/`))

/**
 * Each specifier that Wax imports at run time. Reef's architecture test keeps the
 * list equal to the imports.
 */
export const runtimeImports = [
  '@base-ui/react/avatar',
  '@base-ui/react/combobox',
  '@base-ui/react/dialog',
  '@base-ui/react/field',
  '@base-ui/react/menu',
  '@base-ui/react/number-field',
  '@base-ui/react/popover',
  '@base-ui/react/radio',
  '@base-ui/react/radio-group',
  '@base-ui/react/scroll-area',
  '@base-ui/react/switch',
  '@base-ui/react/tabs',
  '@base-ui/react/tooltip',
  '@internationalized/date',
  '@lezer/highlight',
  '@lezer/javascript',
  '@lezer/json',
  '@tanstack/react-virtual',
  '@vanilla-extract/css',
  '@vanilla-extract/recipes',
  '@vanilla-extract/recipes/createRuntimeFn',
  'classnames',
  'jotai',
  'jotai/utils',
  'lucide-react',
  'react',
  'react-aria',
  'react-aria-components',
  'react-markdown',
  'react-router',
  'react-toastify/unstyled',
  'tinykeys',
]

/**
 * The Vite settings that an app needs to build Wax from source. Add it to `plugins`
 * in each Vite config: the app, Vitest and, through them, Storybook.
 */
export function wax() {
  return {
    name: 'withcoral-wax',
    config() {
      return {
        optimizeDeps: {
          // The dev server must not pre-bundle Wax into one file: the Vanilla Extract
          // plugin must see each `.css.ts` file.
          exclude: [manifest.name],
          // Vite pre-bundles nothing that an excluded package imports, so name each
          // import. Several are CommonJS, which a browser cannot load as it is.
          include: runtimeImports.map((specifier) =>
            isPeer(specifier) ? specifier : `${manifest.name} > ${specifier}`,
          ),
        },
      }
    },
  }
}
