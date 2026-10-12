// Bundles the Electron main process. Desktop has no renderer or preload bundle:
// the window loads Reef from its own server.
import { build } from 'esbuild'
import { resolve } from 'node:path'

const desktopRoot = resolve(import.meta.dirname, '..')

await build({
  entryPoints: [resolve(desktopRoot, 'src/main/index.ts')],
  outfile: resolve(desktopRoot, 'out/main/index.js'),
  bundle: true,
  platform: 'node',
  format: 'esm',
  target: 'node24',
  external: ['electron'],
  // electron-updater is CommonJS. Its bundled `require` calls for Electron and
  // Node's built-in modules need a `require` that ESM does not define.
  banner: {
    js: "import { createRequire as __createRequire } from 'node:module'; const require = __createRequire(import.meta.url);",
  },
  loader: { '.svg': 'text' },
  sourcemap: 'linked',
  logLevel: 'warning',
})
