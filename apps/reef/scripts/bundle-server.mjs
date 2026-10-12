// Bundles Reef's production server, its React Router build and every
// dependency into one ESM file. Docker and Desktop ship that file plus the
// client build, and no node_modules:
//
//   <root>/server.mjs
//   <root>/build/client/
//
// server.js finds `./build/client/` beside itself, so the bundle keeps that
// layout: it lands in the parent of the build directory.
import { build } from 'esbuild'
import { readdir } from 'node:fs/promises'
import { isBuiltin } from 'node:module'
import { basename, dirname, join, relative, resolve, sep } from 'node:path'
import { pathToFileURL } from 'node:url'

const reefRoot = resolve(import.meta.dirname, '..')

/**
 * Writes `server.mjs` beside `buildDirectory` and returns its path. Rejects a
 * bundle that would still need a file it does not contain: a package resolved
 * at run time, or a native addon.
 */
export async function bundleServer({ entry, buildDirectory }) {
  if (basename(buildDirectory) !== 'build')
    throw new Error(`The build directory must be named "build": ${buildDirectory}`)
  const outfile = join(dirname(buildDirectory), 'server.mjs')
  const result = await build({
    entryPoints: [entry],
    outfile,
    bundle: true,
    platform: 'node',
    format: 'esm',
    target: 'node24',
    // React and React Router pick their production code from NODE_ENV, and
    // server.js forces production anyway.
    define: { 'process.env.NODE_ENV': '"production"' },
    // CommonJS dependencies call require() for Node built-ins, which ESM lacks.
    banner: {
      js: "import { createRequire as __reefCreateRequire } from 'node:module';\nconst require = __reefCreateRequire(import.meta.url);",
    },
    plugins: [buildDirectoryPlugin(entry, buildDirectory)],
    // A build staged outside the checkout still resolves Reef's dependencies.
    nodePaths: [join(dirname(entry), 'node_modules')],
    // esbuild leaves a require() of a computed name for run time, when no
    // node_modules exists to answer it. React Router's computed import()
    // calls load route modules in the browser, so those stay allowed.
    logOverride: { 'unsupported-require-call': 'error' },
    metafile: true,
    legalComments: 'eof',
    logLevel: 'silent',
  }).catch((error) => {
    const messages = error.errors?.map(formatMessage) ?? [String(error)]
    throw new Error(`Reef server bundle failed:\n${messages.join('\n')}`, { cause: error })
  })
  const inputs = Object.keys(result.metafile.inputs)
  const unresolved = Object.values(result.metafile.outputs)
    .flatMap((output) => output.imports)
    .filter((imported) => imported.external && !isBuiltin(imported.path))
    .map((imported) => imported.path)
  if (unresolved.length)
    throw new Error(`The Reef server bundle would resolve at run time: ${unresolved.join(', ')}`)
  const addons = await nativeAddons(inputs.map((input) => resolve(input)))
  if (addons.length)
    throw new Error(`The Reef server bundle cannot carry a native addon: ${addons.join(', ')}`)
  return outfile
}

// server.js imports its build through the literal path './build/server/index.js'.
// Point that import at the build being bundled, wherever it was written.
function buildDirectoryPlugin(entry, buildDirectory) {
  return {
    name: 'reef-build-directory',
    setup(builder) {
      builder.onResolve({ filter: /^\.\/build\/server\/index\.js$/ }, (args) =>
        resolve(args.importer) === resolve(entry)
          ? { path: join(buildDirectory, 'server', 'index.js') }
          : undefined,
      )
    },
  }
}

function formatMessage(message) {
  const where = message.location ? `${message.location.file}:${message.location.line}: ` : ''
  return `${where}${message.text}`
}

// A package that loads a `.node` file computes its path at run time, so
// esbuild cannot see the addon. Look for one in every bundled package.
async function nativeAddons(inputs) {
  const packages = new Set()
  for (const input of inputs) {
    const parts = input.split(sep)
    const index = parts.lastIndexOf('node_modules')
    if (index === -1) continue
    const length = parts[index + 1]?.startsWith('@') ? 3 : 2
    packages.add(parts.slice(0, index + length).join(sep))
  }
  const found = []
  for (const directory of packages) {
    for (const entry of await readdir(directory, { recursive: true, withFileTypes: true })) {
      const path = join(entry.parentPath, entry.name)
      if (
        entry.isFile() &&
        entry.name.endsWith('.node') &&
        !relative(directory, path).split(sep).includes('node_modules')
      )
        found.push(path)
    }
  }
  return found
}

if (process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url) {
  // Desktop builds into its own staging directory, so concurrent work in this
  // checkout keeps its build. React Router reads the same variable.
  const buildDirectory = resolve(reefRoot, process.env.REEF_BUILD_DIRECTORY || 'build')
  const outfile = await bundleServer({ entry: join(reefRoot, 'server.js'), buildDirectory })
  console.log(`Bundled the Reef server into ${relative(process.cwd(), outfile)}`)
}
