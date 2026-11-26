// SPIKE (withcoral/coral#3541). Throwaway: never merge this to main.
//
// Assembles `@withcoral/wax` as a package of TypeScript source from Reef, without
// changing Reef, and packs it with `npm pack`.
//
//   node spikes/wax-package/build.ts [--imports relative|alias] [--no-tests] [--no-stories]
//
// `--imports relative` (the default) rewrites each `@/wax/...` import inside the
// package to a relative path, so the package resolves with no alias in the app.
// `--imports alias` keeps `@/wax/...`, so the app must map `@/wax` to the package.
// `--no-tests` leaves the unit tests and their helper out. `--no-stories` leaves the
// stories and the Storybook helpers out.

import { execFileSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'

const here = import.meta.dirname
const reef = path.resolve(here, '../../apps/reef')
const out = path.join(here, 'out')
const pkg = path.join(out, 'package')

const args = process.argv.slice(2)
const imports = args.includes('--imports') ? args[args.indexOf('--imports') + 1] : 'relative'
if (imports !== 'relative' && imports !== 'alias') throw new Error(`--imports ${imports}?`)
const tests = !args.includes('--no-tests')
const stories = !args.includes('--no-stories')

/** Reef paths outside `app/wax` that travel with Wax, and where each lands in the package. */
const moved: [from: string, to: string, only?: 'tests' | 'stories'][] = [
  ['app/styles/utils.ts', 'styles/utils.ts'],
  ['app/styles/breakpoints.ts', 'styles/breakpoints.ts'],
  ['app/utils/filter-query.ts', 'utils/filter-query.ts'],
  ['app/utils/filter-query.test.ts', 'utils/filter-query.test.ts', 'tests'],
  ['app/utils/compose-refs.ts', 'utils/compose-refs.ts'],
  ['app/utils/count-label.ts', 'utils/count-label.ts'],
  ['app/utils/download.ts', 'utils/download.ts'],
  ['app/utils/use-truncation-detection.ts', 'utils/use-truncation-detection.ts'],
  [
    'app/utils/use-truncation-detection.test.tsx',
    'utils/use-truncation-detection.test.tsx',
    'tests',
  ],
  ['app/test-support/route.tsx', 'test-support/route.tsx', 'tests'],
  ['.storybook/browser.ts', 'storybook/browser.ts', 'stories'],
  ['.storybook/addons/theme-comparison', 'storybook/theme-comparison', 'stories'],
  ['public/fonts', 'fonts'],
]

/** Import specifiers that reach outside `app/wax` today, and their place in the package. */
const respecified: [from: RegExp, to: string][] = [
  [/^@\/styles\//, '@/wax/styles/'],
  [/^@\/utils\//, '@/wax/utils/'],
  [/^@\/test-support\//, '@/wax/test-support/'],
  [/^(\.\.\/)+\.storybook\/browser$/, '@/wax/storybook/browser'],
]

const isTestFile = (file: string) => /\.(test|stories)\.tsx?$/.test(file)
const ships = (file: string) =>
  (tests || !/\.test\.tsx?$/.test(file)) && (stories || !/\.stories\.tsx$/.test(file))
const kept = { tests, stories }

fs.rmSync(out, { recursive: true, force: true })
fs.mkdirSync(pkg, { recursive: true })

fs.cpSync(path.join(reef, 'app/wax'), pkg, {
  recursive: true,
  filter: ships,
})
for (const [from, to, only] of moved) {
  if (only && !kept[only]) continue
  fs.cpSync(path.join(reef, from), path.join(pkg, to), { recursive: true })
}

// `shellSignal` is Reef's: it reads Reef's required-actions stream. The rest of the
// file is a memory router for any test.
if (tests) {
  const route = path.join(pkg, 'test-support/route.tsx')
  const text = fs
    .readFileSync(route, 'utf8')
    .replace(/^import \{ ActionSignalContext.*\n/m, '')
    .replace(/\/\*\*\n \* The app shell's required-actions signal[\s\S]*?\n\}\n\n/, '')
  if (text.includes('ActionSignal')) throw new Error('route.tsx still names ActionSignal')
  fs.writeFileSync(route, text)
}

// The fonts ship in the package, so the stylesheet names them by a relative URL and
// the app's bundler emits them. Reef serves them from `public/fonts` today.
const reset = path.join(pkg, 'theme/reset.css')
fs.writeFileSync(reset, fs.readFileSync(reset, 'utf8').replaceAll("url('/fonts/", "url('../fonts/"))

const counts = { respecified: 0, relative: 0, files: 0 }
for (const file of walk(pkg)) {
  if (!/\.tsx?$/.test(file)) continue
  const before = fs.readFileSync(file, 'utf8')
  const after = before.replace(
    /(\bfrom\s+|\bimport\s+|\bimport\(\s*|\bmock\(\s*)(['"])([^'"\n]+)\2/g,
    (whole, lead: string, quote: string, specifier: string) => {
      let next = specifier
      for (const [from, to] of respecified) {
        if (from.test(next)) {
          next = next.replace(from, to)
          counts.respecified++
        }
      }
      if (imports === 'relative' && /^@\/wax(\/|$)/.test(next)) {
        const target = path.join(pkg, next.slice('@/wax'.length))
        const relative = path.relative(path.dirname(file), target)
        next = relative.startsWith('.') ? relative : `./${relative}`
        counts.relative++
      }
      return next === specifier ? whole : `${lead}${quote}${next}${quote}`
    },
  )
  if (after !== before) {
    fs.writeFileSync(file, after)
    counts.files++
  }
}

const left = [...walk(pkg)]
  .filter((file) => /\.tsx?$/.test(file))
  .flatMap((file) =>
    [...fs.readFileSync(file, 'utf8').matchAll(/['"](@\/[^'"\n]*|(?:\.\.\/)+\.storybook[^'"\n]*)['"]/g)]
      .filter(([, specifier]) => imports === 'relative' || !specifier.startsWith('@/wax'))
      .map(([, specifier]) => `${path.relative(pkg, file)}: ${specifier}`),
  )
if (left.length) throw new Error(`Imports still reach outside the package:\n${left.join('\n')}`)

const reefManifest = JSON.parse(fs.readFileSync(path.join(reef, 'package.json'), 'utf8'))
const range = (name: string): string => {
  const found = reefManifest.dependencies[name] ?? reefManifest.devDependencies[name]
  if (!found) throw new Error(`Reef does not depend on ${name}`)
  return found
}
const ranges = (names: string[]) => Object.fromEntries(names.toSorted().map((n) => [n, range(n)]))
// A peer holds state that the app shares with Wax (a React context, a store, the
// Vanilla Extract file scope), so the app must have exactly one copy of it.
const peers = [
  'react',
  'react-dom',
  'react-router',
  'jotai',
  '@vanilla-extract/css',
  '@vanilla-extract/vite-plugin',
]
// Only stories, tests and their helpers import these.
const testPeers = [
  'storybook',
  '@storybook/react-vite',
  '@storybook/icons',
  'vitest',
  '@testing-library/react',
  '@testing-library/user-event',
  'react-hook-form',
]
const dependencies = [
  '@base-ui/react',
  '@internationalized/date',
  '@lezer/highlight',
  '@lezer/javascript',
  '@lezer/json',
  '@tanstack/react-virtual',
  '@vanilla-extract/recipes',
  'classnames',
  'lucide-react',
  'react-aria',
  'react-aria-components',
  'react-markdown',
  'react-toastify',
  'tinykeys',
]

// Every bare import in the package must be declared, or an app that does not happen
// to install the same package fails to resolve it.
const declared = new Set([...peers, ...testPeers, ...dependencies])
const undeclared = new Set<string>()
const imported = new Set<string>()
for (const file of walk(pkg)) {
  const css = file.endsWith('.css')
  if (!css && !/\.tsx?$/.test(file)) continue
  const pattern = css
    ? /@import\s+(['"])([^'"\n]+)\1/g
    : /(?:\bfrom\s+|\bimport\s+|\bimport\(\s*)(['"])([^'"\n]+)\1/g
  for (const [, , specifier] of fs.readFileSync(file, 'utf8').matchAll(pattern)) {
    if (/^[.@]\/|^\.\.?(\/|$)|^node:/.test(specifier) || specifier.startsWith('@/')) continue
    const name = specifier.split('/').slice(0, specifier.startsWith('@') ? 2 : 1).join('/')
    // `@vitest/browser/context` is the path addon-vitest 10 uses; the app aliases it.
    if (!declared.has(name) && name !== '@vitest/browser') undeclared.add(name)
    imported.add(name === '@vitest/browser' ? 'vitest' : name)
  }
}
if (undeclared.size) throw new Error(`Undeclared imports: ${[...undeclared].join(', ')}`)
// An optional peer is one that only the stories and tests in this tarball import.
const optionalPeers = testPeers.filter((name) => imported.has(name))

// What the browser loads from outside the package at run time. An app's dev server
// must pre-bundle each one: `vite.js` below hands Vite this list.
const runtime = new Set<string>(['@vanilla-extract/recipes/createRuntimeFn'])
for (const file of walk(pkg)) {
  const relative = path.relative(pkg, file)
  // A `.css.ts` file is evaluated at build time and leaves only its class names.
  if (!/\.tsx?$/.test(file) || isTestFile(file) || relative.endsWith('.css.ts')) continue
  if (/^(storybook|test-support)\//.test(relative)) continue
  const source = fs.readFileSync(file, 'utf8')
  for (const [, , specifier] of source.matchAll(
    /^(?:import|export)\s+(?!type\b)[^'"]*?\bfrom\s+(['"])([^'"\n]+)\1/gm,
  )) {
    if (!specifier.startsWith('.') && !specifier.startsWith('@/')) runtime.add(specifier)
  }
}
fs.writeFileSync(
  path.join(pkg, 'vite.js'),
  `// Generated by the package build. Plain JavaScript: Node loads it from vite.config.ts,
// and Node does not strip types from a file in node_modules.

const name = '@withcoral/wax'

/** What the package imports at run time: its dependencies first, then its peers. */
export const runtimeImports = ${JSON.stringify([...runtime].toSorted(), null, 2)}

const peers = ${JSON.stringify(peers)}
const isPeer = (specifier) => peers.some((peer) => specifier === peer || specifier.startsWith(peer + '/'))

/**
 * The Vite settings an app needs to build Wax from source. Add it to \`plugins\` in
 * vite.config.ts, in the Vitest config and, through them, in Storybook.
 */
export function wax() {
  return {
    name: 'withcoral-wax',
    config() {
      return {
        optimizeDeps: {
          // The dev server must not pre-bundle the package: the Vanilla Extract plugin
          // has to see each .css.ts file.
          exclude: [name],
          // Vite pre-bundles nothing that an excluded package imports, so name each
          // import. Several are CommonJS, which a browser cannot load as it is.
          include: runtimeImports.map((specifier) =>
            isPeer(specifier) ? specifier : name + ' > ' + specifier,
          ),
        },
      }
    },
  }
}
`,
)
fs.writeFileSync(
  path.join(pkg, 'vite.d.ts'),
  `import type { Plugin } from 'vite'

export declare const runtimeImports: string[]
export declare function wax(): Plugin
`,
)
const sha = execFileSync('git', ['rev-parse', '--short=9', 'HEAD'], { cwd: here })
  .toString()
  .trim()
const manifest = {
  name: '@withcoral/wax',
  version: `0.0.0-spike.${sha}`,
  description: 'Wax, the Coral design system, as TypeScript source for a Vite app.',
  license: 'Apache-2.0',
  type: 'module',
  repository: { type: 'git', url: 'git+https://github.com/withcoral/coral.git' },
  // No `exports` and no `main`: the package has no entry point and no build. An app
  // imports a file by its path, such as `@withcoral/wax/components/button`, and its
  // bundler finds `button/index.ts` as it does for the app's own files.
  dependencies: ranges(dependencies),
  peerDependencies: ranges([...peers, ...optionalPeers]),
  peerDependenciesMeta: optionalPeers.length
    ? Object.fromEntries(optionalPeers.toSorted().map((name) => [name, { optional: true }]))
    : undefined,
}
fs.writeFileSync(path.join(pkg, 'package.json'), `${JSON.stringify(manifest, null, 2)}\n`)

const packed = JSON.parse(
  execFileSync('npm', ['pack', '--json', '--pack-destination', out], { cwd: pkg }).toString(),
)[0]
console.log(
  JSON.stringify(
    {
      tarball: path.join(out, packed.filename),
      imports,
      tests,
      stories,
      files: packed.entryCount,
      packedKb: Math.round(packed.size / 1024),
      unpackedKb: Math.round(packed.unpackedSize / 1024),
      rewritten: counts,
    },
    null,
    2,
  ),
)

function* walk(dir: string): Generator<string> {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name)
    if (entry.isDirectory()) yield* walk(full)
    else yield full
  }
}
