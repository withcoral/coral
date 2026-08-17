import * as fs from 'node:fs'
import * as path from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const appDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const componentsDir = path.join(appDir, 'components')
const routesDir = path.join(appDir, 'routes')
const waxComponentsDir = path.join(appDir, 'wax', 'components')
const coralUIRoot = path.resolve(appDir, '..')
const viewsDir = path.join(appDir, 'views')
// Everything that renders. Data reaches it as loader data or through a fetcher;
// it never goes looking for data itself.
const presentationDirs = [componentsDir, viewsDir, waxComponentsDir]
const effectHooks = ['useEffect', 'useLayoutEffect', 'useInsertionEffect']
// Suspending inside an effect is how data loading keeps reappearing in
// components, so the shape is banned rather than any particular caller.
const suspendsInEffect = /\bawait\b|\.then\s*\(|\.catch\s*\(/
const dataSources = [
  {
    label: 'network call',
    pattern: /\bfetch\s*\(|\bnew (?:EventSource|WebSocket|XMLHttpRequest)\b/,
  },
  // The single handle onto the Electron host: reaching it is a request to
  // another process, whatever method follows.
  { label: 'desktop host access', pattern: /\bcoralDesktopApi\b|\bwindow\.coralDesktop\b/ },
]

function filesUnder(directory: string, matches: (name: string) => boolean): string[] {
  if (!fs.existsSync(directory)) return []

  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = path.join(directory, entry.name)
    if (entry.isDirectory()) return filesUnder(entryPath, matches)
    return entry.isFile() && matches(entry.name) ? [entryPath] : []
  })
}

function importsFrom(source: string): string[] {
  return [
    ...source.matchAll(/(?:import|from)\s+['"]([^'"]+)['"]/g),
    ...source.matchAll(/import\(['"]([^'"]+)['"]\)/g),
  ].map((match) => match[1])
}

function isRouteImport(importer: string, specifier: string): boolean {
  if (/^(?:@|~)\/routes(?:\/|$)/.test(specifier)) return true
  if (!specifier.startsWith('.')) return false

  const resolved = path.resolve(path.dirname(importer), specifier)
  return resolved === routesDir || resolved.startsWith(`${routesDir}${path.sep}`)
}

/** Every argument list passed to `hook`, so its callback can be read. */
function hookArguments(source: string, hook: string): string[] {
  const calls: string[] = []

  for (const match of source.matchAll(new RegExp(`\\b${hook}\\s*\\(`, 'g'))) {
    const open = match.index + match[0].length - 1
    let depth = 0

    for (let index = open; index < source.length; index += 1) {
      if (source[index] === '(') depth += 1
      else if (source[index] === ')' && --depth === 0) {
        calls.push(source.slice(open + 1, index))
        break
      }
    }
  }

  return calls
}

function isVisualTsx(name: string): boolean {
  return (
    name.endsWith('.tsx') &&
    !name.endsWith('.stories.tsx') &&
    !name.endsWith('.test.tsx') &&
    !name.endsWith('.spec.tsx') &&
    name !== 'index.tsx'
  )
}

describe('architecture', () => {
  it('keeps components independent from route modules', () => {
    const componentFiles = [componentsDir, waxComponentsDir].flatMap((directory) =>
      filesUnder(directory, (name) => /\.tsx?$/.test(name)),
    )
    const violations = componentFiles.flatMap((file) =>
      importsFrom(fs.readFileSync(file, 'utf8'))
        .filter((specifier) => isRouteImport(file, specifier))
        .map((specifier) => `${path.relative(appDir, file)} -> ${specifier}`),
    )

    expect(
      violations,
      'component modules must not import route modules. Take what you need as a prop and let ' +
        'the route module pass it in: a component that names a route cannot be rendered from ' +
        'anywhere else, and its story has to build a router to mount it',
    ).toEqual([])
  })

  it('keeps data fetching out of presentation modules', () => {
    const presentationFiles = presentationDirs
      .flatMap((directory) => filesUnder(directory, (name) => /\.tsx?$/.test(name)))
      .filter((file) => !/\.(?:test|spec|stories)\.tsx?$/.test(file) && !file.endsWith('.css.ts'))
    const violations = presentationFiles.flatMap((file) => {
      const source = fs.readFileSync(file, 'utf8')
      const findings = new Set([
        ...importsFrom(source)
          .filter((specifier) => /\.server(?:$|\/)/.test(specifier))
          .map((specifier) => `server module import: ${specifier}`),
        ...dataSources.filter(({ pattern }) => pattern.test(source)).map(({ label }) => label),
        ...effectHooks
          .filter((hook) => hookArguments(source, hook).some((body) => suspendsInEffect.test(body)))
          .map((hook) => `${hook} that awaits`),
      ])

      return [...findings].map((finding) => `${path.relative(appDir, file)} -> ${finding}`)
    })

    expect(
      violations,
      'presentation modules render loader data and submit through fetchers. Move the work into ' +
        'a route module rather than reshaping it here:\n' +
        '  reads -> export a loader, or clientLoader when only the browser can serve them, and ' +
        'render the result from loaderData (see routes/settings-loader.ts)\n' +
        '  writes -> export an action, or clientAction for the same reason, and submit to it ' +
        'with useFetcher, which also gives you the pending state (see ' +
        'routes/desktop-update-action.ts)\n' +
        '  pushed events, which no loader can express -> hold them in an atom that subscribes ' +
        'in onMount (see lib/desktop-update.ts)\n' +
        'Re-exporting the same call from app/lib or app/utils and importing it back is not a ' +
        'fix. This test stops looking there, but the request still runs outside the router, so ' +
        'it still has no caching, no pending state, and no revalidation. Nor is dropping the ' +
        'await while keeping the effect: a subscription belongs in an atom, and a request ' +
        'belongs in a handler.',
    ).toEqual([])
  })

  it('does not depend on Tailwind packages', () => {
    const packageJson = JSON.parse(
      fs.readFileSync(path.join(coralUIRoot, 'package.json'), 'utf8'),
    ) as {
      dependencies?: Record<string, unknown>
      devDependencies?: Record<string, unknown>
      optionalDependencies?: Record<string, unknown>
      peerDependencies?: Record<string, unknown>
    }
    const dependencyNames = [
      ...Object.keys(packageJson.dependencies ?? {}),
      ...Object.keys(packageJson.devDependencies ?? {}),
      ...Object.keys(packageJson.optionalDependencies ?? {}),
      ...Object.keys(packageJson.peerDependencies ?? {}),
    ]

    expect(dependencyNames.filter((name) => name.toLowerCase().includes('tailwind'))).toEqual([])
  })

  it('keeps the production entry dependency-light and import-safe', () => {
    const server = fs.readFileSync(path.join(coralUIRoot, 'server.js'), 'utf8')
    const allowedImports = new Set([
      'node:fs',
      'node:fs/promises',
      'node:http',
      'node:path',
      'node:stream',
      'node:stream/promises',
      'node:url',
      'react-router',
      './build/server/index.js',
    ])

    expect(importsFrom(server).filter((specifier) => !allowedImports.has(specifier))).toEqual([])
    expect(server).not.toMatch(/@react-router\/dev|react-router dev|vite|express/)
    expect(server).toContain('export async function startServer')
    expect(server).toContain('pathToFileURL(process.argv[1]).href === import.meta.url')
  })

  it('keeps a story in every visual Wax component directory', () => {
    const missingStories = fs
      .readdirSync(waxComponentsDir, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .filter((entry) => {
        const files = filesUnder(path.join(waxComponentsDir, entry.name), (name) =>
          name.endsWith('.tsx'),
        )
        return (
          files.some((file) => isVisualTsx(path.basename(file))) &&
          !files.some((file) => file.endsWith('.stories.tsx'))
        )
      })
      .map((entry) => `wax/components/${entry.name}`)

    expect(missingStories, 'visual Wax component directories must include a story').toEqual([])
  })

  it('keeps an adjacent story for every app component', () => {
    const missingStories = filesUnder(componentsDir, isVisualTsx)
      .filter((file) => !fs.existsSync(file.replace(/\.tsx$/, '.stories.tsx')))
      .map((file) => path.relative(appDir, file))

    expect(missingStories, 'app components must have adjacent same-name stories').toEqual([])
  })
})
