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

const testFile = /\.(?:test|spec)\.tsx?$/
// Coral UI takes no new Vitest coverage, so every test file it still carries is
// named here. Adding one is then a deliberate edit to this list, not a side
// effect of writing a feature.
const allowedTests = [
  '__tests__/architecture.test.ts',
  'auth/config.server.test.ts',
  'auth/coral-oauth.server.test.ts',
  'auth/csrf.server.test.ts',
  'auth/response.server.test.ts',
  'auth/safe-path.server.test.ts',
  'auth/session.server.test.ts',
  'lib/coral-endpoint.server.test.ts',
  'lib/coral-request-boundary.server.test.ts',
  'lib/coral-request.server.test.ts',
  'lib/mcp-connection.server.test.ts',
  'lib/onboarding-query.server.test.ts',
  'lib/runtime-config.server.test.ts',
  'lib/schema-explorer.test.ts',
  'lib/source-install-form.server.test.ts',
  'lib/source-oauth-install-flow.server.test.tsx',
  'lib/source-oauth-install-stream.test.ts',
  'lib/utils.test.ts',
  'lib/workspace-name.test.ts',
  'lib/workspace-routing.test.ts',
  'routes.test.ts',
  'routes/_protected.server.test.tsx',
  'routes/auth.callback.server.test.tsx',
  'routes/functions.server.test.ts',
  'routes/healthz.server.test.ts',
  'routes/login.server.test.tsx',
  'routes/logout.server.test.tsx',
  'routes/oauth-client-metadata.server.test.ts',
  'routes/onboarding.server.test.ts',
  'routes/readyz.server.test.ts',
  'routes/settings.server.test.ts',
  'routes/settings/runtime-features.server.test.ts',
  'routes/source-detail.server.test.tsx',
  'routes/source-discovery.server.test.ts',
  'routes/source-oauth-import.server.test.ts',
  'routes/source-oauth-install.server.test.ts',
  'routes/traces-loader.server.test.ts',
  'utils/format-time.test.ts',
]

function filesUnder(directory: string, matches: (name: string) => boolean): string[] {
  if (!fs.existsSync(directory)) return []

  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = path.join(directory, entry.name)
    if (entry.isDirectory()) return filesUnder(entryPath, matches)
    return entry.isFile() && matches(entry.name) ? [entryPath] : []
  })
}

function appTestFiles(): string[] {
  return filesUnder(appDir, (name) => testFile.test(name)).map((file) =>
    path.relative(appDir, file).split(path.sep).join('/'),
  )
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

/** A stylesheet that comes from a package rather than from app code. */
function isPackageStylesheet(specifier: string): boolean {
  return specifier.endsWith('.css') && !/^[.~]|^@\//.test(specifier)
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

  it('keeps test files out of presentation modules', () => {
    const violations = presentationDirs
      .flatMap((directory) => filesUnder(directory, (name) => testFile.test(name)))
      .map((file) => path.relative(appDir, file))

    expect(
      violations,
      'presentation modules are covered by Storybook and Chromatic, not Vitest. Delete the ' +
        'test rather than moving it: a single-use helper pulled into its own module so it has ' +
        'something to assert against is the shape this rule exists to stop, and relocating it ' +
        'to app/lib keeps the cost without adding the coverage. Write a Vitest case here only ' +
        'when asked, and put it beside the module that owns the behaviour.',
    ).toEqual([])
  })

  it('adds no Vitest coverage beyond the recorded set', () => {
    const recorded = new Set(allowedTests)

    expect(
      appTestFiles().filter((file) => !recorded.has(file)),
      'apps/coral-ui takes no new Vitest coverage. Delete the test: Storybook and Chromatic ' +
        'cover presentation, and the recorded suite already covers auth, session, discovery, ' +
        'and the formatting edges. Keep it only for a regression you can name, and add the ' +
        'path to allowedTests in the same commit that names it.',
    ).toEqual([])
  })

  it('records no test file that has been deleted', () => {
    const present = new Set(appTestFiles())

    expect(
      allowedTests.filter((file) => !present.has(file)),
      'allowedTests names a test file that no longer exists. Drop the entry so the list keeps ' +
        'describing the suite.',
    ).toEqual([])
  })

  it('keeps third party stylesheets in a layer', () => {
    const importedRaw = filesUnder(appDir, (name) => /\.tsx?$/.test(name)).flatMap((file) =>
      importsFrom(fs.readFileSync(file, 'utf8'))
        .filter(isPackageStylesheet)
        .map((specifier) => `${path.relative(appDir, file)} -> import '${specifier}'`),
    )
    const importedUnlayered = filesUnder(appDir, (name) => name.endsWith('.css')).flatMap((file) =>
      [...fs.readFileSync(file, 'utf8').matchAll(/@import\s+([^;]+);/g)]
        .filter((match) => !match[1].includes('layer('))
        .map((match) => `${path.relative(appDir, file)} -> @import ${match[1].trim()}`),
    )

    expect(
      [...importedRaw, ...importedUnlayered],
      'a third party stylesheet has to arrive through an @import that names a layer, the way ' +
        'app/wax/components/toast/toastify.css imports react-toastify. Imported straight from ' +
        'the package its rules are unlayered, an unlayered rule beats every layer whatever its ' +
        'specificity, and it silently outranks the wax rules written to restyle it.',
    ).toEqual([])
  })

  it('takes react-toastify from the entry point that ships no styles', () => {
    const importers = filesUnder(appDir, (name) => /\.tsx?$/.test(name)).filter((file) =>
      importsFrom(fs.readFileSync(file, 'utf8')).includes('react-toastify'),
    )

    expect(
      importers.map((file) => path.relative(appDir, file)),
      "react-toastify has to come from 'react-toastify/unstyled'. The default entry point " +
        'appends its stylesheet to the head as an inline style element when ToastContainer ' +
        'mounts. Those rules are unlayered, an unlayered rule beats every layer whatever its ' +
        'specificity, and they silently outrank both the vendor import in ' +
        'app/wax/components/toast/toastify.css and the wax rules written to restyle them.',
    ).toEqual([])
  })

  it('states one layer order for the whole app', () => {
    const globals = fs.readFileSync(path.join(appDir, 'styles', 'globals.css'), 'utf8')
    const layers = fs.readFileSync(path.join(appDir, 'wax', 'theme', 'layers.css.ts'), 'utf8')
    const stated = /@layer ([^;]+);/
      .exec(globals)?.[1]
      .split(',')
      .map((name) => name.trim())
    const declared = [...layers.matchAll(/globalLayer\('([^']+)'\)/g)].map((match) => match[1])

    expect(
      stated,
      'app/styles/globals.css states the layer order for the app and ' +
        'app/wax/theme/layers.css.ts declares the same layers for Vanilla Extract. Both have to ' +
        'name the same layers in the same order. A layer left out of the statement is created ' +
        'the first time a rule uses it, which puts it above everything already there.',
    ).toEqual(declared)
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
