/**
 * Architectural/Structural Tests
 *
 * These tests enforce "taste invariants" - rules about code organization and patterns
 * that ensure the codebase follows architectural conventions. They act as guardrails
 * for developers and AI agents working on the codebase.
 *
 * Based on the OpenAI "harness engineering" approach.
 * frontend and adapted to reef's layout (`app/` source root, wax design system under
 * `app/wax/components`, app-level components under `app/components`, routes under
 * `app/routes`). The file-level storybook-coverage rule targets `app/components`, which
 * doesn't exist yet — it'll start enforcing once those components are added.
 *
 * ## How these tests work:
 *
 * Each test has a BASELINE count representing known existing violations. The test
 * will FAIL if:
 *   - New violations are introduced (count > baseline)
 *   - The baseline is too high after fixing violations (count < baseline - encourages lowering it)
 *
 * ## Fixing violations:
 *
 * 1. Run the tests to see current violations: `npm test -- --project=architecture`
 * 2. Fix the violations in your code
 * 3. Lower the BASELINE constant in this file
 * 4. Repeat until baseline reaches 0
 *
 * ## Running just these tests:
 *
 * ```bash
 * npm test -- --project=architecture
 * ```
 */

import * as fs from 'node:fs'
import * as path from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const testsDir = path.dirname(fileURLToPath(import.meta.url))

const APP_SRC = path.resolve(testsDir, '..')
const COMPONENTS_DIR = path.join(APP_SRC, 'components')
const ROUTE_CONFIG_FILE = path.join(APP_SRC, 'routes.ts')
const ROUTES_DIR = path.join(APP_SRC, 'routes')
const WAX_COMPONENTS_DIR = path.join(APP_SRC, 'wax', 'components')
const REEF_ROOT = path.resolve(APP_SRC, '..')
const DESKTOP_SRC_DIR = path.resolve(REEF_ROOT, '..', 'desktop', 'src')
const DESKTOP_MAIN_DIR = path.join(DESKTOP_SRC_DIR, 'main')

// Rules that apply to both the wax design system and app-level components.
const ALL_COMPONENT_DIRS = [WAX_COMPONENTS_DIR, COMPONENTS_DIR]

/**
 * Baseline counts for known violations. Lower these as violations are fixed.
 * Set to 0 to enforce the rule strictly (no violations allowed).
 */
const BASELINES = {
  componentIsolation: 0, // Wax components importing from routes
  storybookCoverageComponents: 0, // app/components files missing stories (dir not created yet)
  storybookCoverageWax: 0, // Wax component directories missing stories
  tailwindInComponents: 0, // Wax components using inline Tailwind
}

/**
 * Build a dependency graph for route files
 */
function buildRouteDependencyGraph(routeFiles: string[]): Map<string, string[]> {
  const graph = new Map<string, string[]>()

  for (const file of routeFiles) {
    const content = fs.readFileSync(file, 'utf-8')
    const imports = extractImports(content)
    const relativePath = path.relative(ROUTES_DIR, file)

    const routeImports = imports
      .filter((imp) => imp.startsWith('./') || imp.startsWith('../'))
      .filter((imp) => !imp.endsWith('.css'))
      .map((imp) => {
        // Resolve the import relative to the file's directory
        const fileDir = path.dirname(file)
        let resolved = path.resolve(fileDir, imp)

        // Add .ts/.tsx extension if missing
        if (!resolved.endsWith('.ts') && !resolved.endsWith('.tsx')) {
          if (fs.existsSync(resolved + '.ts')) {
            resolved += '.ts'
          } else if (fs.existsSync(resolved + '.tsx')) {
            resolved += '.tsx'
          } else if (fs.existsSync(path.join(resolved, 'index.ts'))) {
            resolved = path.join(resolved, 'index.ts')
          } else if (fs.existsSync(path.join(resolved, 'index.tsx'))) {
            resolved = path.join(resolved, 'index.tsx')
          }
        }

        return path.relative(ROUTES_DIR, resolved)
      })
      .filter((imp) => !imp.startsWith('..')) // Only include imports within routes/

    graph.set(relativePath, routeImports)
  }

  return graph
}

/**
 * Check if a TSX file uses inline Tailwind classes
 */
function containsInlineTailwindClasses(content: string): { found: boolean; lines: number[] } {
  const lines = content.split('\n')
  const tailwindLines: number[] = []

  // Common Tailwind class patterns that indicate inline Tailwind usage
  // These are distinctive enough to not match vanilla-extract usage
  const tailwindPatterns = [
    /className="[^"]*(?:flex|grid|block|inline|hidden)\b/,
    /className="[^"]*(?:w-|h-|m-|p-|gap-|space-)\d+/,
    /className="[^"]*(?:text-|bg-|border-)[a-z]+-\d+/,
    /className="[^"]*(?:rounded-|shadow-|opacity-)/,
    /className="[^"]*(?:justify-|items-|self-)/,
    // Brace form: only flag tailwind tokens inside a string literal (e.g. cn('flex'),
    // `grid ${x}`). This avoids false positives on vanilla-extract references like
    // className={styles.gridLine}, where the token is part of an identifier.
    /className={[^}]*['"`][^'"`]*(?:flex|grid|block|w-|h-|m-|p-)/,
  ]

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]

    // Skip commented lines
    if (line.trim().startsWith('//') || line.trim().startsWith('*')) {
      continue
    }

    for (const pattern of tailwindPatterns) {
      if (pattern.test(line)) {
        tailwindLines.push(i + 1)
        break
      }
    }
  }

  return { found: tailwindLines.length > 0, lines: tailwindLines }
}

/**
 * Extract import statements from a file
 */
function extractImports(content: string): string[] {
  const importRegex = /(?:import|from)\s+['"]([^'"]+)['"]/g
  const dynamicImportRegex = /import\(['"]([^'"]+)['"]\)/g
  const imports: string[] = []
  let match

  while ((match = importRegex.exec(content)) !== null) {
    imports.push(match[1])
  }

  while ((match = dynamicImportRegex.exec(content)) !== null) {
    imports.push(match[1])
  }

  return imports
}

/**
 * Detect circular dependencies using DFS
 */
function findCircularDependencies(graph: Map<string, string[]>): string[][] {
  const cycles: string[][] = []
  const visited = new Set<string>()
  const recursionStack = new Set<string>()
  const currentPath: string[] = []

  function dfs(node: string): boolean {
    visited.add(node)
    recursionStack.add(node)
    currentPath.push(node)

    const dependencies = graph.get(node) ?? []

    for (const dep of dependencies) {
      if (!visited.has(dep)) {
        if (dfs(dep)) {
          return true
        }
      } else if (recursionStack.has(dep)) {
        // Found a cycle
        const cycleStart = currentPath.indexOf(dep)
        const cycle = currentPath.slice(cycleStart)
        cycle.push(dep) // Complete the cycle
        cycles.push(cycle)
      }
    }

    currentPath.pop()
    recursionStack.delete(node)
    return false
  }

  for (const node of graph.keys()) {
    if (!visited.has(node)) {
      dfs(node)
    }
  }

  return cycles
}

/**
 * Helper to format violations for error messages
 */
function formatViolationMessage(
  violations: { details?: number[] | string[]; file: string }[],
  baseline: number,
  ruleName: string,
  fixInstructions: string,
): string {
  const count = violations.length
  const violationList = violations
    .slice(0, 10)
    .map((v) => {
      if (v.details && Array.isArray(v.details)) {
        if (typeof v.details[0] === 'number') {
          const lines = v.details as number[]
          return `  ${v.file} (lines: ${lines.slice(0, 5).join(', ')}${lines.length > 5 ? '...' : ''})`
        }
        return `  ${v.file}:\n    - ${(v.details as string[]).join('\n    - ')}`
      }
      return `  ${v.file}`
    })
    .join('\n')

  const moreMessage =
    violations.length > 10 ? `\n  ... and ${violations.length - 10} more files` : ''

  return (
    `${ruleName}\n\n` +
    `Found ${count} violation(s) (baseline: ${baseline}):\n${violationList}${moreMessage}\n\n` +
    `${fixInstructions}`
  )
}

/**
 * Get all directories in a directory (non-recursive)
 */
function getDirectories(dir: string): string[] {
  if (!fs.existsSync(dir)) {
    return []
  }

  return fs
    .readdirSync(dir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
}

/**
 * Recursively get all files in a directory matching a pattern
 */
function getFilesRecursively(dir: string, pattern: RegExp): string[] {
  const files: string[] = []

  if (!fs.existsSync(dir)) {
    return files
  }

  function walk(currentDir: string) {
    const entries = fs.readdirSync(currentDir, { withFileTypes: true })

    for (const entry of entries) {
      const fullPath = path.join(currentDir, entry.name)

      if (entry.isDirectory()) {
        walk(fullPath)
      } else if (entry.isFile() && pattern.test(entry.name)) {
        files.push(fullPath)
      }
    }
  }

  walk(dir)
  return files
}

function sourceSection(content: string, startMarker: string, endMarker: string): string {
  const start = content.indexOf(startMarker)
  const end = content.indexOf(endMarker, start)
  expect(start, `Missing source marker: ${startMarker}`).toBeGreaterThanOrEqual(0)
  expect(end, `Missing source marker: ${endMarker}`).toBeGreaterThan(start)
  return content.slice(start, end).replace(/\s/g, '')
}

describe('Architectural Tests', () => {
  describe('1. Component Isolation', () => {
    it('components should not import directly from routes (separation of concerns)', () => {
      const componentFiles = ALL_COMPONENT_DIRS.flatMap((dir) =>
        getFilesRecursively(dir, /\.tsx?$/),
      )
      const violations: { details: string[]; file: string }[] = []

      for (const file of componentFiles) {
        const content = fs.readFileSync(file, 'utf-8')
        const imports = extractImports(content)

        // Check for imports from routes (relative, @/routes, or ~/routes)
        const routeImports = imports.filter(
          (imp) =>
            imp.includes('/routes/') ||
            imp.startsWith('@/routes/') ||
            imp.startsWith('~/routes/') ||
            /^\.\.\/.*routes\//.exec(imp),
        )

        if (routeImports.length > 0) {
          violations.push({
            details: routeImports,
            file: path.relative(APP_SRC, file),
          })
        }
      }

      const baseline = BASELINES.componentIsolation

      if (violations.length > baseline) {
        expect.fail(
          formatViolationMessage(
            violations,
            baseline,
            'Wax components should not import from routes to maintain separation of concerns.',
            'Fix: Move shared code to a common location (e.g., utils/, hooks/, types/) ' +
              'that both components and routes can import from.',
          ),
        )
      }

      if (violations.length < baseline) {
        expect.fail(
          `Good news! Violations decreased from ${baseline} to ${violations.length}.\n` +
            `Please update BASELINES.componentIsolation to ${violations.length} in this file.`,
        )
      }
    })
  })

  describe('2. Route Dependency Direction', () => {
    it('app shell should wrap only top-level app routes', () => {
      const routeConfig = fs.readFileSync(ROUTE_CONFIG_FILE, 'utf-8')

      expect(routeConfig).toContain(
        "route(`${routePattern('workspaceSource')}/oauth-install`, 'routes/source-oauth-install.ts')",
      )
      // Onboarding renders its own full-page chrome, so it stays outside the app shell.
      expect(routeConfig).toContain("route(routePattern('onboarding'), 'routes/onboarding.tsx')")
      expect(routeConfig).toContain("layout('routes/app-shell.tsx', [")
      expect(routeConfig).toContain("index('routes/index.tsx')")
      expect(routeConfig).toContain(
        "route(routePattern('workspaces'), 'routes/workspaces-action.ts')",
      )
      expect(routeConfig).toContain(
        "route(routePattern('workspaceSources'), 'routes/sources.tsx', [",
      )
      expect(routeConfig).toContain("route('install', 'routes/source-install.tsx')")
      expect(routeConfig).toContain("route(':sourceName', 'routes/source-detail.tsx')")
      // Schema is a layout with nested table-detail routes.
      expect(routeConfig).toContain("route(routePattern('workspaceSchema'), 'routes/schema.tsx', [")
      expect(routeConfig).toContain("index('routes/schema-empty.tsx')")
      expect(routeConfig).toContain("route(':schemaName/:tableName', 'routes/schema-table.tsx')")
      expect(routeConfig).toContain(
        "route(':schemaName/functions/:functionName', 'routes/schema-table-function.tsx')",
      )
      expect(routeConfig).toContain("route(routePattern('workspaceTraces'), 'routes/traces.tsx', [")
      expect(routeConfig).toContain("route(':traceId', 'routes/trace-detail.tsx')")
      // Settings stays in the shared app shell; desktop-only sections are gated in the route.
      expect(routeConfig).toContain("route(routePattern('settings'), 'routes/settings.tsx')")

      // Structural check: the same-origin resource route and onboarding stay
      // outside the app shell, while canonical workspace routes and settings
      // stay inside it.
      expect(routeConfig).toMatch(
        /export default \[\s*(?:\/\/[^\n]*\n\s*)*route\(`\$\{routePattern\('workspaceSource'\)\}\/oauth-install`, 'routes\/source-oauth-install\.ts'\),\s*route\(routePattern\('onboarding'\), 'routes\/onboarding\.tsx'\),\s*layout\(\s*'routes\/app-shell\.tsx',\s*\[[\s\S]*\]\s*\),?\s*\] satisfies RouteConfig/,
      )
    })

    it('route files should not have circular dependencies', () => {
      const routeFiles = getFilesRecursively(ROUTES_DIR, /\.tsx?$/)
      const graph = buildRouteDependencyGraph(routeFiles)
      const cycles = findCircularDependencies(graph)

      if (cycles.length > 0) {
        const message = cycles.map((cycle) => `  ${cycle.join(' -> ')}`).join('\n')

        expect.fail(
          'Route files should have a clear dependency direction without circular dependencies.\n\n' +
            `Found ${cycles.length} circular dependency chain(s):\n${message}\n\n` +
            'Fix: Break the cycle by:\n' +
            '  1. Moving shared code to a common module outside the cycle\n' +
            '  2. Using dependency injection or context for shared state\n' +
            '  3. Restructuring the imports to have a clear hierarchy',
        )
      }
    })
  })

  describe('3. Styling Consistency', () => {
    it('component TSX files should use vanilla-extract (.css.ts) not inline Tailwind classes', () => {
      const componentFiles = ALL_COMPONENT_DIRS.flatMap((dir) => getFilesRecursively(dir, /\.tsx$/))
      const violations: { details: number[]; file: string }[] = []

      // Exclude test and story files - Tailwind is acceptable there for layout
      const sourceFiles = componentFiles.filter(
        (f) => !f.includes('.test.') && !f.includes('.stories.'),
      )

      for (const file of sourceFiles) {
        const content = fs.readFileSync(file, 'utf-8')
        const result = containsInlineTailwindClasses(content)

        if (result.found) {
          violations.push({
            details: result.lines,
            file: path.relative(APP_SRC, file),
          })
        }
      }

      const baseline = BASELINES.tailwindInComponents

      if (violations.length > baseline) {
        expect.fail(
          formatViolationMessage(
            violations,
            baseline,
            'Wax component files should use vanilla-extract (.css.ts) for styling, not inline Tailwind classes.',
            'Fix: Move styles to a colocated .css.ts file using vanilla-extract.\n' +
              'Example:\n' +
              "  // component.css.ts\n  import { style } from '@vanilla-extract/css'\n" +
              "  export const container = style({ display: 'flex', gap: '8px' })\n\n" +
              "  // component.tsx\n  import * as styles from './component.css'\n" +
              '  <div className={styles.container}>...</div>\n\n' +
              'Note: Tailwind is acceptable in .stories.tsx files for layout purposes.',
          ),
        )
      }

      if (violations.length < baseline) {
        expect.fail(
          `Good news! Violations decreased from ${baseline} to ${violations.length}.\n` +
            `Please update BASELINES.tailwindInComponents to ${violations.length} in this file.`,
        )
      }
    })
  })

  describe('4. Storybook Coverage', () => {
    it('wax/components/ directories should each have at least one .stories.tsx file', () => {
      const componentDirs = getDirectories(WAX_COMPONENTS_DIR)
      const violations: { file: string }[] = []

      for (const dir of componentDirs) {
        const dirPath = path.join(WAX_COMPONENTS_DIR, dir)

        const componentFiles = fs
          .readdirSync(dirPath)
          .filter(
            (f) => f.endsWith('.tsx') && !f.endsWith('.stories.tsx') && !f.endsWith('.test.tsx'),
          )

        if (componentFiles.length === 0) {
          continue
        }

        const storyFiles = getFilesRecursively(dirPath, /\.stories\.tsx$/)

        if (storyFiles.length === 0) {
          violations.push({ file: `wax/components/${dir}` })
        }
      }

      const baseline = BASELINES.storybookCoverageWax

      if (violations.length > baseline) {
        expect.fail(
          formatViolationMessage(
            violations,
            baseline,
            'Every Wax component directory should have at least one .stories.tsx file.',
            'Fix: Create a .stories.tsx file in the component directory.\n' +
              'Example: wax/components/<name>/<name>.stories.tsx',
          ),
        )
      }

      if (violations.length < baseline) {
        expect.fail(
          `Good news! Violations decreased from ${baseline} to ${violations.length}.\n` +
            `Please update BASELINES.storybookCoverageWax to ${violations.length} in this file.`,
        )
      }
    })

    it('components/ files should each have a corresponding .stories.tsx file', () => {
      const allTsxFiles = getFilesRecursively(COMPONENTS_DIR, /\.tsx$/)

      const componentFiles = allTsxFiles.filter(
        (f) =>
          !f.endsWith('.stories.tsx') &&
          !f.endsWith('.test.tsx') &&
          path.basename(f) !== 'index.tsx',
      )

      // Exclude non-visual .tsx files that don't need stories:
      // - Context providers (*-context.tsx, files in /contexts/ dirs)
      // - Providers (*-provider.tsx)
      // - Hooks (use-*.tsx)
      // - Lexical editor internals (files in /plugins/ or /commands/ dirs)
      // - React Flow graph elements (files in /nodes/ or /edges/ dirs)
      // - Deprecated files (*-old.tsx)
      const visualComponentFiles = componentFiles.filter((f) => {
        const fileName = path.basename(f)
        const relativePath = path.relative(COMPONENTS_DIR, f)

        if (fileName.endsWith('-context.tsx') || relativePath.includes('/contexts/')) return false
        if (fileName.endsWith('-provider.tsx')) return false
        if (fileName.startsWith('use-')) return false
        if (relativePath.includes('/plugins/') || relativePath.includes('/commands/')) return false
        if (relativePath.includes('/nodes/') || relativePath.includes('/edges/')) return false
        if (fileName.endsWith('-old.tsx')) return false

        return true
      })

      const violations: { file: string }[] = []

      for (const file of visualComponentFiles) {
        const dir = path.dirname(file)
        const basename = path.basename(file, '.tsx')
        const storyFile = path.join(dir, `${basename}.stories.tsx`)

        if (!fs.existsSync(storyFile)) {
          violations.push({ file: path.relative(APP_SRC, file) })
        }
      }

      const baseline = BASELINES.storybookCoverageComponents

      if (violations.length > baseline) {
        expect.fail(
          formatViolationMessage(
            violations,
            baseline,
            'Every component file should have a corresponding .stories.tsx file.',
            'Fix: Create a .stories.tsx file for each component file.\n' +
              'Example: components/copy-button/copy-button.stories.tsx for components/copy-button/copy-button.tsx',
          ),
        )
      }

      if (violations.length < baseline) {
        expect.fail(
          `Good news! Violations decreased from ${baseline} to ${violations.length}.\n` +
            `Please update BASELINES.storybookCoverageComponents to ${violations.length} in this file.`,
        )
      }
    })
  })

  describe('5. Coral Transport Boundaries', () => {
    it('keeps Connect transport construction out of browser application source', () => {
      const browserSourceFiles = getFilesRecursively(APP_SRC, /\.tsx?$/).filter(
        (file) =>
          !file.includes('.server.') &&
          !file.includes('.test.') &&
          !file.includes('.stories.') &&
          !file.includes(`${path.sep}__tests__${path.sep}`),
      )
      const violations = browserSourceFiles
        .filter((file) => {
          const content = fs.readFileSync(file, 'utf-8')
          return (
            extractImports(content).includes('@connectrpc/connect-web') ||
            /\bcreateGrpcWebTransport\s*\(/.test(content)
          )
        })
        .map((file) => path.relative(APP_SRC, file))

      expect(violations).toEqual([])
    })

    it('keeps Source and Trace clients behind the request-scoped server boundary', () => {
      const requestClient = fs.readFileSync(
        path.join(APP_SRC, 'lib', 'coral-request.server.ts'),
        'utf-8',
      )

      expect(extractImports(requestClient)).toContain('@connectrpc/connect-web')
      expect(requestClient).toMatch(/export function sourceClientForRequest\s*\(/)
      expect(requestClient).toMatch(/export function traceClientForRequest\s*\(/)
    })

    it('retains root sidebar hydration without warming a browser Coral runtime', () => {
      const root = fs.readFileSync(path.join(APP_SRC, 'root.tsx'), 'utf-8')

      expect(root).not.toMatch(/ensureCoralRuntime|coral-runtime/)
      expect(root).toMatch(/readSidebarCollapsedCookieValue\(document\.cookie\)/)
      expect(root).toMatch(/clientLoader\.hydrate\s*=\s*true as const/)
    })

    it('keeps Desktop main responsible for sidecar readiness', () => {
      const desktopIndex = fs.readFileSync(path.join(DESKTOP_MAIN_DIR, 'index.ts'), 'utf-8')
      const appRenderer = fs.readFileSync(path.join(DESKTOP_MAIN_DIR, 'app-renderer.ts'), 'utf-8')
      const devEntry = sourceSection(
        desktopIndex,
        'async function rendererEntryUrl()',
        'function urlOrigin',
      )
      const readyHandler = sourceSection(desktopIndex, 'app.whenReady().then', "app.on('activate'")
      const responseHandler = sourceSection(
        appRenderer,
        'async function reactRouterResponse',
        'async function secureDocumentResponse',
      )
      const endpointRefresh = sourceSection(
        appRenderer,
        'async function refreshServerSidecarEndpoint',
        'async function reactRouterResponse',
      )
      const refreshIndex = responseHandler.indexOf(
        'awaitrefreshServerSidecarEndpoint(resolveSidecarBaseUrl)',
      )
      const handlerIndex = responseHandler.indexOf('awaitloadReactRouterHandler()')

      expect(devEntry).toContain('awaitensureSidecar()')
      expect(readyHandler).toContain(
        'registerAppProtocol(()=>ensureSidecar().then((started)=>started.url))',
      )
      expect(readyHandler).toContain('voidensureSidecar().catch(')
      expect(endpointRefresh).toContain('process.env.CORAL_ENDPOINT=awaitresolveSidecarBaseUrl()')
      expect(refreshIndex).toBeGreaterThanOrEqual(0)
      expect(handlerIndex).toBeGreaterThanOrEqual(0)
      expect(refreshIndex).toBeLessThan(handlerIndex)
    })

    it('derives shared-render and browser Desktop behavior from one build marker', () => {
      const desktopRoot = path.resolve(DESKTOP_SRC_DIR, '..')
      const viteConfig = fs.readFileSync(path.join(REEF_ROOT, 'vite.config.ts'), 'utf-8')
      const desktopHelper = fs.readFileSync(path.join(APP_SRC, 'lib', 'coral-desktop.ts'), 'utf-8')
      const devScript = fs.readFileSync(path.join(desktopRoot, 'scripts', 'dev.mjs'), 'utf-8')
      const stageScript = fs.readFileSync(
        path.join(desktopRoot, 'scripts', 'stage-coral.mjs'),
        'utf-8',
      )
      const desktopBuildSources = [viteConfig, desktopHelper, devScript, stageScript]

      for (const source of desktopBuildSources) {
        expect(source).not.toContain('VITE_CORAL_DESKTOP_APP')
      }
      expect(viteConfig).toMatch(
        /'import\.meta\.env\.CORAL_DESKTOP_APP':\s*JSON\.stringify\(\s*process\.env\.CORAL_DESKTOP_APP === '1',?\s*\)/,
      )
      expect(desktopHelper).toMatch(/return import\.meta\.env\.CORAL_DESKTOP_APP/)
      expect(devScript.match(/CORAL_DESKTOP_APP:\s*'1'/g)).toHaveLength(1)
      expect(stageScript.match(/CORAL_DESKTOP_APP:\s*'1'/g)).toHaveLength(1)
    })

    it('does not expose Coral transport through the renderer or Desktop preload', () => {
      const appRenderer = fs.readFileSync(path.join(DESKTOP_MAIN_DIR, 'app-renderer.ts'), 'utf-8')
      const desktopIndex = fs.readFileSync(path.join(DESKTOP_MAIN_DIR, 'index.ts'), 'utf-8')
      const preload = fs.readFileSync(path.join(DESKTOP_SRC_DIR, 'preload', 'index.ts'), 'utf-8')
      const sharedTypes = fs.readFileSync(path.join(DESKTOP_SRC_DIR, 'shared', 'types.ts'), 'utf-8')
      const viteConfig = fs.readFileSync(path.join(REEF_ROOT, 'vite.config.ts'), 'utf-8')
      const transportSurfaces = [appRenderer, desktopIndex, preload, sharedTypes, viteConfig]

      for (const source of transportSurfaces) {
        expect(source).not.toMatch(
          /\/__coral__|GRPC_PATH_PREFIX|APP_GRPC_BASE|grpcBaseUrl|awaitInitialization|coral:await-initialization|proxyToSidecar/,
        )
      }
      expect(viteConfig).toContain("'/coral.v1'")
      expect(preload).toContain("contextBridge.exposeInMainWorld('coralDesktop', api)")
      expect(preload).toContain('listMcpClients:')
      expect(preload).toContain('configureMcp:')
      expect(preload).toContain('getMcpLaunchConfig:')
      expect(preload).toContain('getUpdateState:')
      expect(preload).toContain('onUpdateStateChange:')
      expect(desktopIndex).toContain("'coral:get-mcp-launch-config'")
      expect(desktopIndex).toContain("'coral:get-update-state'")
      expect(sharedTypes).toMatch(/interface CoralDesktopApi[\s\S]*listMcpClients\(\)/)
      expect(sharedTypes).toMatch(/interface CoralDesktopApi[\s\S]*configureMcp\(/)
      expect(sharedTypes).toMatch(/interface CoralDesktopApi[\s\S]*getMcpLaunchConfig\(\)/)
      expect(sharedTypes).toMatch(/interface CoralDesktopApi[\s\S]*getUpdateState\(\)/)
      expect(sharedTypes).toMatch(/interface CoralDesktopApi[\s\S]*onUpdateStateChange\(/)
    })
  })
})
