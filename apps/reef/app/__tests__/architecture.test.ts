import * as fs from 'node:fs'
import * as path from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import { parseSync, traverse } from '@babel/core'

import { runtimeImports } from '../wax/vite.js'

const appDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const componentsDir = path.join(appDir, 'components')
const routesDir = path.join(appDir, 'routes')
const waxComponentsDir = path.join(appDir, 'wax', 'components')
const coralWebRoot = path.resolve(appDir, '..')
const viewsDir = path.join(appDir, 'views')
// Everything that renders. Data reaches it as loader data or through a fetcher;
// it never goes looking for data itself.
const presentationDirs = [componentsDir, viewsDir, waxComponentsDir]
const effectHooks = ['useEffect', 'useLayoutEffect', 'useInsertionEffect']
// Suspending inside an effect is how data loading keeps reappearing in
// components, so the shape is banned rather than any particular caller.
const suspendsInEffect = /\bawait\b|\.then\s*\(|\.catch\s*\(/

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

function within(file: string, directory: string): boolean {
  return file.startsWith(directory + path.sep)
}

describe('architecture', () => {
  it('keeps Reel outside the Reef app, sharing only Wax presentation', () => {
    const toolsDir = path.join(coralWebRoot, 'e2e')
    const viewerDir = path.join(toolsDir, 'viewer')
    const waxDir = path.join(appDir, 'wax')
    const violations = [appDir, viewerDir]
      .flatMap((directory) => filesUnder(directory, (name) => /\.tsx?$/.test(name)))
      .filter((file) => !/\.(?:test|spec|stories)\.tsx?$/.test(file))
      .flatMap((file) =>
        importsFrom(fs.readFileSync(file, 'utf8')).flatMap((specifier) => {
          const resolved = /^[~@]\//.test(specifier)
            ? path.resolve(appDir, specifier.slice(2))
            : specifier.startsWith('.')
              ? path.resolve(path.dirname(file), specifier)
              : null
          if (!resolved) return []
          const importsToolFromApp = within(file, appDir) && within(resolved, toolsDir)
          const importsAppFromViewer =
            within(file, viewerDir) && within(resolved, appDir) && !within(resolved, waxDir)
          return importsToolFromApp || importsAppFromViewer
            ? [`${path.relative(coralWebRoot, file)} -> ${specifier}`]
            : []
        }),
      )
    expect(violations).toEqual([])
  })

  it('keeps visible controls and text primitives in Wax', () => {
    const primitiveTags = new Set([
      'a',
      'button',
      'input',
      'select',
      'textarea',
      'h1',
      'h2',
      'h3',
      'h4',
      'h5',
      'h6',
      'p',
      'label',
      'strong',
      'em',
      'code',
      'pre',
      'table',
      'thead',
      'tbody',
      'tr',
      'th',
      'td',
    ])
    const files = filesUnder(appDir, (name) => name.endsWith('.tsx'))
      .filter((file) => !file.startsWith(path.join(appDir, 'wax') + path.sep))
      .filter((file) => !/\.(?:test|spec|stories)\.tsx$/.test(file))
    const violations: string[] = []
    for (const file of files) {
      const source = parseSync(fs.readFileSync(file, 'utf8'), {
        babelrc: false,
        configFile: false,
        parserOpts: { plugins: ['typescript', 'jsx'] },
      })!
      traverse(source, {
        JSXOpeningElement({ node }) {
          if (node.name.type !== 'JSXIdentifier') return
          const tag = node.name.name
          const hiddenInput =
            tag === 'input' &&
            node.attributes.some(
              (attribute) =>
                attribute.type === 'JSXAttribute' &&
                attribute.name.name === 'type' &&
                attribute.value?.type === 'StringLiteral' &&
                attribute.value.value === 'hidden',
            )
          if (primitiveTags.has(tag) && !hiddenInput) {
            violations.push(`${path.relative(appDir, file)}:${node.loc?.start.line} -> <${tag}>`)
          }
        },
      })
    }
    expect(
      violations,
      'Compose Wax controls, links, Typography, and Table. Hidden form fields and semantic layout elements stay native; reusable visual primitives belong in Wax.',
    ).toEqual([])
  })

  it('keeps credential entry out of React and router submission state', () => {
    const violations: string[] = []
    for (const file of filesUnder(viewsDir, isVisualTsx)) {
      const source = parseSync(fs.readFileSync(file, 'utf8'), {
        babelrc: false,
        configFile: false,
        parserOpts: { plugins: ['typescript', 'jsx'] },
      })!
      traverse(source, {
        JSXOpeningElement(element) {
          const { node } = element
          const password = node.attributes.some(
            (a) =>
              a.type === 'JSXAttribute' &&
              a.name.name === 'type' &&
              a.value?.type === 'StringLiteral' &&
              a.value.value === 'password',
          )
          const credential =
            node.name.type === 'JSXMemberExpression' &&
            node.name.property.name === 'CredentialInput'
          if (password)
            violations.push(
              `${path.relative(appDir, file)}: use Wax CredentialInput for write-only entry`,
            )
          if (!credential) return
          const form = element
            .getAncestry()
            .find(
              (p) =>
                p.isJSXElement() &&
                p.node.openingElement.name.type === 'JSXIdentifier' &&
                ['form', 'Form'].includes(p.node.openingElement.name.name),
            )
          if (!form?.isJSXElement()) {
            violations.push(
              `${path.relative(appDir, file)}: credential entry needs a document form`,
            )
            return
          }
          const opening = form.node.openingElement
          const native = opening.name.type === 'JSXIdentifier' && opening.name.name === 'form'
          const reloads = opening.attributes.some((a) => {
            if (a.type !== 'JSXAttribute' || a.name.name !== 'reloadDocument') return false
            if (a.value === null) return true
            if (
              a.value?.type !== 'JSXExpressionContainer' ||
              a.value.expression.type !== 'Identifier'
            )
              return false
            // A mixed form can remain enhanced only while it has no credential input.
            // The exact same predicate must guard both rendering and document submission.
            const predicate = a.value.expression.name
            return element
              .getAncestry()
              .some(
                (p) =>
                  p.isLogicalExpression() &&
                  p.node.operator === '&&' &&
                  p.node.left.type === 'Identifier' &&
                  p.node.left.name === predicate,
              )
          })
          if (!native && !reloads)
            violations.push(
              `${path.relative(appDir, file)}: credential entry must bypass router formData`,
            )
        },
      })
    }
    expect(violations).toEqual([])
  })

  it('gives every reload-document form local submission feedback', () => {
    const violations = filesUnder(viewsDir, isVisualTsx).flatMap((file) => {
      const source = fs.readFileSync(file, 'utf8')
      if (!/<Form\b[^>]*\breloadDocument\b/s.test(source)) return []
      return importsFrom(source).includes('@/components/document-submit-button')
        ? []
        : [`${path.relative(appDir, file)}: use DocumentSubmitButton with reloadDocument`]
    })

    expect(violations).toEqual([])
  })

  it('keeps button labels stable while they submit', () => {
    const violations: string[] = []
    for (const file of [componentsDir, viewsDir].flatMap((dir) => filesUnder(dir, isVisualTsx))) {
      const source = parseSync(fs.readFileSync(file, 'utf8'), {
        babelrc: false,
        configFile: false,
        parserOpts: { plugins: ['typescript', 'jsx'] },
      })!
      traverse(source, {
        JSXElement(element) {
          const { name } = element.node.openingElement
          if (name.type !== 'JSXMemberExpression' || name.object.type !== 'JSXIdentifier') return
          if (name.object.name !== 'Button' || !['TextButton', 'Text'].includes(name.property.name))
            return
          element.traverse({
            'JSXText|StringLiteral|TemplateElement'(text) {
              const value =
                text.node.type === 'TemplateElement' ? text.node.value.raw : text.node.value
              if (value.includes('…') && !text.findParent((p) => p.isJSXAttribute()))
                violations.push(`${path.relative(appDir, file)}:${text.node.loc?.start.line}`)
            },
          })
        },
      })
    }
    expect(
      violations,
      'Pass progress to Button.TextButton busy; it shows a spinner and keeps the label.',
    ).toEqual([])
  })

  it('keeps low-level UI libraries behind Wax', () => {
    const violations = filesUnder(appDir, (name) => /\.tsx?$/.test(name))
      .filter((file) => !file.startsWith(path.join(appDir, 'wax') + path.sep))
      .filter((file) => !/\.(?:test|spec|stories)\.tsx?$/.test(file))
      .flatMap((file) =>
        importsFrom(fs.readFileSync(file, 'utf8'))
          .filter((specifier) =>
            /^(?:@base-ui\/|react-aria(?:-components)?(?:\/|$)|lucide-react(?:\/|$))/.test(
              specifier,
            ),
          )
          .map((specifier) => `${path.relative(appDir, file)} -> ${specifier}`),
      )
    expect(
      violations,
      'Use or extend Wax instead of importing a second set of UI primitives into app modules.',
    ).toEqual([])
  })

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
        ...importsFrom(source)
          .filter(
            (specifier) =>
              specifier.includes('/generated/') || specifier.startsWith('@connectrpc/'),
          )
          .map((specifier) => `Coral wire contract in presentation: ${specifier}`),
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
        'render the result from loaderData (see routes/login.tsx)\n' +
        '  writes -> export an action, or clientAction for the same reason, and submit to it ' +
        'with useFetcher, which also gives you the pending state (see ' +
        'views/management/forms.tsx)\n' +
        '  pushed events, which no loader can express -> hold them in an atom that subscribes ' +
        'in onMount (see lib/activity-observation.ts). Coral live updates go through ' +
        'lib/activity-stream.ts\n' +
        'Re-exporting the same call from app/lib or app/utils and importing it back is not a ' +
        'fix. This test stops looking there, but the request still runs outside the router, so ' +
        'it still has no caching, no pending state, and no revalidation. Nor is dropping the ' +
        'await while keeping the effect: a subscription belongs in an atom, and a request ' +
        'belongs in a handler.',
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
    const globals = fs.readFileSync(path.join(appDir, 'wax', 'theme', 'reset.css'), 'utf8')
    const layers = fs.readFileSync(path.join(appDir, 'wax', 'theme', 'layers.css.ts'), 'utf8')
    const stated = /@layer ([^;]+);/
      .exec(globals)?.[1]
      .split(',')
      .map((name) => name.trim())
    const declared = [...layers.matchAll(/globalLayer\('([^']+)'\)/g)].map((match) => match[1])

    expect(
      stated,
      'app/wax/theme/reset.css states the layer order for the app and ' +
        'app/wax/theme/layers.css.ts declares the same layers for Vanilla Extract. Both have to ' +
        'name the same layers in the same order. A layer left out of the statement is created ' +
        'the first time a rule uses it, which puts it above everything already there.',
    ).toEqual(declared)
  })

  it('does not depend on Tailwind packages', () => {
    const packageJson = JSON.parse(
      fs.readFileSync(path.join(coralWebRoot, 'package.json'), 'utf8'),
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
    const server = fs.readFileSync(path.join(coralWebRoot, 'server.js'), 'utf8')
    const allowedImports = new Set([
      'node:fs',
      'node:fs/promises',
      'node:http',
      'node:net',
      'node:path',
      'node:stream',
      'node:stream/promises',
      'node:url',
      'node:zlib',
      'react-router',
      './build/server/index.js',
      './server-tracing.js',
    ])

    expect(importsFrom(server).filter((specifier) => !allowedImports.has(specifier))).toEqual([])
    expect(server).not.toMatch(/@react-router\/dev|react-router dev|vite|express/)
    expect(server).toContain('export async function startServer')
    expect(server).toContain('pathToFileURL(process.argv[1]).href === import.meta.url')
  })

  it('reads runtime configuration from the environment only at bootstrap', () => {
    // Each of these reads process.env. Request code reads runtimeConfigContext instead.
    const environmentReaders = new Map([
      ['coralWebAuthConfig', 'auth/config.server.ts'],
      ['readCoralWebRuntimeConfig', 'lib/runtime-config.server.ts'],
      ['resolveCoralEndpoint', 'lib/coral-endpoint.server.ts'],
    ])
    const bootstrap = 'lib/runtime-config.server.ts'
    const violations: string[] = []
    for (const file of filesUnder(appDir, (name) => /\.tsx?$/.test(name))) {
      const name = path.relative(appDir, file)
      if (/\.(?:test|spec|stories)\.tsx?$/.test(name) || name === bootstrap) continue
      const ast = parseSync(fs.readFileSync(file, 'utf8'), {
        babelrc: false,
        configFile: false,
        parserOpts: { plugins: ['typescript', 'jsx'] },
      })!
      traverse(ast, {
        Identifier({ node }) {
          const owner = environmentReaders.get(node.name)
          if (owner !== undefined && owner !== name)
            violations.push(`${name}:${node.loc?.start.line} -> ${node.name}`)
        },
      })
    }

    expect(
      violations,
      `Only ${bootstrap} reads configuration from the environment, once at startup. ` +
        'Read the resolved values with context.get(runtimeConfigContext); the root route ' +
        'middleware sets it for every route.',
    ).toEqual([])
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

it('confines Provider consent navigation to the fresh reference-based redirect adapter', () => {
  const violations: string[] = []
  const issuer = path.join(appDir, 'lib/provider-consent.server.ts')
  const accounts = path.join(appDir, 'lib/accounts.server.ts')
  for (const file of filesUnder(appDir, (name) => /\.tsx?$/.test(name))) {
    if (
      file.includes(`${path.sep}generated${path.sep}`) ||
      /\.(test|spec|stories)\.tsx?$/.test(file) ||
      file === issuer
    )
      continue
    const ast = parseSync(fs.readFileSync(file, 'utf8'), {
      babelrc: false,
      configFile: false,
      parserOpts: { plugins: ['typescript', 'jsx'] },
    })!
    traverse(ast, {
      Identifier(location) {
        if (!['consentNavigation', 'ProviderConsentNavigationSchema'].includes(location.node.name))
          return
        // Account presentation may expose availability, never the URL or wire object.
        const expression = location.parentPath
        const call = expression.parentPath
        if (
          file === accounts &&
          expression.isMemberExpression() &&
          call?.isCallExpression() &&
          call.node.callee.type === 'Identifier' &&
          call.node.callee.name === 'Boolean'
        )
          return
        violations.push(path.relative(appDir, file))
      },
      StringLiteral({ node }) {
        if (node.value === 'consentNavigation') violations.push(path.relative(appDir, file))
      },
    })
  }
  expect(
    violations,
    'Redirect through provider-consent.server using an authorization reference; never cache, fabricate, or present consent URLs.',
  ).toEqual([])
})

// Server actions that do not take a management form, and what guards each instead.
const actionsWithoutMutationForm = new Map([
  [
    'activity-count.ts',
    'a read-only SSE stream: assertManagementOrigin, then the session deadline',
  ],
  [
    'activity-observe.ts',
    'a read-only SSE stream: assertManagementOrigin, then the session deadline',
  ],
  ['logout.tsx', 'outside the protected boundary: validates its own CSRF token'],
])

it('guards every route action with mutationForm or names why not', () => {
  const unguarded: string[] = []
  const withAction = new Set<string>()
  for (const file of filesUnder(routesDir, (name) => /\.tsx?$/.test(name))) {
    if (/\.(test|spec|stories|css)\.tsx?$/.test(file)) continue
    const name = path.relative(routesDir, file)
    const ast = parseSync(fs.readFileSync(file, 'utf8'), {
      babelrc: false,
      configFile: false,
      parserOpts: { plugins: ['typescript', 'jsx'] },
    })!
    traverse(ast, {
      ExportNamedDeclaration(location) {
        const declaration = location.get('declaration')
        const action = declaration.isFunctionDeclaration()
          ? declaration.node.id?.name === 'action'
            ? declaration
            : undefined
          : declaration.isVariableDeclaration()
            ? declaration
                .get('declarations')
                .find(
                  (item) => item.node.id.type === 'Identifier' && item.node.id.name === 'action',
                )
            : undefined
        if (!action) return
        withAction.add(name)
        let guarded = false
        action.traverse({
          CallExpression(call) {
            const callee = call.node.callee
            if (callee.type === 'Identifier' && callee.name === 'mutationForm') guarded = true
          },
        })
        if (!guarded && !actionsWithoutMutationForm.has(name)) unguarded.push(name)
      },
    })
  }

  expect(
    unguarded,
    'A route action must start from mutationForm(request, context), which checks the method, ' +
      'origin and CSRF token. Add a route here only when another guard covers it, and say which.',
  ).toEqual([])
  expect(
    [...actionsWithoutMutationForm.keys()].filter((name) => !withAction.has(name)),
    'Remove allowlist entries whose route no longer has an action.',
  ).toEqual([])
})

// Wax is published as `@withcoral/wax`: the folder `app/wax` is the package, as source.
// Reef reads it as its own files and never installs it, so nothing else in Reef's gates
// notices a file, a manifest entry or a pre-bundle entry that only an installed copy needs.
describe('the Wax package', () => {
  const waxDir = path.join(appDir, 'wax')
  const waxFiles = filesUnder(waxDir, (name) => /\.(?:tsx?|css)$/.test(name))

  it('imports nothing outside itself', () => {
    const violations = waxFiles.flatMap((file) =>
      moduleSpecifiers(file)
        .filter(({ specifier }) => {
          if (specifier.startsWith('.')) {
            return !within(path.resolve(path.dirname(file), specifier), waxDir)
          }
          // `@/` and `~/` are Reef's aliases, and a path from `/` is Reef's `public` folder.
          return /^[@~]?\//.test(specifier)
        })
        .map(({ specifier }) => `${path.relative(appDir, file)} -> ${specifier}`),
    )

    expect(
      violations,
      'app/wax is published as @withcoral/wax, and an app that installs it has only the ' +
        'files in that folder. Import a Wax file by a relative path. Move a helper that Wax ' +
        'needs into app/wax; Reef then imports it from @/wax.',
    ).toEqual([])
  })

  it('declares each package it imports, at the range Reef installs', () => {
    const wax = readManifest(path.join(waxDir, 'package.json'))
    const reef = readManifest(path.join(coralWebRoot, 'package.json'))
    const declared = { ...wax.dependencies, ...wax.peerDependencies }
    const installed = { ...reef.dependencies, ...reef.devDependencies }

    // The unit tests stay out of the tarball, so what only they import is not declared.
    const undeclared = waxFiles
      .filter((file) => !/\.test\.tsx?$/.test(file))
      .flatMap((file) =>
        moduleSpecifiers(file)
          .map(({ specifier }) => packageName(specifier))
          .filter((name) => name !== null && !(name in declared))
          .map((name) => `${path.relative(appDir, file)} -> ${name}`),
      )
    expect(
      undeclared,
      'app/wax/package.json must declare each package that a published Wax file imports. ' +
        'A peer is a package that holds state the app shares with Wax, such as React. ' +
        'An optional peer is one that only the stories and their helpers import.',
    ).toEqual([])

    // An app builds Wax against its own lockfile, so Wax states the versions it was
    // tested with. A package that Reef pins to one version takes a caret, or an app
    // would have to move that package in the same change as Wax.
    const tested = Object.fromEntries(
      Object.keys(declared).map((name) => [
        name,
        /^\d/.test(installed[name] ?? '') ? `^${installed[name]}` : installed[name],
      ]),
    )
    expect(
      declared,
      'Each range in app/wax/package.json must equal the range in apps/reef/package.json. ' +
        'Change both files together.',
    ).toEqual(tested)
  })

  it('lists each run-time import for the dev server of an app', () => {
    const imported = waxFiles
      // A test and a story never reach an app's pages, and a `.css.ts` file runs at
      // build time and leaves only class names.
      .filter((file) => /\.tsx?$/.test(file) && !/\.(?:test|stories|css|d)\.tsx?$/.test(file))
      .filter((file) => !within(file, path.join(waxDir, 'storybook')))
      .flatMap((file) => moduleSpecifiers(file))
      .filter(({ runtime, specifier }) => runtime && packageName(specifier) !== null)
      .map(({ specifier }) => specifier)
    // The Vanilla Extract plugin writes this import into each compiled recipe.
    const expected = new Set([...imported, '@vanilla-extract/recipes/createRuntimeFn'])

    expect(
      runtimeImports,
      'runtimeImports in app/wax/vite.js must name each specifier that a Wax file imports ' +
        'at run time, in order. The dev server of an app pre-bundles only what the list ' +
        'names, and a CommonJS package that it misses fails in the browser.',
    ).toEqual([...expected].toSorted())
  })
})

/** The packages a `package.json` names, by section. */
function readManifest(file: string) {
  return JSON.parse(fs.readFileSync(file, 'utf8')) as Record<
    'dependencies' | 'devDependencies' | 'peerDependencies',
    Record<string, string> | undefined
  >
}

/** The package a bare specifier names, or null for a path, an alias or a Node built-in. */
function packageName(specifier: string): string | null {
  if (/^[./~]|^@\/|^node:/.test(specifier)) return null
  // addon-vitest 10 names this Vitest 4 path, and an app maps it to `vitest/browser`.
  if (specifier === '@vitest/browser/context') return 'vitest'
  return specifier
    .split('/')
    .slice(0, specifier.startsWith('@') ? 2 : 1)
    .join('/')
}

/**
 * Each module a file names: an import, a re-export, a dynamic `import()`, a Vitest mock,
 * and in a stylesheet an `@import` or a `url()`. `runtime` is false for a type-only one.
 */
function moduleSpecifiers(file: string): { runtime: boolean; specifier: string }[] {
  const source = fs.readFileSync(file, 'utf8')
  if (file.endsWith('.css')) {
    return [
      ...source.matchAll(/@import\s+(?:url\(\s*)?['"]([^'"]+)['"]/g),
      ...source.matchAll(/url\(\s*['"]?([^'")]+)['"]?\s*\)/g),
    ]
      .map((match) => match[1])
      .filter((specifier) => !/^(?:data:|https?:|#)/.test(specifier))
      .map((specifier) => ({ runtime: false, specifier }))
  }

  const found: { runtime: boolean; specifier: string }[] = []
  const ast = parseSync(source, {
    babelrc: false,
    configFile: false,
    parserOpts: { plugins: file.endsWith('.tsx') ? ['typescript', 'jsx'] : ['typescript'] },
  })!
  traverse(ast, {
    ImportDeclaration({ node }) {
      const typeOnly =
        node.importKind === 'type' ||
        (node.specifiers.length > 0 &&
          node.specifiers.every(
            (specifier) => specifier.type === 'ImportSpecifier' && specifier.importKind === 'type',
          ))
      found.push({ runtime: !typeOnly, specifier: node.source.value })
    },
    ExportNamedDeclaration({ node }) {
      if (node.source) {
        found.push({ runtime: node.exportKind !== 'type', specifier: node.source.value })
      }
    },
    ExportAllDeclaration({ node }) {
      found.push({ runtime: node.exportKind !== 'type', specifier: node.source.value })
    },
    CallExpression({ node }) {
      const [first] = node.arguments
      if (first?.type !== 'StringLiteral') return
      const { callee } = node
      const mock =
        callee.type === 'MemberExpression' &&
        callee.object.type === 'Identifier' &&
        callee.object.name === 'vi' &&
        callee.property.type === 'Identifier' &&
        /^(?:mock|doMock|unmock|importActual|importMock)$/.test(callee.property.name)
      if (mock) found.push({ runtime: false, specifier: first.value })
    },
    ImportExpression({ node }) {
      if (node.source.type === 'StringLiteral') {
        found.push({ runtime: true, specifier: node.source.value })
      }
    },
    TSImportType({ node }) {
      found.push({ runtime: false, specifier: node.source.value })
    },
  })
  return found
}
