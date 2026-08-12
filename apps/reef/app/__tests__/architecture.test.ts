import * as fs from 'node:fs'
import * as path from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

const appDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const componentsDir = path.join(appDir, 'components')
const routesDir = path.join(appDir, 'routes')
const waxComponentsDir = path.join(appDir, 'wax', 'components')
const reefRoot = path.resolve(appDir, '..')

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

    expect(violations, 'component modules must not import route modules').toEqual([])
  })

  it('does not depend on Tailwind packages', () => {
    const packageJson = JSON.parse(
      fs.readFileSync(path.join(reefRoot, 'package.json'), 'utf8'),
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
