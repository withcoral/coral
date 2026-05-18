#!/usr/bin/env node
import { spawnSync } from 'node:child_process'
import { mkdirSync, readdirSync, writeFileSync } from 'node:fs'
import { dirname, relative, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

const scriptDir = dirname(fileURLToPath(import.meta.url))
const uiRoot = resolve(scriptDir, '../..')
const repoRoot = resolve(uiRoot, '..')
const testResultsDir = resolve(uiRoot, 'test-results')
const selectionPath = resolve(testResultsDir, 'screencast-selection.json')

function toPosix(path) {
  return path.split(sep).join('/')
}

function repoRelative(path) {
  const normalized = toPosix(path.trim()).replace(/^\.\//, '')
  if (!normalized) return ''
  if (normalized.startsWith('ui/')) return normalized
  if (normalized.startsWith('crates/')) return normalized
  return `ui/${normalized}`
}

function listSpecFiles(dir = resolve(uiRoot, 'tests/ui')) {
  const entries = readdirSync(dir, { withFileTypes: true })
  return entries.flatMap((entry) => {
    const absolute = resolve(dir, entry.name)
    if (entry.isDirectory()) return listSpecFiles(absolute)
    if (!entry.isFile() || !entry.name.endsWith('.spec.ts')) return []
    return [toPosix(relative(uiRoot, absolute))]
  }).sort()
}

function gitDiffChangedFiles() {
  const candidates = [
    process.env.PW_UI_DIFF_BASE,
    process.env.GITHUB_BASE_SHA,
    process.env.GITHUB_BASE_REF ? `origin/${process.env.GITHUB_BASE_REF}` : undefined,
    'origin/main',
    'main',
  ].filter(Boolean)

  for (const base of candidates) {
    const result = spawnSync('git', ['diff', '--name-only', `${base}...HEAD`], {
      cwd: repoRoot,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    if (result.status === 0) {
      return result.stdout.split('\n').map((line) => line.trim()).filter(Boolean)
    }
  }

  const fallback = spawnSync('git', ['diff', '--name-only', 'HEAD~1...HEAD'], {
    cwd: repoRoot,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  if (fallback.status === 0) {
    return fallback.stdout.split('\n').map((line) => line.trim()).filter(Boolean)
  }

  throw new Error(`Could not determine changed files. Last git error: ${fallback.stderr}`)
}

function changedFilesFromInput() {
  const argvFiles = process.argv.slice(2).filter((arg) => !arg.startsWith('-'))
  if (argvFiles.length > 0) return argvFiles

  const envFiles = process.env.PW_UI_CHANGED_FILES
  if (envFiles) {
    return envFiles.split(/[\n,]/).map((line) => line.trim()).filter(Boolean)
  }

  return gitDiffChangedFiles()
}

function selectSpecs(changedFiles, allSpecs) {
  const selected = new Set()
  const reasons = []
  const selectAll = (reason) => {
    for (const spec of allSpecs) selected.add(spec)
    reasons.push(reason)
  }

  for (const changed of changedFiles.map(repoRelative).filter(Boolean)) {
    if (changed.startsWith('crates/coral-api/proto/')) {
      selectAll(`${changed}: protobuf contract can affect generated UI clients`)
      continue
    }

    if (!changed.startsWith('ui/')) continue
    const uiRelative = changed.slice('ui/'.length)

    if (allSpecs.includes(uiRelative)) {
      selected.add(uiRelative)
      reasons.push(`${changed}: changed Playwright spec`)
      continue
    }

    if (
      uiRelative === 'playwright.config.ts' ||
      uiRelative === 'package.json' ||
      uiRelative === 'package-lock.json' ||
      uiRelative === 'tsconfig.tests.json' ||
      uiRelative === 'vite.config.ts' ||
      uiRelative === 'index.html' ||
      uiRelative.startsWith('src/') ||
      uiRelative.startsWith('tests/ui/support/') ||
      uiRelative === 'tests/ui/playwright.setup.ts'
    ) {
      selectAll(`${changed}: shared UI/test dependency; run all UI screencasts`)
    }
  }

  return { specs: [...selected].sort(), reasons: [...new Set(reasons)] }
}

const allSpecs = listSpecFiles()
const changedFiles = changedFilesFromInput()
const selection = selectSpecs(changedFiles, allSpecs)

mkdirSync(testResultsDir, { recursive: true })
writeFileSync(selectionPath, `${JSON.stringify({ changedFiles, allSpecs, ...selection }, null, 2)}\n`)

if (selection.specs.length === 0) {
  console.log('No UI Playwright screencasts selected for changed files:')
  for (const file of changedFiles) console.log(`  - ${file}`)
  console.log(`Selection manifest: ${toPosix(relative(repoRoot, selectionPath))}`)
  process.exit(0)
}

console.log('Recording UI screencasts for selected Playwright specs:')
for (const spec of selection.specs) console.log(`  - ${spec}`)
console.log('Selection reasons:')
for (const reason of selection.reasons) console.log(`  - ${reason}`)
console.log(`Selection manifest: ${toPosix(relative(repoRoot, selectionPath))}`)

const command = process.platform === 'win32' ? 'playwright.cmd' : 'playwright'
const result = spawnSync(command, ['test', '--workers=1', ...selection.specs], {
  cwd: uiRoot,
  env: {
    ...process.env,
    PW_UI_SCREENCAST: '1',
    PW_UI_REVIEW_PAUSE_MS: process.env.PW_UI_REVIEW_PAUSE_MS ?? '1200',
  },
  stdio: 'inherit',
})

process.exit(result.status ?? 1)
