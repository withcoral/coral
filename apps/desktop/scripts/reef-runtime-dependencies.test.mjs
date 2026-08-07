import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

async function packageJson(relativePath) {
  return JSON.parse(await readFile(new URL(relativePath, import.meta.url), 'utf8'))
}

test('desktop packages every Reef runtime dependency', async () => {
  const [desktopPackage, reefPackage] = await Promise.all([
    packageJson('../package.json'),
    packageJson('../../reef/package.json'),
  ])
  const desktopDependencies = desktopPackage.dependencies ?? {}
  const reefDependencies = Object.keys(reefPackage.dependencies ?? {})
  const missingDependencies = reefDependencies.filter(
    (dependency) => !(dependency in desktopDependencies),
  )

  assert.deepEqual(
    missingDependencies,
    [],
    `packaged Reef server dependencies missing from apps/desktop/package.json: ${missingDependencies.join(', ')}`,
  )
})

// Declaring a dependency is only half of packaging it. `npm ci` refuses to
// install when the manifest names something the lockfile does not resolve, so a
// manifest-only edit turns the test above green and breaks the install step that
// runs before it — which is exactly how a missing `@connectrpc/connect-node`
// lock entry reached CI once already.
test('the desktop lockfile resolves every declared dependency', async () => {
  const [desktopPackage, lockfile] = await Promise.all([
    packageJson('../package.json'),
    packageJson('../package-lock.json'),
  ])
  const declared = {
    ...(desktopPackage.dependencies ?? {}),
    ...(desktopPackage.devDependencies ?? {}),
  }
  const lockRoot = lockfile.packages?.[''] ?? {}
  const lockedRootDependencies = {
    ...(lockRoot.dependencies ?? {}),
    ...(lockRoot.devDependencies ?? {}),
  }

  const unlocked = Object.keys(declared).filter(
    (name) =>
      lockedRootDependencies[name] !== declared[name] ||
      !(`node_modules/${name}` in (lockfile.packages ?? {})),
  )

  assert.deepEqual(
    unlocked,
    [],
    `apps/desktop/package-lock.json is out of sync with package.json — npm ci will fail on: ${unlocked.join(', ')}. Run \`npm install --package-lock-only --prefix apps/desktop\`.`,
  )
})
