import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

async function packageJson(relativePath) {
  return JSON.parse(await readFile(new URL(relativePath, import.meta.url), 'utf8'))
}

test('desktop packages every Coral UI runtime dependency', async () => {
  const [desktopPackage, coralUIPackage] = await Promise.all([
    packageJson('../package.json'),
    packageJson('../../coral-ui/package.json'),
  ])
  const desktopDependencies = desktopPackage.dependencies ?? {}
  const coralUIDependencies = Object.keys(coralUIPackage.dependencies ?? {})
  const missingDependencies = coralUIDependencies.filter(
    (dependency) => !(dependency in desktopDependencies),
  )

  assert.deepEqual(
    missingDependencies,
    [],
    `packaged Coral UI server dependencies missing from apps/desktop/package.json: ${missingDependencies.join(', ')}`,
  )
})
