import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8')

test('every Node image matches the Reef toolchain version', async () => {
  const [dockerfile, nodeVersionFile] = await Promise.all([
    read('./Dockerfile.reef'),
    read('../apps/reef/.node-version'),
  ])
  const nodeVersion = nodeVersionFile.trim()
  const nodeFromLines = dockerfile.match(/^FROM .*\bnode(?::\S+)?(?:\s|$).*$/gm) ?? []

  assert.ok(nodeVersion, 'apps/reef/.node-version must not be empty')
  assert.ok(nodeFromLines.length > 0, 'Dockerfile.reef must declare at least one Node image')
  const expectedTag = new RegExp(`\\bnode:${nodeVersion.replaceAll('.', '\\.')}(-|\\s|$)`)
  for (const fromLine of nodeFromLines) {
    assert.match(fromLine, expectedTag)
  }
})

test('runtime stage is COPY-only, non-root, and health checked', async () => {
  const source = await read('./Dockerfile.reef')
  const runtime = source.slice(source.lastIndexOf('\nFROM '))

  assert.doesNotMatch(runtime, /^RUN\s/m)
  assert.match(runtime, /^USER node$/m)
  assert.match(runtime, /^EXPOSE 3000$/m)
  assert.match(runtime, /^HEALTHCHECK .* CMD .*\/healthz/m)
  assert.match(source, /^FROM --platform=\$BUILDPLATFORM .* AS (?:build|deps)$/gm)
  assert.equal([...source.matchAll(/^RUN\s/gm)].length, 3)
})

test('image copies production output instead of published binaries', async () => {
  const source = await read('./Dockerfile.reef')

  assert.match(source, /COPY --from=build \/src\/apps\/reef\/build \.\/build/)
  assert.doesNotMatch(source, /curl|wget|ghcr\.io\/withcoral\/coral/)
})

test('smoke harness covers supported config shapes and failure policy', async () => {
  const source = await read('./reef-smoke.sh')

  for (const marker of [
    'container:',
    'REEF_ALLOW_INSECURE_CORAL_ENDPOINT=1',
    'https://coral-tls:443',
    'invalid cleartext config',
    'REEF_AUTH_MODE=disabled',
    'process.getuid()',
    '/readyz',
    'reef-smoke-nginx.conf',
  ]) {
    assert.match(source, new RegExp(marker.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')))
  }
})

test('validation watches and aggregates the Reef image contract', async () => {
  const workflow = await read('../.github/workflows/validate.yml')

  assert.match(workflow, /reef-image:\n(?:.|\n)*docker\/Dockerfile\.reef/)
  assert.match(workflow, /needs:\n(?:.|\n)*reef-image,/)
  assert.match(workflow, /REEF_IMAGE_RESULT: \$\{\{ needs\.reef-image\.result \}\}/)
  assert.match(workflow, /"reef-image:\$REEF_IMAGE_RESULT"/)
  assert.match(workflow, /make docker-build DOCKER_IMAGE=coral:local/)
  assert.match(workflow, /make reef-docker-build REEF_DOCKER_IMAGE=reef:local/)
  assert.match(workflow, /make reef-docker-smoke/)
})
