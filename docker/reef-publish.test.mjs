import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8')

test('Reef publication is immutable and release-source pinned', async () => {
  const workflow = await read('../.github/workflows/reef-docker-publish.yml')

  assert.match(workflow, /IMAGE: ghcr\.io\/withcoral\/reef/)
  assert.match(workflow, /ref: \$\{\{ env\.TAG \}\}/)
  assert.match(workflow, /git rev-parse HEAD/)
  assert.match(workflow, /--platform linux\/amd64/)
  assert.match(workflow, /--provenance=true/)
  assert.match(workflow, /--tag "\$\{IMAGE\}:\$\{VERSION\}"/)
  assert.doesNotMatch(workflow, /\$\{IMAGE\}:(?:latest|\$\{MAJOR_MINOR\})/)
})

test('publication exposes verified version and digest metadata', async () => {
  const [workflow, dockerfile] = await Promise.all([
    read('../.github/workflows/reef-docker-publish.yml'),
    read('./Dockerfile.reef'),
  ])

  assert.match(workflow, /digest: \$\{\{ steps\.manifest\.outputs\.digest \}\}/)
  assert.match(workflow, /org\.opencontainers\.image\.version/)
  assert.match(workflow, /org\.opencontainers\.image\.revision/)
  assert.match(workflow, /docker buildx imagetools inspect/)
  assert.match(dockerfile, /org\.opencontainers\.image\.version="\$REEF_VERSION"/)
  assert.match(dockerfile, /org\.opencontainers\.image\.revision="\$REEF_REVISION"/)
})

test('release waits for completed artifacts before publishing Reef', async () => {
  const workflow = await read('../.github/workflows/release.yml')

  assert.match(workflow, /publish-reef-docker:\n(?:.|\n)*- publish-version/)
  assert.match(workflow, /uses: \.\/\.github\/workflows\/reef-docker-publish\.yml/)
  assert.match(workflow, /commit-sha: \$\{\{ needs\.validate-release-ref\.outputs\.commit-sha \}\}/)
})
