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
  assert.match(workflow, /push-by-digest=true/)
  assert.doesNotMatch(workflow, /\$\{IMAGE\}:(?:latest|\$\{MAJOR_MINOR\})/)
})

test('publication verifies and signs the digest before exposing its version tag', async () => {
  const [workflow, dockerfile] = await Promise.all([
    read('../.github/workflows/reef-docker-publish.yml'),
    read('./Dockerfile.reef'),
  ])

  assert.match(workflow, /digest: \$\{\{ steps\.publish\.outputs\.digest \}\}/)
  assert.match(workflow, /org\.opencontainers\.image\.version/)
  assert.match(workflow, /org\.opencontainers\.image\.revision/)
  assert.match(workflow, /docker buildx imagetools inspect/)
  assert.match(dockerfile, /org\.opencontainers\.image\.version="\$REEF_VERSION"/)
  assert.match(dockerfile, /org\.opencontainers\.image\.revision="\$REEF_REVISION"/)

  const verify = workflow.indexOf('- name: Verify staged image metadata')
  const sign = workflow.indexOf('- name: Sign the staged manifest')
  const publish = workflow.indexOf('- name: Publish immutable version tag')
  assert.ok(verify < sign && sign < publish)
  assert.doesNotMatch(workflow.slice(0, publish), /--tag "?\$\{IMAGE\}:\$\{VERSION\}/)
})

test('immutable version publication handles absent, same, and different digests', async () => {
  const workflow = await read('../.github/workflows/reef-docker-publish.yml')
  const publish = workflow.slice(workflow.indexOf('- name: Publish immutable version tag'))

  assert.match(workflow, /group: reef-docker-publish-\$\{\{ inputs\.tag \}\}/)
  assert.match(publish, /existing_digest="\$\(gh api --paginate/)
  assert.match(publish, /if \[ -n "\$existing_digest" \]/)
  assert.match(publish, /existing_digest" != "\$BUILD_DIGEST"/)
  assert.match(publish, /refusing to replace it/)
  assert.match(publish, /already points to the verified digest/)
  assert.match(publish, /imagetools create --tag "\$version_ref"/)
  assert.match(publish, /published_digest" != "\$BUILD_DIGEST"/)
})

test('release waits for completed artifacts before publishing Reef', async () => {
  const workflow = await read('../.github/workflows/release.yml')

  assert.match(workflow, /publish-reef-docker:\n(?:.|\n)*- publish-version/)
  assert.match(workflow, /uses: \.\/\.github\/workflows\/reef-docker-publish\.yml/)
  assert.match(workflow, /commit-sha: \$\{\{ needs\.validate-release-ref\.outputs\.commit-sha \}\}/)
})

test('moving aliases have one coordinator after both immutable publishers', async () => {
  const [release, coral, aliases] = await Promise.all([
    read('../.github/workflows/release.yml'),
    read('../.github/workflows/docker-publish.yml'),
    read('../.github/workflows/docker-aliases.yml'),
  ])

  assert.doesNotMatch(coral, /\$\{IMAGE\}:(?:latest|\$\{MAJOR_MINOR\})/)
  assert.match(release, /coordinate-docker-aliases:\n(?:.|\n)*- publish-docker\n(?:.|\n)*- publish-reef-docker/)
  assert.match(aliases, /group: coordinated-docker-aliases/)
  assert.match(aliases, /test "\$draft" = false/)
  assert.match(aliases, /\[ "\$TAG" != "\$newest_line" \] \|\| aliases\+=\("\$line"\)/)
  assert.match(aliases, /\[ "\$TAG" != "\$newest" \] \|\| aliases\+=\(latest\)/)
})

test('alias publication converges absent, same, mismatch, and partial-failure states', async () => {
  const workflow = await read('../.github/workflows/docker-aliases.yml')
  const coralWrite = workflow.indexOf('imagetools create --tag "ghcr.io/withcoral/coral:${alias}"')
  const reefWrite = workflow.indexOf('imagetools create --tag "ghcr.io/withcoral/reef:${alias}"')
  const coralRead = workflow.indexOf('coral_actual="$(docker buildx imagetools inspect')
  const reefRead = workflow.indexOf('reef_actual="$(docker buildx imagetools inspect')

  // create is an idempotent upsert: absent, same, and mismatched aliases all converge.
  assert.ok(coralWrite >= 0 && coralWrite < reefWrite)
  assert.ok(reefWrite < coralRead && coralRead < reefRead)
  assert.match(workflow, /test "\$coral_actual" = "\$CORAL_DIGEST"/)
  assert.match(workflow, /test "\$reef_actual" = "\$REEF_DIGEST"/)
  assert.match(workflow, /set -euo pipefail/)
  assert.match(workflow, /retry is idempotent and converges both aliases before completion/)
})

test('GitHub release promotion is the final coordinated commit point', async () => {
  const release = await read('../.github/workflows/release.yml')
  const coordinate = release.indexOf('coordinate-docker-aliases:')
  const promote = release.indexOf('promote-release:', coordinate)

  assert.ok(coordinate >= 0 && coordinate < promote)
  assert.match(release.slice(promote), /- coordinate-docker-aliases/)
  assert.match(release.slice(promote), /-F prerelease=false -F make_latest=true/)
  assert.doesNotMatch(release.slice(0, coordinate), /-F prerelease=false/)
})
