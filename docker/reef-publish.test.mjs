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
  assert.doesNotMatch(coral, /major-minor=/)
  assert.match(coral, /group: docker-publish-\$\{\{ inputs\.tag \}\}/)
  assert.match(
    release,
    /coordinate-docker-aliases:\n(?:.|\n)*- publish-docker\n(?:.|\n)*- publish-reef-docker/,
  )
  assert.match(aliases, /group: coordinated-docker-aliases/)
  assert.match(aliases, /value: \$\{\{ jobs\.aliases\.outputs\.make-latest \}\}/)
  assert.match(aliases, /make-latest: \$\{\{ steps\.advance\.outputs\.make-latest \}\}/)
  assert.match(aliases, /test "\$draft" = false/)
  assert.match(aliases, /\[ "\$TAG" != "\$newest_line" \] \|\| aliases\+=\("\$line"\)/)
  assert.match(aliases, /if \[ "\$TAG" = "\$newest" \]; then\n\s+aliases\+=\(latest\)/)
  assert.match(aliases, /make_latest=true/)
  assert.match(aliases, /echo "make-latest=\$\{make_latest\}"/)
})

test('alias publication converges absent, same, mismatch, and partial-failure states', async () => {
  const workflow = await read('../.github/workflows/docker-aliases.yml')
  const operations = [
    ['write-coral', workflow.indexOf('imagetools create --tag "ghcr.io/withcoral/coral:${alias}"')],
    ['write-reef', workflow.indexOf('imagetools create --tag "ghcr.io/withcoral/reef:${alias}"')],
    ['read-coral', workflow.indexOf('coral_actual="$(docker buildx imagetools inspect')],
    ['read-reef', workflow.indexOf('reef_actual="$(docker buildx imagetools inspect')],
  ].sort((left, right) => left[1] - right[1])

  assert.deepEqual(
    operations.map(([name, offset]) => [name, offset >= 0]),
    [
      ['write-coral', true],
      ['write-reef', true],
      ['read-coral', true],
      ['read-reef', true],
    ],
  )
  assert.match(workflow, /test "\$coral_actual" = "\$CORAL_DIGEST"/)
  assert.match(workflow, /test "\$reef_actual" = "\$REEF_DIGEST"/)

  const expected = { coral: 'coral-new', reef: 'reef-new' }
  const run = (initial, failAfter = Number.POSITIVE_INFINITY) => {
    const state = { ...initial }
    for (const [index, [operation]] of operations.entries()) {
      if (index === failAfter) throw Object.assign(new Error('injected failure'), { state })
      if (operation === 'write-coral') state.coral = expected.coral
      if (operation === 'write-reef') state.reef = expected.reef
      if (operation === 'read-coral') assert.equal(state.coral, expected.coral)
      if (operation === 'read-reef') assert.equal(state.reef, expected.reef)
    }
    return state
  }

  for (const initial of [{}, expected, { coral: 'old', reef: 'old' }]) {
    assert.deepEqual(run(initial), expected)
  }
  for (let failAfter = 0; failAfter < operations.length; failAfter += 1) {
    let partial
    assert.throws(
      () => run({}, failAfter),
      (error) => ((partial = error.state), true),
    )
    assert.deepEqual(run(partial), expected)
  }
})

test('release promotion is coordinated and notification is independently retryable', async () => {
  const release = await read('../.github/workflows/release.yml')
  const coordinate = release.indexOf('coordinate-docker-aliases:')
  const promote = release.indexOf('promote-release:', coordinate)
  const notify = release.indexOf('notify-release:', promote)

  assert.ok(coordinate >= 0 && coordinate < promote && promote < notify)
  assert.match(release.slice(promote, notify), /- coordinate-docker-aliases/)
  assert.match(release.slice(promote, notify), /environment: release/)
  assert.match(
    release.slice(promote, notify),
    /outputs:\n\s+promoted: \$\{\{ steps\.promote\.outputs\.promoted \}\}/,
  )
  assert.match(
    release.slice(promote, notify),
    /MAKE_LATEST: \$\{\{ needs\.coordinate-docker-aliases\.outputs\.make-latest \}\}/,
  )
  assert.match(release.slice(promote, notify), /if \[ "\$was_prerelease" = true \]/)
  assert.match(
    release.slice(promote, notify),
    /-F prerelease=false -f "make_latest=\$\{MAKE_LATEST\}"/,
  )
  assert.doesNotMatch(release.slice(promote, notify), /DISCORD_WEBHOOK/)
  assert.match(release.slice(notify), /- promote-release/)
  assert.match(release.slice(notify), /environment: release/)
  assert.match(
    release.slice(notify),
    /if: needs\.promote-release\.outputs\.promoted == 'true'/,
  )
  assert.match(release.slice(notify), /DISCORD_WEBHOOK/)
  assert.doesNotMatch(release.slice(0, coordinate), /releases\/tags\/\$\{TAG_NAME\}/)
  assert.doesNotMatch(release.slice(0, coordinate), /-F prerelease=false/)
})

test('validation watches every workflow consumed by the release contract', async () => {
  const validate = await read('../.github/workflows/validate.yml')
  const reefImagePaths = validate.slice(
    validate.indexOf('            reef-image:\n'),
    validate.indexOf('            desktop-checks:\n'),
  )

  for (const workflow of [
    'docker-aliases.yml',
    'docker-publish.yml',
    'reef-docker-publish.yml',
    'release.yml',
  ]) {
    assert.match(reefImagePaths, new RegExp(`\\.github/workflows/${workflow.replace('.', '\\.')}`))
  }
  assert.match(reefImagePaths, /docker\/reef-publish\.test\.mjs/)
})
