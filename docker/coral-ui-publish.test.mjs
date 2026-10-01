import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8')

test('Coral UI publication is immutable and release-source pinned', async () => {
  const workflow = await read('../.github/workflows/coral-ui-docker-publish.yml')

  assert.match(workflow, /IMAGE: ghcr\.io\/withcoral\/coral-ui/)
  assert.match(workflow, /ref: \$\{\{ env\.TAG \}\}/)
  assert.match(workflow, /git rev-parse HEAD/)
  assert.match(workflow, /--platform linux\/amd64/)
  assert.match(workflow, /--provenance=true/)
  assert.match(workflow, /push-by-digest=true/)
  assert.doesNotMatch(workflow, /\$\{IMAGE\}:(?:latest|\$\{MAJOR_MINOR\})/)
})

test('publication verifies and signs the digest before exposing its version tag', async () => {
  const [workflow, dockerfile] = await Promise.all([
    read('../.github/workflows/coral-ui-docker-publish.yml'),
    read('./Dockerfile.coral-ui'),
  ])

  assert.match(workflow, /digest: \$\{\{ steps\.publish\.outputs\.digest \}\}/)
  assert.match(workflow, /org\.opencontainers\.image\.version/)
  assert.match(workflow, /org\.opencontainers\.image\.revision/)
  assert.match(workflow, /docker buildx imagetools inspect/)
  assert.match(dockerfile, /org\.opencontainers\.image\.version="\$CORAL_UI_VERSION"/)
  assert.match(dockerfile, /org\.opencontainers\.image\.revision="\$CORAL_UI_REVISION"/)

  const verify = workflow.indexOf('- name: Verify staged image metadata')
  const smoke = workflow.indexOf('- name: Smoke staged Coral UI image')
  const sign = workflow.indexOf('- name: Sign the staged manifest')
  const publish = workflow.indexOf('- name: Publish immutable version tag')
  assert.ok(verify < smoke && smoke < sign && sign < publish)
  assert.match(workflow.slice(smoke, sign), /"\$\{IMAGE\}@\$\{DIGEST\}"/)
  assert.match(workflow.slice(smoke, sign), /State\.Health\.Status/)
  assert.match(workflow.slice(smoke, sign), /process\.getuid\(\).*process\.getgid\(\)/)
  assert.match(workflow.slice(smoke, sign), /= 1000:1000/)
  assert.doesNotMatch(workflow.slice(0, publish), /--tag "?\$\{IMAGE\}:\$\{VERSION\}/)
})

test('Coral UI reruns reuse the signed immutable version digest without rebuilding it', async () => {
  const workflow = await read('../.github/workflows/coral-ui-docker-publish.yml')
  const resolve = workflow.slice(
    workflow.indexOf('- name: Resolve existing immutable version'),
    workflow.indexOf('- name: Build and push image by digest'),
  )
  const publish = workflow.slice(workflow.indexOf('- name: Publish immutable version tag'))

  assert.match(workflow, /group: coral-ui-docker-publish-\$\{\{ inputs\.tag \}\}/)
  assert.match(resolve, /matches="\$\(gh api --paginate/)
  assert.match(resolve, /test "\$\(printf '%s\\n' "\$matches" \| wc -l\)" -eq 1/)
  assert.match(resolve, /resolved" != "\$digest"/)
  assert.match(resolve, /elif grep -Fq '\(HTTP 404\)'/)
  assert.match(resolve, /cat "\$error_file" >&2\n\s+exit 1/)
  assert.match(workflow, /if: steps\.existing\.outputs\.digest == ''\n\s+id: build/)
  assert.match(workflow, /if: steps\.existing\.outputs\.digest != ''[\s\S]*cosign verify/)
  assert.match(
    workflow,
    /EXPECTED_IDENTITY: \$\{\{ github\.server_url \}\}\/\$\{\{ github\.repository \}\}\/\.github\/workflows\/coral-ui-docker-publish\.yml@\$\{\{ github\.ref \}\}/,
  )
  assert.doesNotMatch(workflow, /EXPECTED_IDENTITY: .*github\.workflow_ref/)
  assert.match(workflow, /digest="\$\{EXISTING_DIGEST:-\$BUILD_DIGEST\}"/)
  assert.match(publish, /if \[ -z "\$EXISTING_DIGEST" \]/)
  assert.match(publish, /imagetools create --tag "\$version_ref"/)
  assert.match(publish, /published_digest" != "\$DIGEST"/)
})

test('release waits for completed artifacts before publishing Coral UI', async () => {
  const workflow = await read('../.github/workflows/release.yml')

  assert.match(workflow, /publish-coral-ui-docker:\n(?:.|\n)*- publish-version/)
  assert.match(workflow, /uses: \.\/\.github\/workflows\/coral-ui-docker-publish\.yml/)
  assert.match(workflow, /commit-sha: \$\{\{ needs\.validate-release-ref\.outputs\.commit-sha \}\}/)
})

test('release aliases stay paired while direct Coral CVE rebuilds advance eligible aliases', async () => {
  const [release, coral, aliases] = await Promise.all([
    read('../.github/workflows/release.yml'),
    read('../.github/workflows/docker-publish.yml'),
    read('../.github/workflows/docker-aliases.yml'),
  ])

  assert.match(coral, /defer-aliases:/)
  assert.match(coral, /format\('docker-publish-deferred-\{0\}', github\.run_id\)/)
  assert.match(coral, /\|\| 'coordinated-docker-aliases'/)
  assert.match(coral, /group: docker-publish-\$\{\{ inputs\.tag \}\}/)
  assert.match(coral, /major-minor=\$\{version%\.\*\}/)
  assert.match(coral, /if: \$\{\{ inputs\.defer-aliases != true \}\}/)
  assert.match(coral, /- name: Snapshot direct rebuild alias ownership/)
  assert.match(coral, /if ! matches="\$\(gh api --paginate/)
  assert.match(coral, /if ! version_digest="\$\(tag_digest "\$VERSION"\)"; then\n\s+exit 1/)
  assert.match(
    coral,
    /if ! alias_digest="\$\(tag_digest "\$alias"\)"; then\n\s+return 2/,
  )
  assert.match(coral, /\[ "\$status" -eq 1 \] \|\| exit "\$status"/)
  assert.match(
    coral,
    /\[ -z "\$alias_digest" \] \|\| \{ \[ -n "\$version_digest" \] && \[ "\$alias_digest" = "\$version_digest" \]; \}/,
  )
  assert.match(coral, /if \[ "\$DEFER_ALIASES" != true \]; then/)
  assert.match(coral, /\[ "\$PRERELEASE" = false \] && \[ "\$NEWEST_LINE_TAG" = "\$TAG" \]/)
  assert.match(coral, /\[ "\$PRERELEASE" = false \] && \[ "\$NEWEST_TAG" = "\$TAG" \]/)
  assert.match(coral, /echo "move-line=\$\{move_line\}"/)
  assert.match(coral, /echo "move-latest=\$\{move_latest\}"/)
  assert.match(coral, /if \[ "\$MOVE_LINE" = true \]; then/)
  assert.match(coral, /if \[ "\$MOVE_LATEST" = true \]; then/)
  assert.match(coral, /tags\+=\(--tag "\$\{IMAGE\}:\$\{MAJOR_MINOR\}"\)/)
  assert.match(coral, /tags\+=\(--tag "\$\{IMAGE\}:latest"\)/)
  assert.match(release, /publish-docker:[\s\S]*defer-aliases: true/)
  assert.match(
    release,
    /coordinate-docker-aliases:\n(?:.|\n)*- publish-docker\n(?:.|\n)*- publish-coral-ui-docker/,
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

test('Coral signs the staged digest before exposing the exact version or moving tags', async () => {
  const workflow = await read('../.github/workflows/docker-publish.yml')
  const sign = workflow.indexOf('- name: Sign the staged manifest')
  const publish = workflow.indexOf('- name: Tag the validated manifest')

  assert.ok(sign >= 0 && sign < publish)
  assert.match(workflow.slice(sign, publish), /DIGEST: \$\{\{ steps\.build\.outputs\.digest \}\}/)
  assert.match(workflow.slice(sign, publish), /cosign sign --yes "\$\{IMAGE\}@\$\{DIGEST\}"/)
  assert.doesNotMatch(workflow.slice(0, sign), /imagetools create/)
  assert.match(workflow.slice(publish), /test "\$digest" = "\$BUILD_DIGEST"/)
})

test('alias publication adopts the current exact-version Coral digest and converges after partial failures', async () => {
  const workflow = await read('../.github/workflows/docker-aliases.yml')
  const resolveCoral = workflow.indexOf('if ! coral_digest="$(docker buildx imagetools inspect')
  const operations = [
    ['write-coral', workflow.indexOf('imagetools create --tag "ghcr.io/withcoral/coral:${alias}"')],
    ['write-coral-ui', workflow.indexOf('imagetools create --tag "ghcr.io/withcoral/coral-ui:${alias}"')],
    ['read-coral', workflow.indexOf('coral_actual="$(docker buildx imagetools inspect')],
    ['read-coral-ui', workflow.indexOf('coral_ui_actual="$(docker buildx imagetools inspect')],
  ].sort((left, right) => left[1] - right[1])

  assert.deepEqual(
    operations.map(([name, offset]) => [name, offset >= 0]),
    [
      ['write-coral', true],
      ['write-coral-ui', true],
      ['read-coral', true],
      ['read-coral-ui', true],
    ],
  )
  assert.ok(resolveCoral >= 0 && resolveCoral < operations[0][1])
  assert.match(workflow, /PUBLISHED_CORAL_DIGEST: \$\{\{ inputs\.coral-digest \}\}/)
  assert.match(workflow, /if \[ "\$coral_digest" != "\$PUBLISHED_CORAL_DIGEST" \]; then/)
  assert.match(workflow, /"ghcr\.io\/withcoral\/coral@\$\{coral_digest\}"/)
  assert.doesNotMatch(workflow, /"ghcr\.io\/withcoral\/coral@\$\{PUBLISHED_CORAL_DIGEST\}"/)
  assert.match(workflow, /test "\$coral_actual" = "\$coral_digest"/)
  assert.match(workflow, /test "\$coral_ui_actual" = "\$CORAL_UI_DIGEST"/)

  const releaseProducedCoral = 'coral-d1'
  const expected = { coral: 'coral-d2-current-exact-tag', coralUI: 'coral-ui-new' }
  assert.notEqual(expected.coral, releaseProducedCoral)
  const run = (initial, failAfter = Number.POSITIVE_INFINITY) => {
    const state = { ...initial }
    for (const [index, [operation]] of operations.entries()) {
      if (index === failAfter) throw Object.assign(new Error('injected failure'), { state })
      if (operation === 'write-coral') state.coral = expected.coral
      if (operation === 'write-coral-ui') state.coralUI = expected.coralUI
      if (operation === 'read-coral') assert.equal(state.coral, expected.coral)
      if (operation === 'read-coral-ui') assert.equal(state.coralUI, expected.coralUI)
    }
    return state
  }

  for (const initial of [{}, expected, { coral: 'old', coralUI: 'old' }]) {
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
  const coralUIImagePaths = validate.slice(
    validate.indexOf('            coral-ui-image:\n'),
    validate.indexOf('            desktop-checks:\n'),
  )

  for (const workflow of [
    'docker-aliases.yml',
    'docker-publish.yml',
    'coral-ui-docker-publish.yml',
    'release.yml',
  ]) {
    assert.match(coralUIImagePaths, new RegExp(`\\.github/workflows/${workflow.replace('.', '\\.')}`))
  }
  assert.match(coralUIImagePaths, /docker\/coral-ui-publish\.test\.mjs/)
})
