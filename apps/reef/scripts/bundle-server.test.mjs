import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { test } from 'node:test'
import { promisify } from 'node:util'

import { bundleServer } from './bundle-server.mjs'

const exec = promisify(execFile)

// A miniature Reef: an entry that imports its build through the same literal
// path server.js uses, and a build that imports an installed dependency.
const serverBuild = (name) =>
  `import { greet } from 'greeter'\nexport const message = greet('${name}')\n`

async function fixture(files) {
  const root = await mkdtemp(join(tmpdir(), 'reef-bundle-'))
  const all = {
    'server.js': `const build = await import('./build/server/index.js')\nconsole.log(build.message)\n`,
    'build/server/index.js': serverBuild('bundle'),
    'node_modules/greeter/package.json': '{"name":"greeter","main":"index.js"}',
    'node_modules/greeter/index.js': `exports.greet = (name) => require('node:util').format('hello %s', name)\n`,
    ...files,
  }
  for (const [path, content] of Object.entries(all)) {
    await mkdir(dirname(join(root, path)), { recursive: true })
    await writeFile(join(root, path), content)
  }
  return root
}

test('bundles the server, its build and CommonJS dependencies into one runnable file', async () => {
  const root = await fixture({})
  // Desktop stages its build outside the checkout; server.mjs lands beside it.
  const stage = await mkdtemp(join(tmpdir(), 'reef-stage-'))
  try {
    const staged = join(stage, 'build')
    await mkdir(join(staged, 'server'), { recursive: true })
    await writeFile(join(staged, 'server', 'index.js'), serverBuild('staged'))
    const outfile = await bundleServer({ entry: join(root, 'server.js'), buildDirectory: staged })

    assert.equal(outfile, join(stage, 'server.mjs'))
    await rm(root, { recursive: true, force: true })
    const { stdout } = await exec(process.execPath, [outfile], { cwd: stage })
    assert.equal(stdout, 'hello staged\n')
  } finally {
    await rm(root, { recursive: true, force: true })
    await rm(stage, { recursive: true, force: true })
  }
})

test('refuses a bundle that would need files it does not contain', async () => {
  for (const [name, files, buildDirectory, expected] of [
    [
      'a native addon in a bundled package',
      {
        'node_modules/greeter/index.js': `const addon = { exports: {} }\nprocess.dlopen(addon, __dirname + '/build/greeter.node')\nexports.greet = addon.exports.greet\n`,
        'node_modules/greeter/build/greeter.node': '',
      },
      'build',
      /native addon.*greeter[/\\]build[/\\]greeter\.node/,
    ],
    [
      'a dependency resolved at run time',
      {
        'node_modules/greeter/index.js': `const id = process.env.GREETER\nexports.greet = require(id)\n`,
      },
      'build',
      /greeter[/\\]index\.js:2: .*"require" will not be bundled/,
    ],
    [
      'a build directory whose client files the server cannot find',
      {},
      'output',
      /must be named "build"/,
    ],
  ]) {
    const root = await fixture(files)
    try {
      await assert.rejects(
        bundleServer({
          entry: join(root, 'server.js'),
          buildDirectory: join(root, buildDirectory),
        }),
        expected,
        name,
      )
    } finally {
      await rm(root, { recursive: true, force: true })
    }
  }
})
