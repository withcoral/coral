import assert from 'node:assert/strict'
import test from 'node:test'

import { createConfig } from '../electron.vite.config.ts'

function releaseDefine(env) {
  return createConfig(env).main?.define?.__CORAL_DESKTOP_RELEASE__
}

test('non-release builds compile the updater out', () => {
  assert.equal(releaseDefine({}), 'false')
})

test('release builds compile the updater in', () => {
  assert.equal(releaseDefine({ CORAL_DESKTOP_RELEASE: '1' }), 'true')
})
