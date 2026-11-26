import { expect, it } from 'vitest'

import { wax } from './vite.js'

// Catches: a dependency that the dev server of an app cannot find, because the app
// does not install it itself, or a peer that the server bundles a second time.
it('pre-bundles a dependency through Wax and a peer by its own name', () => {
  const { optimizeDeps } = (wax().config as () => { optimizeDeps: Record<string, string[]> })()

  expect(optimizeDeps.exclude).toEqual(['@withcoral/wax'])
  expect(optimizeDeps.include).toContain('@withcoral/wax > classnames')
  expect(optimizeDeps.include).toContain('@withcoral/wax > @base-ui/react/dialog')
  expect(optimizeDeps.include).toContain('react')
  expect(optimizeDeps.include).toContain('jotai/utils')
  expect(optimizeDeps.include).not.toContain('@withcoral/wax > react')
  expect(optimizeDeps.include).not.toContain('classnames')
})
