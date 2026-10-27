import { defineConfig } from 'vitest/config'
import { checkSuiteDatabase, reefRoot } from './support/config.ts'

const suite = process.env.E2E_SUITE || 'main'
const database = process.env.E2E_DATABASE || 'sqlite'
const probes = process.env.E2E_HARNESS_PROBES === '1'
checkSuiteDatabase(suite, database, probes)

export default defineConfig({
  root: reefRoot,
  test: {
    environment: 'node',
    projects: [
      {
        extends: true,
        test: {
          name: probes ? 'harness' : suite === 'main' ? `main-${database}` : suite,
          include: probes
            ? ['e2e/probes/*.probe.ts']
            : suite === 'local-mode' && process.env.E2E_BASE_URL
              ? ['e2e/scenarios/local-mode/entry.test.ts']
              : [`e2e/scenarios/${suite}/**/*.test.ts`],
          globalSetup: ['e2e/global-setup.ts'],
        },
      },
    ],
    fileParallelism: false,
    maxWorkers: 1,
    testTimeout: 60_000,
    hookTimeout: 90_000,
    teardownTimeout: 30_000,
    retry: 0,
    reporters: ['default', './e2e/support/scenario-reporter.ts'],
  },
})
