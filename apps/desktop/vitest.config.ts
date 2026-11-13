import { defineConfig } from 'vitest/config'

// CI sets this on Windows, where a worker has crashed without a trace. Node
// then writes a report there of a fatal error it catches; a native fast-fail
// can bypass Node, and the job's Windows minidump covers that.
const reports = process.env.CORAL_TEST_REPORT_DIRECTORY

export default defineConfig({
  test: {
    environment: 'node',
    include: ['src/**/*.test.ts', '*.test.ts'],
    execArgv: reports ? ['--report-on-fatalerror', `--report-directory=${reports}`] : [],
  },
})
