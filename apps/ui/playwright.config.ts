import { defineConfig, devices } from '@playwright/test'

const viewport = { width: 1440, height: 900 }
const testTimeout = process.env.PW_UI_SCREENCAST === '1' ? 90_000 : 30_000

export default defineConfig({
  testDir: './tests/ui',
  fullyParallel: true,
  reporter: [['list']],
  retries: process.env.CI ? 2 : 0,
  timeout: testTimeout,
  use: {
    baseURL: 'http://127.0.0.1:5178',
    trace: 'on-first-retry',
    viewport,
  },
  webServer: {
    command: 'npm run dev -- --host 127.0.0.1 --port 5178 --strictPort',
    url: 'http://127.0.0.1:5178',
    reuseExistingServer: false,
    stdout: 'pipe',
    stderr: 'pipe',
    timeout: 120_000,
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'], viewport },
    },
  ],
})
