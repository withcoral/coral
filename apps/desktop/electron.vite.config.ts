import { defineConfig, externalizeDepsPlugin } from 'electron-vite'

export default defineConfig({
  main: {
    plugins: [externalizeDepsPlugin()],
    // Bake the release flag into the bundle. Only release packages set
    // CORAL_DESKTOP_RELEASE=1; unsigned QA and local builds bake `false` so
    // the auto-updater stays inert (it cannot install updates into an
    // unsigned app, and only release builds publish an update feed).
    define: {
      __CORAL_DESKTOP_RELEASE__: JSON.stringify(process.env.CORAL_DESKTOP_RELEASE === '1'),
    },
    build: {
      rollupOptions: {
        input: { index: 'src/main/index.ts' },
      },
    },
  },
  preload: {
    plugins: [externalizeDepsPlugin()],
    build: {
      rollupOptions: {
        input: { index: 'src/preload/index.ts' },
        output: {
          format: 'cjs',
          entryFileNames: '[name].cjs',
        },
      },
    },
  },
})
