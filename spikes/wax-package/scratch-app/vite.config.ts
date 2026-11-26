// SPIKE (withcoral/coral#3541). An app with Vanilla Extract, plus the one plugin that
// the package ships. `wax()` holds the settings the trial found necessary; the comments
// in its source (`vite.js` in the package) give the error that each one removes.
import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'
import react from '@vitejs/plugin-react'
import { wax } from '@withcoral/wax/vite.js'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react(), vanillaExtractPlugin(), wax()],
})
