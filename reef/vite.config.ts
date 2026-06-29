import { reactRouter } from '@react-router/dev/vite'
import { vanillaExtractPlugin } from '@vanilla-extract/vite-plugin'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [vanillaExtractPlugin(), reactRouter()],
  resolve: {
    tsconfigPaths: true,
  },
})
