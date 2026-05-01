import path from "node:path";
import react from "@vitejs/plugin-react";
import { vanillaExtractPlugin } from "@vanilla-extract/vite-plugin";
import { defineConfig } from "vite";

const CORAL_SERVER_URL =
  process.env.CORAL_SERVER_URL ?? "http://127.0.0.1:1457";

export default defineConfig({
  plugins: [react(), vanillaExtractPlugin()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    port: 5173,
    proxy: {
      "^/coral\\.[^/]+\\.[^/]+/": {
        target: CORAL_SERVER_URL,
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
