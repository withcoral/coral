import { defineConfig } from "vite";

const CORAL_SERVER_URL =
  process.env.CORAL_SERVER_URL ?? "http://127.0.0.1:1457";

// Proxy gRPC-Web traffic to the local Coral server so the browser sees a
// single same-origin endpoint during development.
export default defineConfig({
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
