/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly CORAL_DESKTOP_APP: boolean
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}

declare module '*.css' {}
