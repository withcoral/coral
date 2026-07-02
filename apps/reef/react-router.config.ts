import type { Config } from '@react-router/dev/config'

const isDesktopBuild = process.env.CORAL_DESKTOP_APP === '1'

export default {
  future: {
    v8_middleware: true,
  },
  // Desktop needs static renderer assets that Electron can load from the app bundle.
  // Browser-only state, such as the saved sidebar preference, is restored client-side.
  ssr: !isDesktopBuild,
} satisfies Config
