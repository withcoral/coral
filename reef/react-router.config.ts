import type { Config } from '@react-router/dev/config'

const isDesktopBuild = process.env.CORAL_DESKTOP_APP === '1'

export default {
  future: {
    v8_middleware: true,
  },
  // Desktop needs static renderer assets that Electron can load from the app bundle.
  ssr: !isDesktopBuild,
} satisfies Config
