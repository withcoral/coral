// The platforms a release build ships to, and what a release build entails on
// each. Both the packaging config (electron-builder.config.ts) and the running
// app (main/auto-update.ts) read this table, so they cannot disagree about who
// gets an update feed.
//
// Membership means the platform ships an updater: the package carries
// app-update.yml, the build writes a `latest-<platform>.yml` feed, and
// desktopUpdatesSupported() can return true there. Windows is absent because it
// has neither an updater nor a signing story yet; adding it here is what puts
// it on par.

export interface ReleaseTarget {
  // Apple code signing and notarization, which release mode then demands
  // credentials for.
  appleSigning: boolean
}

export const RELEASE_TARGETS = {
  // Squirrel.Mac refuses to install an update into an unsigned app, so macOS
  // updates and Apple signing always travel together.
  darwin: { appleSigning: true },
  // The AppImage replaces its own image file, and nothing inspects a signature
  // on the way. See desktopUpdatesSupported(): an installed deb belongs to
  // dpkg, so only the AppImage updates itself.
  linux: { appleSigning: false },
} as const satisfies Record<string, ReleaseTarget>

export const RELEASE_PLATFORMS = Object.keys(RELEASE_TARGETS)

// Null for a platform that ships no release build.
export function releaseTarget(platform: NodeJS.Platform): ReleaseTarget | null {
  const targets: Partial<Record<string, ReleaseTarget>> = RELEASE_TARGETS
  return targets[platform] ?? null
}
