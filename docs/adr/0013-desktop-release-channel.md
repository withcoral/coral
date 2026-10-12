---
status: accepted
---

# Desktop releases through a v1 feed of its own

Release builds of Desktop update from GitHub releases in `withcoral/coral-releases`, a public repository that holds
only release assets. The release workflow builds every platform, uploads the verified assets to a draft release there
and then publishes the draft, so an updater never sees a partial release. Only a release package names the feed. Every
other package has no `app-update.yml` and no update menu item.

The feed is separate because v1's code replaces the 0.x code in `withcoral/coral`, while 0.x installs keep
polling that repository's releases. A v1 feed there would update those installs into v1, which has another
app id and another data folder. A repository of its own has the same name before and after that replacement,
so the release path the betas tested is the one that ships.

Rejected: v1 feed files with another channel name in `withcoral/coral`'s releases. electron-updater's GitHub provider
reads whichever release is latest there, so the two apps' releases would steer each other. Rejected: a generic HTTPS
bucket. Coral has no bucket infrastructure, and GitHub drafts already give atomic promotion.

## Transition

v1 never takes over the 0.x identity (`com.withcoral.desktop`). It keeps `com.withcoral.app`, its data folder and
its updater cache (`coral-app-updater`). The deprecated app's last release, cut from `withcoral/coral` before v1 lands there,
tells its users to download v1 and stops checking for updates. After that, releases in `withcoral/coral` carry no
`latest*.yml`, so a deprecated install that still checks finds no update rather than v1.

## Consequences

- **v1 starts at 1.0.0.** The 0.x app's tags in `withcoral/coral` reach 0.15, so v1's 0.14 development versions would
  collide with them. release-please moves the runtime and Desktop together. Its first v1 release, or a beta,
  comes from a `Release-As: 1.0.0-beta.1` commit footer. The release workflow requires the tag, `Cargo.toml` and
  `apps/desktop/package.json` to agree.
- **Prereleases test the real path.** Release a version such as `1.0.0-beta.1`. The release workflow
  publishes a GitHub prerelease. Builds that are prereleases follow prereleases; stable builds never see them. The feed
  repository is public, so those assets are public but not linked from anywhere.
- **Every release ships every platform.** Betas could leave out Windows while its signing was not ready. That
  opt-out is gone: a release without a signed Windows installer fails.
- **Updates download in the background and install on restart or quit.** The release brief asked for user-initiated downloads. We
  changed that once the betas ran: a check runs at launch and every four hours and downloads what it finds. Installing
  waits for the person: Restart to Update, or their next quit. Desktop has no bridge into Reef, so a notification, a
  menu item and a Dock badge say an update is ready; a more visible indicator is an open decision.
- **The runtime gets JIT on macOS.** The release brief asked for JIT on Electron only, but v1's runtime runs Programs in V8. Under
  the hardened runtime, V8 needs `MAP_JIT` and aborts at startup without `allow-jit`, as `1.0.0-beta.2` did. Every
  binary therefore shares Electron's one entitlement, and xtask checks it on the app and the runtime.
- **Windows signature checks depend on the publisher name.** electron-builder writes the Azure Trusted Signing
  publisher into `app-update.yml`, and the updater installs only an installer signed by that publisher. Changing the
  signing certificate's subject strands installed apps on their version.
