---
status: accepted
---

# Desktop keeps serving in the background; the runtime is not an OS service

Agent clients store Coral's MCP URL and fail when Coral is not running. Closing Desktop's window therefore does not stop
Coral; Quit does, and a person can choose to start Coral at login without a window. Desktop keeps supervising the
runtime and Reef as it does with a window open. Background serving is not a setting: a person who wants Coral stopped
quits it.

Rejected: installing the runtime as an OS service (launchd, `systemd --user`, Task Scheduler) that Desktop attaches to.

- **MCP without Reef is half a product.** Approval Requests, Account Connection Requests and Run Status Page links
  resolve on Reef, which runs on Electron's Node (ADR 0012). A runtime-only service would give agents links that do not
  load.
- **Updates replace the package the service would run from.** An AppImage runs from a temporary mount, Windows cannot
  overwrite a running executable, and macOS swaps the whole app. Each would need a copy of the runtime outside the
  package and logic to replace a stale one.
- **Desktop would need a second way to run Coral.** Today the runtime never outlives Desktop, which writes its
  configuration and picks its port at every launch. An attached runtime reverses each of those and adds three OS
  backends.

## Consequences

- **Habitual gestures do not cut off agents.** On macOS, Command+Q closes the windows and leaves Coral serving, and a
  Coral with no window leaves the Dock and app switcher, because a person reads a Dock icon as an open app. Quit Coral in
  the menu bar item, Quit Completely, a logout or a shutdown stops Coral.
- **Desktop never restarts Coral by itself,** not even to install an update, because a restart interrupts agents
  mid-task.
- **The loopback listeners stay open for the whole login session** (ADR 0012). That lengthens the exposure and does not
  widen it.
- **Coral does not serve before login or without a desktop session.** Use the Docker image or the `coral` runtime there.
  Covering that case in Desktop first needs Reef to run outside Electron.
