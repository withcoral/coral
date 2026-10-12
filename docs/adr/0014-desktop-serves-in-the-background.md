---
status: accepted
---

# Desktop keeps serving in the background; the runtime is not an OS service

Agent clients store Coral's MCP URL and fail when Desktop is not running. Coral therefore keeps serving after the
window closes, and a person can choose to start it at login without a window. Desktop itself stays running and keeps
supervising the runtime and Reef, as it does with a window open. Closing the window does not stop Coral; Quit does.
Keeping Coral running is not a setting: a person who wants Coral stopped quits it. Desktop says so the first time the
last window closes. Start at login is off until the person turns it on.

Rejected: install the runtime as an OS service (launchd, `systemd --user`, Task Scheduler) that Desktop attaches to.
That fits a single binary that serves MCP and its own web interface. Coral's differs:

- **MCP without Reef is half a product.** Approval Requests, Account Connection Requests and Run Status Page links all
  resolve on Reef, and Reef runs on Electron's Node ([ADR 0012](0012-desktop-hosts-reef-as-a-loopback-server.md)). A
  runtime-only service would give agents links that do not load.
- **The service binary would live inside the package that updates replace.** An AppImage runs from a temporary mount,
  the Windows installer cannot overwrite a running executable and macOS swaps the whole app. Each needs a copy of the
  runtime outside the package, and logic to detect and replace a stale one.
- **Desktop would need a second way to run Coral.** Today the runtime never outlives Desktop, Desktop writes its
  configuration at every launch and picks the gRPC port. An attached runtime that Desktop does not own reverses each
  of those, and adds three OS backends to maintain.

## Consequences

- **The settings are native.** Desktop has no bridge into Reef, so a tray or menu bar item and the application menu
  carry Open Coral, the Start at login setting and Quit. The operating system keeps the login item, and Desktop reads
  it from there, so the checkbox shows what the system will do.
- **Desktop tells, and does not ask.** A question at the first close made the person decide before they knew what
  the choice meant. A notice says that Coral still runs and where to quit it.
- **No setting makes the window's close stop Coral.** 1.0.0-beta.8 had one. It only made closing the window, a habit,
  cut off the person's agents, and each platform already has a Quit that stops Coral.
- **On macOS, Command+Q does not stop Coral.** Quit in the Coral menu only closes the windows, so a habitual Command+Q
  does not cut off the person's agents. Quit in the Dock and Q in the app switcher do the same: a person uses them as
  they use Command+Q. macOS sends those and a logout or shutdown as the same quit request, so Desktop listens for the
  system's power-off notice and lets that one stop Coral. Quit Coral in the menu bar item stops Coral, and so does
  Quit Completely in the Coral menu and the Dock menu. On Windows and Linux, Quit in the File menu stops Coral: stock
  GNOME shows no tray icon, so that menu must keep a way to quit.
- **On macOS, a Coral with no window leaves the Dock and the app switcher.** A person reads a Dock icon as an open
  app. The menu bar item remains, as it does for other menu bar apps. The Dock
  icon returns with the window.
- **A waiting update needs a signal that works with no window.** A background Desktop rarely quits, so a downloaded
  update can wait for days ([ADR 0013](0013-desktop-release-channel.md)). Desktop still installs only on Restart to
  Update or on quit, and never restarts Coral by itself: a restart interrupts agents that are mid-task. The tray or
  menu bar item shows that an update waits and offers the restart.
- **Linux differs.** Electron's login item API does not cover Linux, so Desktop writes an XDG autostart entry that
  names the AppImage. Stock GNOME shows no tray icon; launching Coral again opens the window.
- **Nothing restarts Desktop when its main process crashes.** The runtime stops with it. An OS agent that launches
  Desktop without a window could add that later and would not change this decision.
- **The loopback listeners stay open for the whole login session.** They accept any local process as the local User
  (ADR 0012). Background serving lengthens that exposure and does not widen it.
- **Coral does not serve before login or on a machine with no desktop session.** Use the Docker image or a local
  `coral` runtime there. If Desktop must cover that case, first make Reef run outside Electron.
