---
status: accepted
---

# Desktop hosts Reef as a loopback server

Desktop runs Reef's bundled server in an Electron utility process on `127.0.0.1` with auth disabled, and its window
loads that URL. Reef runs exactly as it does in Docker and local mode, with no Desktop marker, preload or bridge.
Desktop is a supervisor and holds no product logic.

Rejected: keeping the earlier `coral-app://` scheme, which rendered Reef in the Electron main process. It rewrote CSP,
set `process.env` per request, passed Desktop-only headers, exempted Origin-less POSTs from the Origin check and copied
Reef's dependencies by hand. Each was a request path or packaging step that no other deployment tested.

## Consequences

- **Any local process can reach Reef** as the local Organization Owner, as in local mode. React Router derives the
  origin from `Host`, so a DNS-rebinding page would pass the Origin check. Reef therefore answers 421 unless `Host` names
  its own loopback listener. Do not remove that check while auth can be disabled.
- **Links open in the system browser.** Run Status Page links are built on Reef's own URL, and Desktop keeps Reef's
  port across launches so they stay valid. Do not reintroduce a bridge that pulls links into the window.
- **Provider consent leaves the window** for the system browser, because Chromium applies `form-action` to redirects.
