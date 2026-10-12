---
status: accepted
---

# Desktop hosts Reef as a loopback server

Desktop runs Reef's bundled `server.mjs` in an Electron utility process, listening on `127.0.0.1` with auth disabled,
and its window loads that URL. Reef then runs as it does in Docker and in local mode: the same bundled server entry, the
same request path and the same Origin check. Every deployment ships the same Reef build, with no Desktop marker. The
window has no preload and no bridge. Desktop is a supervisor and holds no product logic.

Rejected: keep the earlier `coral-app://` scheme, which rendered Reef in the Electron main process. Its scheme handler
rewrote CSP, set `process.env` on every request and passed `x-coral-desktop-*` headers to Reef. Reef exempted POSTs
without an Origin header from the Origin check. Desktop copied Reef's production dependencies by hand. Every one of
those was a Desktop-only request path or packaging step that the other deployments never tested.

## Consequences

- **Any local process can reach Reef.** With auth disabled, Reef serves the implicit local User, the Organization
  Owner, to every loopback caller, as local mode does. React Router derives the request origin from `Host`, so a
  DNS-rebinding page would pass the Origin check. Reef therefore answers 421 with no body unless `Host` names its own
  loopback listener (`<address>:<port>` or `localhost:<port>`) and the target is origin-form. Do not remove this check
  while auth can be disabled.
- **The Run-navigation bridge is gone on purpose.** The earlier Desktop ran a separate loopback server that turned
  Run Status Page links from agents into navigation inside its window. The runtime now builds those links on Reef's own
  URL (`CORAL_LOCAL_WEB_URL`), so they open in the system browser against the same Reef. Desktop keeps Reef's port
  across launches, so the links stay valid. Do not reintroduce a bridge to bring links into the window.
- **Provider consent leaves the window.** Chromium applies `form-action` to redirects, so Desktop hands a consent
  redirect marked under [ADR 0009](0009-provider-consent-navigation.md) to the system browser and keeps the window on
  Reef. A device sign-in form targets a new tab, as it does in a browser. Desktop submits that form in a hidden
  window, so its consent redirect reaches the same hand-off.
