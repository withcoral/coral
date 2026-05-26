# AGENTS.md — Coral UI Playwright Tests

This directory contains hermetic UI/integration tests for the Coral UI. Tests use
Playwright Test for browser automation and `@msw/playwright` + MSW handlers for
network mocking. They must not start the Rust Coral server.

## How the test system works

- `playwright.config.ts` starts only the Vite dev server on `127.0.0.1:5178`.
- `playwright.setup.ts` defines shared fixtures:
  - `network`: an `@msw/playwright` fixture backed by Playwright routing.
  - `review`: optional screencast/chapter helpers enabled by
    `PW_UI_SCREENCAST=1`.
- `support/grpc-web.ts` builds binary gRPC-Web responses. The UI uses
  Connect/gRPC-Web, so handlers must return framed protobuf responses, not JSON.
- `support/trace-handlers.ts` maps Coral TraceService RPC paths to fixture
  responses.
- `support/trace-fixtures.ts` holds fake-but-realistic source/query/span data.

The MSW setup is Playwright-side only. Do not add browser `setupWorker()` or
`public/mockServiceWorker.js`; production builds must not contain MSW/test code.

## How to run

From the repository root:

```sh
npm run test:ui --prefix ui
npm run test:ui:headed --prefix ui
npm run test:ui:debug --prefix ui
npm run test:ui:screencast --prefix ui
```

Screencast review mode writes `.webm` files under `ui/test-results/**`. These
artifacts are ignored by git.

## How to write tests

- Import from `./playwright.setup`, not directly from `@playwright/test`:

```ts
import { expect, test } from './playwright.setup'
```

- Register network behavior before navigation:

```ts
network.use(...traceHandlers.tenTraceDetailFlow)
await page.goto('/')
```

- Prefer user-visible locators (`getByRole`, `getByText`, placeholders) over CSS
  selectors. Use CSS only when the UI has no accessible hook yet.
- Keep fixtures realistic and domain-shaped. Prefer source names that match
  Coral bundled/configured source names such as `github`, `linear`, and `slack`.
- Cover both happy paths and unhappy paths. For every new UI flow, consider at
  least one success case and one failure/empty/error state, for example:
  - populated list and empty list,
  - successful detail load and TraceService error,
  - matching search and no-results search,
  - span with response body and span with missing/truncated body.
- Keep tests hermetic. Do not depend on local Coral config, real credentials,
  real APIs, or the Rust server.

## gRPC-Web handlers

Use `grpcWebResponse(schema, message)` for successful unary RPCs and
`grpcWebError(status, message)` for Connect/gRPC errors. Handlers should target
TraceService paths, for example:

```ts
http.post('*/coral.v1.TraceService/ListTraces', () =>
  grpcWebResponse(ListTracesResponseSchema, traceListResponse),
)
```

Unhandled `/coral.v1.*` requests fail the test. Static assets and Vite requests
are allowed to pass through. If a TraceService request is not matched, Vite may
try to proxy it to the default Coral server target and fail with `ECONNREFUSED`;
treat that as a missing MSW handler, not as a server-start problem.

## Review video and annotations

`npm run test:ui:screencast --prefix ui` enables the `review` fixture. Use it to
make recordings understandable:

```ts
await review.chapter('Open query details', 'Load the selected trace with mocked spans')
await page.getByText(/linear\.issues/).click()
await review.pause()
```

Guidelines:

- Add chapters before important user-visible transitions.
- Use concise chapter titles; explain intent in the optional description.
- Use `review.pause()` after major assertions or visual states so the recording
  is readable.
- Do not make normal tests depend on review mode; `review.chapter()` and
  `review.pause()` are no-ops unless `PW_UI_SCREENCAST=1`.

Default screencast settings favor review quality over speed: 1440x900
frames, quality 100, action overlays, and sequential workers. Adjust with:

```sh
PW_UI_REVIEW_PAUSE_MS=1800 npm run test:ui:screencast --prefix ui
```
