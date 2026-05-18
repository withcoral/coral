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
npm run test:ui:screencast:changed --prefix ui
```

Screencast review mode writes `.webm` files under `ui/test-results/**`. These
artifacts are ignored by git. CI converts those recordings to `.mp4` and uploads
both MP4 and original WebM files in the `ui-screencasts-<pr-number>` workflow
artifact. CI also uploads one primary MP4 as an unzipped artifact using
`archive: false`, because GitHub only supports unzipped upload for a single file.
The CI job creates or updates a PR comment with the unzipped primary MP4 artifact
URL, the full bundle URL, the workflow run artifacts link, and one-liner `gh run
download` commands to open the primary MP4 or full bundle locally. Screencast
jobs are advisory: they do not gate aggregate `validate`, and the comment job
only runs when recording/upload succeeds.

The changed-test command compares the branch to the PR base/main, records changed
`*.spec.ts` files directly, and falls back to all UI specs when shared UI code,
Playwright support files, package/config files, or Coral protobuf contracts
changed. If no UI app/test files changed, it records nothing and writes only a
selection manifest.

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

`npm run test:ui:screencast --prefix ui` enables the `review` fixture.
`npm run test:ui:screencast:ci --prefix ui` runs the same review fixture in the
headless CI selector path. Use review annotations to make recordings
understandable:

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

Default screencast settings favor review quality over speed: fixed 1440x900
frames, quality 100, action overlays, and sequential workers. Adjust pauses with:

```sh
PW_UI_REVIEW_PAUSE_MS=1800 npm run test:ui:screencast --prefix ui
```


## CI screencast selector

`tests/ui/run-changed-screencasts.mjs` owns the simple selector used by CI. Keep
it conservative and easy to read:

- changed `ui/tests/ui/**/*.spec.ts` files run directly;
- shared test support, Playwright config, package files, Vite config, UI source,
  and Coral protobuf changes run all UI Playwright specs;
- non-UI changes skip recording.

Update this selector only when a new UI test area has an obvious, stable mapping
from source files to specs. If the mapping is not obvious, run all UI specs rather
than maintaining a fragile dependency graph. To test the selector locally without
editing files, pass explicit changed files:

```sh
PW_UI_CHANGED_FILES='ui/tests/ui/traces.spec.ts' npm run test:ui:screencast:ci --prefix ui
```

To mirror CI's MP4 conversion locally after recording:

```sh
find ui/test-results -name '*.webm' -print0 |
  while IFS= read -r -d '' file; do
    ffmpeg -nostdin -y -i "$file" \
      -c:v libx264 -pix_fmt yuv420p -movflags +faststart \
      "${file%.webm}.mp4"
  done
```
