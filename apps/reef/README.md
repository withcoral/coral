# Reef

Reef is the Coral frontend shell.

## Local Development

Install dependencies:

```bash
npm install
```

Start the React Router development server:

```bash
npm run dev
```

Open `http://localhost:5173` unless the dev server prints a different URL.

Authentication is disabled by default for local development and for Coral
Desktop. No auth environment variables are needed for those modes.

Hosted Reef must choose its auth behavior explicitly. Set
`REEF_AUTH_MODE=required` together with the server-only Coral authorization
issuer, Reef public URL, and session values documented in `.env.example`. Reef
derives its OAuth resource, client metadata URL, callback URL, and cookie
security from `REEF_PUBLIC_URL`. A non-desktop production server without an
explicit mode fails closed rather than accidentally publishing an
unauthenticated app.

Run checks:

```bash
npm run format:check
npm run lint -- --deny-warnings
npm run typecheck
npm test
npm run build
```

## Storybook

Run the Wax component library locally:

```bash
npm run storybook
```
