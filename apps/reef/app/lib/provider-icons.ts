// Maps a Coral source name (or arbitrary key) to a provider icon URL under
// /images/providers/. Returns null when there's no matching asset and the
// caller should fall back to the generic Plug glyph.
//
// Two reasons a source might not have an entry here:
// 1. We don't have a logo asset yet — drop a file into reef/public/images/providers/
//    and add a line below.
// 2. The source is genuinely generic (e.g. hn, osv). The fallback glyph is fine.
//
// Longer term this mapping should move into the manifest (`icon:` field on the
// source spec) so source authors own their own iconography rather than the UI
// maintaining a curated list.

const PROVIDER_ICONS: Record<string, string> = {
  // Cloud platforms
  aws: '/images/providers/aws.svg',
  cloudwatch_logs: '/images/providers/aws.svg',
  cloudwatch_metrics: '/images/providers/aws.svg',
  gcp: '/images/providers/gcp.svg',
  google: '/images/providers/google.svg',
  gmail: '/images/providers/gmail.svg',
  google_calendar: '/images/providers/google_calendar.svg',
  google_contacts: '/images/providers/google.svg',
  google_drive: '/images/providers/google.svg',
  cloudflare: '/images/providers/cloudflare.png',
  digitalocean: '/images/providers/digitalocean.svg',
  vercel: '/images/providers/vercel.png',

  // Atlassian suite
  atlassian: '/images/providers/atlassian.svg',
  bitbucket: '/images/providers/atlassian.svg',
  confluence: '/images/providers/confluence.svg',
  jira: '/images/providers/jira.svg',

  // Observability
  axiom: '/images/providers/axiom.png',
  datadog: '/images/providers/datadog.svg',
  grafana: '/images/providers/grafana.svg',
  loki: '/images/providers/grafana.svg',
  honeycomb: '/images/providers/honeycomb.svg',
  new_relic: '/images/providers/new_relic.svg',
  opentelemetry: '/images/providers/opentelemetry.svg',
  openobserve: '/images/providers/openobserve.svg',
  otel_metrics: '/images/providers/opentelemetry.svg',
  sentry: '/images/providers/sentry.svg',
  statusgator: '/images/providers/statusgator.png',
  statuspage: '/images/providers/statuspage.svg',

  // Incident / paging
  incident_io: '/images/providers/incident_io.svg',
  pagerduty: '/images/providers/pagerduty.svg',

  // Code hosts
  github: '/images/providers/github.svg',
  gitlab: '/images/providers/gitlab.svg',

  // Databases & data
  clickhouse: '/images/providers/Clickhouse.png',
  clickhouse_mcp: '/images/providers/Clickhouse.png',
  elastic: '/images/providers/elastic.png',
  elasticsearch: '/images/providers/elastic.png',
  neon: '/images/providers/neon.svg',

  // Agent tooling — retrieval, crawling, docs and browser automation
  browserbase: '/images/providers/browserbase.svg',
  context7: '/images/providers/context7.png',
  deepwiki: '/images/providers/deepwiki.png',
  exa: '/images/providers/exa.png',
  firecrawl: '/images/providers/firecrawl.png',

  // Communications
  resend: '/images/providers/resend.png',
  twilio: '/images/providers/twilio.png',

  // LLM providers
  claude: '/images/providers/claude_code.svg',
  codex: '/images/providers/openai.svg',
  openai: '/images/providers/openai.svg',
  xai: '/images/providers/xai.svg',

  // Project / product
  asana: '/images/providers/asana.png',
  clickup: '/images/providers/clickup.svg',
  figma: '/images/providers/figma.svg',
  intercom: '/images/providers/intercom.svg',
  launchdarkly: '/images/providers/launchdarkly.svg',
  linear: '/images/providers/linear.svg',
  notion: '/images/providers/notion.svg',
  posthog: '/images/providers/posthog.svg',
  slack: '/images/providers/slack.svg',
  spotify: '/images/providers/spotify.png',
  stripe: '/images/providers/stripe.svg',
  val_town: '/images/providers/val_town.svg',
  wandb: '/images/providers/wandb.svg',
}

// These icons need to be inverted to look nice in dark mode. Usually means they are all black (e.g. GitHub), so
// inverting them makes them white.
// Leave self-inverting assets out of this set: digitalocean.svg and neon.svg carry
// their own `prefers-color-scheme` rule, so inverting them again undoes their work.
const DARK_MODE_INVERT_PROVIDER_ICONS = new Set(['codex', 'github', 'notion', 'openai', 'val_town'])

export function providerIcon(key: string): string | null {
  return PROVIDER_ICONS[key.toLowerCase()] ?? null
}

export function providerIconNeedsDarkInvert(key: string): boolean {
  return DARK_MODE_INVERT_PROVIDER_ICONS.has(key.toLowerCase())
}
