// Maps a Coral source name (or arbitrary key) to a provider icon URL under
// /images/providers/. Returns null when there's no matching asset and the
// caller should fall back to the generic Plug glyph.
//
// Two reasons a source might not have an entry here:
// 1. We don't have a logo asset yet — drop a file into ui/public/images/providers/
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
  google_calendar: '/images/providers/google.svg',
  google_contacts: '/images/providers/google.svg',
  google_drive: '/images/providers/google.svg',

  // Atlassian suite
  atlassian: '/images/providers/atlassian.svg',
  bitbucket: '/images/providers/atlassian.svg',
  confluence: '/images/providers/atlassian.svg',
  jira: '/images/providers/atlassian.svg',

  // Observability
  datadog: '/images/providers/datadog.svg',
  grafana: '/images/providers/grafana.svg',
  loki: '/images/providers/grafana.svg',
  honeycomb: '/images/providers/honeycomb.svg',
  new_relic: '/images/providers/new_relic.svg',
  opentelemetry: '/images/providers/opentelemetry.svg',
  otel_metrics: '/images/providers/opentelemetry.svg',
  sentry: '/images/providers/sentry.svg',
  statusgator: '/images/providers/statusgator.png',
  statuspage: '/images/providers/statuspage.svg',

  // Incident / paging
  pagerduty: '/images/providers/pagerduty.png',

  // Code hosts
  github: '/images/providers/github.svg',

  // Databases & data
  clickhouse: '/images/providers/Clickhouse.png',
  clickhouse_mcp: '/images/providers/Clickhouse.png',
  elastic: '/images/providers/elastic.png',
  elasticsearch: '/images/providers/elastic.png',

  // LLM providers
  anthropic: '/images/providers/anthropic.svg',
  openai: '/images/providers/openai.svg',
  xai: '/images/providers/xai.svg',

  // Project / product
  clickup: '/images/providers/clickup.svg',
  intercom: '/images/providers/intercom.svg',
  launchdarkly: '/images/providers/launchdarkly.svg',
  linear: '/images/providers/linear.svg',
  notion: '/images/providers/notion.svg',
  slack: '/images/providers/slack.png',
  stripe: '/images/providers/stripe.svg',
  wandb: '/images/providers/wandb.svg',
}

export function providerIcon(key: string): string | null {
  return PROVIDER_ICONS[key.toLowerCase()] ?? null
}
