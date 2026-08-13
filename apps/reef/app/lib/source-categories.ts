export interface SourceCategory {
  key: string
  label: string
}

export const SOURCE_CATEGORY_ORDER: SourceCategory[] = [
  { key: 'observability', label: 'Observability' },
  { key: 'incident-management', label: 'Incident Management' },
  { key: 'developer-tools', label: 'Developer Tools' },
  { key: 'communication', label: 'Communication' },
  { key: 'project-management', label: 'Project Management' },
  { key: 'knowledge', label: 'Knowledge & Docs' },
  { key: 'analytics', label: 'Analytics' },
  { key: 'business', label: 'Business' },
  { key: 'ai-ml', label: 'AI/ML' },
  // Last because it is the least central to the product. Empty categories are
  // dropped in groupSourceCatalogSections, so this only shows once a media source
  // is in the catalog.
  { key: 'media', label: 'Media' },
]

const SOURCE_CATEGORY: Record<string, string> = {
  asana: 'project-management',
  axiom: 'observability',
  browserbase: 'ai-ml',
  clickup: 'project-management',
  claude: 'ai-ml',
  cloudflare: 'developer-tools',
  context7: 'ai-ml',
  deepwiki: 'ai-ml',
  digitalocean: 'developer-tools',
  exa: 'ai-ml',
  figma: 'developer-tools',
  firecrawl: 'ai-ml',
  cloudwatch_logs: 'observability',
  cloudwatch_metrics: 'observability',
  codex: 'ai-ml',
  confluence: 'knowledge',
  datadog: 'observability',
  github: 'developer-tools',
  gitlab: 'developer-tools',
  google_calendar: 'communication',
  grafana: 'observability',
  incident_io: 'incident-management',
  intercom: 'communication',
  jira: 'project-management',
  launchdarkly: 'developer-tools',
  linear: 'project-management',
  neon: 'developer-tools',
  notion: 'knowledge',
  openai: 'ai-ml',
  openobserve: 'observability',
  pagerduty: 'incident-management',
  posthog: 'analytics',
  resend: 'communication',
  sentry: 'observability',
  slack: 'communication',
  spotify: 'media',
  statusgator: 'observability',
  stripe: 'business',
  val_town: 'developer-tools',
  vercel: 'developer-tools',
  wandb: 'ai-ml',
}

export function getCategoryForSource(source: string): string {
  return SOURCE_CATEGORY[source] ?? 'other'
}
