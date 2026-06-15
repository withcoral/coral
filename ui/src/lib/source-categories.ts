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
]

const SOURCE_CATEGORY: Record<string, string> = {
  clickup: 'project-management',
  claude: 'ai-ml',
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
  notion: 'knowledge',
  openobserve: 'observability',
  pagerduty: 'incident-management',
  posthog: 'analytics',
  sentry: 'observability',
  slack: 'communication',
  statusgator: 'observability',
  stripe: 'business',
  wandb: 'ai-ml',
}

export function getCategoryForSource(source: string): string {
  return SOURCE_CATEGORY[source] ?? 'other'
}
