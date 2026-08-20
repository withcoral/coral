export const ONBOARDING_SAMPLE_QUERY = `SELECT schema_name AS source, COUNT(*) AS tables
FROM coral.tables
GROUP BY schema_name
ORDER BY schema_name`

export interface OnboardingSampleQueryRow {
  source: string
  tables: bigint | number | string
}

export type OnboardingSampleQueryResult =
  | { rows: OnboardingSampleQueryRow[]; status: 'success' }
  | { message: string; status: 'error' }
