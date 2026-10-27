import type { SearchCoverage } from '../types'

export type CoverageTone = 'success' | 'neutral' | 'warning' | 'error'
/** One row of Search Coverage in Reef's words. */
export interface CoverageLine {
  subject: string
  label: string
  tone: CoverageTone
  detail: string | null
}

const subjects = { provider_tool: 'Tools', saved_function: 'Saved functions' }
/** What Coral counts as searched: a provider's tools, or one function. */
const units = { provider_tool: 'providers', saved_function: 'functions' }

/** A count says when more matched than Coral returned; only missing results keep a sentence. */
export function coverageLine(coverage: SearchCoverage): CoverageLine {
  const subject = subjects[coverage.kind]
  const unit = units[coverage.kind]
  switch (coverage.state) {
    case 'results_found':
      return {
        subject,
        label: `${coverage.returned}${coverage.hasMore ? '+' : ''} found`,
        tone: 'success',
        detail: null,
      }
    case 'empty':
      return { subject, label: 'none match', tone: 'neutral', detail: null }
    case 'skipped':
      return { subject, label: 'skipped by the provider filter', tone: 'neutral', detail: null }
    case 'partial':
      return {
        subject,
        label:
          coverage.failed > 0
            ? `${coverage.failed} of ${coverage.eligible} ${unit} failed`
            : 'some are not indexed yet',
        tone: 'warning',
        detail: 'Results may be missing.',
      }
    case 'error':
      return { subject, label: 'could not search', tone: 'error', detail: 'Results are missing.' }
  }
}

/** The tint of the whole strip: the worst state on the page. */
export function worstTone(coverage: SearchCoverage[]): 'neutral' | 'warning' | 'error' {
  const tones = coverage.map((row) => coverageLine(row).tone)
  if (tones.includes('error')) return 'error'
  return tones.includes('warning') ? 'warning' : 'neutral'
}
