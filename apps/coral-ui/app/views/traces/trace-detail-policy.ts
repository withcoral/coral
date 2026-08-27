import { TraceStatus } from '@/generated/coral/v1/traces_pb'

import { isSearchOperation, type TraceSummaryData } from './trace-utils'

export interface PrimaryDetailTab {
  id: 'results' | 'timeline'
  label: string
}

export interface TraceDetailPolicy {
  defaultTab: 'results' | 'timeline'
  primaryTabs: PrimaryDetailTab[]
}

export function traceDetailPolicy(
  summary: TraceSummaryData,
  resultsLabel: string,
  showSearchTrace: boolean,
): TraceDetailPolicy {
  if (isSearchOperation(summary)) {
    return {
      defaultTab: showSearchTrace && summary.status === TraceStatus.ERROR ? 'timeline' : 'results',
      primaryTabs: [
        { id: 'results', label: resultsLabel },
        ...(showSearchTrace ? [{ id: 'timeline' as const, label: 'Trace' }] : []),
      ],
    }
  }

  return {
    defaultTab: 'timeline',
    primaryTabs: [{ id: 'timeline', label: 'Trace' }],
  }
}

export function timelineShortcutsEnabled(summary: TraceSummaryData, activeTab: string): boolean {
  return !isSearchOperation(summary) || activeTab === 'timeline'
}
