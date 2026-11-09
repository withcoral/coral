/** What stops this process from sending, whatever the setting says. */
export type SendBlock =
  | { kind: 'environment'; variable: 'DO_NOT_TRACK' | 'CORAL_TELEMETRY' | 'CI' }
  | { kind: 'build' }

export interface UsageDataView {
  /** The deployment's one setting. It is on until an organization owner turns it off. */
  choice: UsageChoice
  /** Only organization owners can change the setting. */
  canChange: boolean
  block?: SendBlock
  /** Desktop sends from the user's computer; a self-hosted server from itself. */
  kind: 'desktop' | 'server'
  /** The next report, exactly as Coral sends it: compact JSON, byte for byte. */
  next: string
  /** `at` is an RFC 3339 UTC time. `json` is the report exactly as Coral sent it. */
  lastSent?: { at: string; json: string }
}

export type UsageChoice = 'on' | 'off'
