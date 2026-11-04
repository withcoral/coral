import { createContext, useContext } from 'react'
import { toolOwner } from '@/lib/call-root'
import type { ProviderLogo } from '@/views/providers/types'
import { Logo, type LogoIcons } from '@/wax/components/logo'
import { ToolName } from '@/wax/components/tool-name'
import * as styles from './call-tool.css'

/** A provider of the workspace, as a call under its Call Root names its tool. */
export interface ProviderMark {
  callRoot: string
  displayName: string
  icons?: ProviderLogo
}

/** The workspace's providers. Without them, every call names its tool by its full Call Path. */
export const ProviderMarksContext = createContext<readonly ProviderMark[]>([])

/** The provider that owns `callPath`, and the tool path inside it. Null when no provider of
 * the workspace owns it now, as after the provider is removed. */
export function useToolOwner(callPath: string) {
  return toolOwner(callPath, useContext(ProviderMarksContext))
}

/**
 * A call's tool, named one way wherever Reef lists calls: its provider's logo, then the
 * tool's path inside that provider. A call whose provider is gone shows its full Call Path.
 * The full Call Path is on hover either way.
 */
export function CallTool({
  callPath,
  size,
  wrap,
}: {
  callPath: string
  size?: 'default' | 'small'
  /** Break a long name over lines instead of cutting it. */
  wrap?: boolean
}) {
  const owner = useToolOwner(callPath)
  return (
    <ToolName
      size={size}
      wrap={wrap}
      title={callPath}
      provider={
        owner ? { name: owner.provider.displayName, icons: owner.provider.icons } : undefined
      }
    >
      {owner?.relativePath ?? callPath}
    </ToolName>
  )
}

/** A provider as a panel's heading shows it: by its logo, named for a screen reader. */
export interface CallProvider {
  name: string
  icons?: LogoIcons
}

/** The provider of `callPath` as a heading shows it, while the workspace has that provider. */
export function useCallProvider(callPath: string): CallProvider | null {
  const owner = useToolOwner(callPath)
  return owner ? { name: owner.provider.displayName, icons: owner.provider.icons } : null
}

/** A call's title in a panel's heading: its provider's logo, then its full Call Path. */
export function CallTitle({ callPath, provider }: { callPath: string; provider: CallProvider }) {
  return (
    <span className={styles.title}>
      <span role="img" aria-label={provider.name} className={styles.logo}>
        <Logo name={provider.name} icons={provider.icons} size="20" />
      </span>
      {callPath}
    </span>
  )
}
