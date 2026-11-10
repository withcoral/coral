import type { ProviderLogo } from './types'
import coralMarkDark from './coral-mark-dark.svg'
import coralMarkLight from './coral-mark-light.svg'
import privateMarkDark from './private-mark-dark.svg'
import privateMarkLight from './private-mark-light.svg'

/** Coral's mark, which stands for the Built-in Provider on the Tools page and beside
 * the calls Activity and Approvals name. */
export const CORAL_ICONS: ProviderLogo = { light: coralMarkLight, dark: coralMarkDark }

/** The lock that stands for the private tools on the Tools page. */
export const PRIVATE_ICONS: ProviderLogo = { light: privateMarkLight, dark: privateMarkDark }
