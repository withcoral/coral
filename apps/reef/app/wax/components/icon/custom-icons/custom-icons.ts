import { ArchiveIcon } from './archive'
import { ArchiveRestoreIcon } from './archive-restore'
import { ArrowDownIcon } from './arrow-down'
import { ArrowRightIcon } from './arrow-right'
import { ArrowUpIcon } from './arrow-up'
import { ArrowUpRightIcon } from './arrow-up-right'
import { AtSignIcon } from './at-sign'
import { BanIcon } from './ban'
import { BellIcon } from './bell'
import { BellRingIcon } from './bell-ring'
import { BookOpenIcon } from './book-open'
import { BotIcon } from './bot'
import { BrainIcon } from './brain'
import { Building2Icon } from './building-2'
import { CalendarIcon } from './calendar'
import { ChartIcon } from './chart'
import { ChartColumnIncreasingIcon } from './chart-column-increasing'
import { CheckIcon } from './check'
import { ChevronDownIcon } from './chevron-down'
import { ChevronLeftIcon } from './chevron-left'
import { ChevronRightIcon } from './chevron-right'
import { ChevronUpIcon } from './chevron-up'
import { ChevronsDownUpIcon } from './chevrons-down-up'
import { ChevronsUpDownIcon } from './chevrons-up-down'
import { CircleAlertIcon } from './circle-alert'
import { CircleAlertFullIcon } from './circle-alert-full'
import { CircleCheckIcon } from './circle-check'
import { CircleCheckFullIcon } from './circle-check-full'
import { CircleDashedIcon } from './circle-dashed'
import { CircleDotIcon } from './circle-dot'
import { CirclePlusIcon } from './circle-plus'
import { CircleQuestionMarkIcon } from './circle-question-mark'
import { CircleStopIcon } from './circle-stop'
import { CircleXIcon } from './circle-x'
import { ClockCheckIcon } from './clock-check'
import { ClockSearchIcon } from './clock-search'
import { CopyIcon } from './copy'
import { CoralIcon } from './coral'
import { CornerDownLeftIcon } from './corner-down-left'
import { CpuIcon } from './cpu'
import { CreditCardIcon } from './credit-card'
import { DocIcon } from './doc'
import { DownloadIcon } from './download'
import { EllipsisIcon } from './ellipsis'
import { ExternalLinkIcon } from './external-link'
import { EyeIcon } from './eye'
import { EyeOffIcon } from './eye-off'
import { FileCodeIcon } from './file-code'
import { FileOutputIcon } from './file-output'
import { FlagIcon } from './flag'
import { FolderIcon } from './folder'
import { FoldersIcon } from './folders'
import { GitHubIcon } from './github'
import { GlobeIcon } from './globe'
import { HandIcon } from './hand'
import { HomeIcon } from './home'
import { InfoIcon } from './info'
import { LightbulbIcon } from './lightbulb'
import { LinkIcon } from './link'
import { ListFilterIcon } from './list-filter'
import { LockIcon } from './lock'
import { LogOutIcon } from './log-out'
import { Maximize2Icon } from './maximize-2'
import { MemoryStickIcon } from './memory-stick'
import { MessageCircleIcon } from './message-circle'
import { MinusIcon } from './minus'
import { NotepadTextIcon } from './notepad-text'
import { PanelLeftIcon } from './panel-left'
import { PanelsTopLeftIcon } from './panels-top-left'
import { PencilIcon } from './pencil'
import { PlayIcon } from './play'
import { PlugIcon } from './plug'
import { PlusIcon } from './plus'
import { RefreshCcwIcon } from './refresh-ccw'
import { RefreshCwIcon } from './refresh-cw'
import { SaveIcon } from './save'
import { ScalingIcon } from './scaling'
import { SearchIcon } from './search'
import { SettingsIcon } from './settings'
import { Settings2Icon } from './settings-2'
import { SeverityCriticalIcon } from './severity-critical'
import { SeverityLowIcon } from './severity-low'
import { SeverityModerateIcon } from './severity-moderate'
import { SeveritySevereIcon } from './severity-severe'
import { ShieldBanIcon } from './shield-ban'
import { SlackIcon } from './slack'
import { SparklesIcon } from './sparkles'
import { SquareFunctionIcon } from './square-function'
import { SunMoonIcon } from './sun-moon'
import { ThumbsDownIcon } from './thumbs-down'
import { ThumbsMiddleIcon } from './thumbs-middle'
import { ThumbsUpIcon } from './thumbs-up'
import { TimerIcon } from './timer'
import { TrashIcon } from './trash'
import { TriangleAlertIcon } from './triangle-alert'
import { TriangleAlertFullIcon } from './triangle-alert-full'
import { UploadIcon } from './upload'
import { UserRoundXIcon } from './user-round-x'
import { UsersIcon } from './users'
import { WaypointsIcon } from './waypoints'
import { WrenchIcon } from './wrench'
import { XIcon } from './x'
import { ZapIcon } from './zap'
import type { IconName } from '../icon'

/**
 * How to add a custom icon:
 * - Go to Figma, copy the 24px icon. Lucide icon is based on 24px, so it MUST be that size.
 * - If it's not, Claude Code will be able to fix it later.
 * - Paste it into https://studio.lucide.dev/, click "Share > Copy React Code" from the topbar.
 * - Create new file, paste that, fix it up.
 * - Add it to the Storybook story, so you can check its alignment (should be OK if Figma had 24px icon)
 * - If misaligned, you can ask Claude to fix it.
 */
export const customIcons = {
  Archive: ArchiveIcon,
  ArchiveRestore: ArchiveRestoreIcon,
  ArrowDown: ArrowDownIcon,
  ArrowRight: ArrowRightIcon,
  ArrowUp: ArrowUpIcon,
  ArrowUpRight: ArrowUpRightIcon,
  AtSign: AtSignIcon,
  Ban: BanIcon,
  Bell: BellIcon,
  BellRing: BellRingIcon,
  BookOpen: BookOpenIcon,
  Bot: BotIcon,
  Brain: BrainIcon,
  Building2: Building2Icon,
  Calendar: CalendarIcon,
  Chart: ChartIcon,
  ChartColumnIncreasing: ChartColumnIncreasingIcon,
  Check: CheckIcon,
  ChevronDown: ChevronDownIcon,
  ChevronLeft: ChevronLeftIcon,
  ChevronRight: ChevronRightIcon,
  ChevronsDownUp: ChevronsDownUpIcon,
  ChevronsUpDown: ChevronsUpDownIcon,
  ChevronUp: ChevronUpIcon,
  CircleAlert: CircleAlertIcon,
  CircleAlertFull: CircleAlertFullIcon,
  CircleCheck: CircleCheckIcon,
  CircleCheckFull: CircleCheckFullIcon,
  CircleDashed: CircleDashedIcon,
  CircleDot: CircleDotIcon,
  CirclePlus: CirclePlusIcon,
  CircleQuestionMark: CircleQuestionMarkIcon,
  CircleStop: CircleStopIcon,
  CircleX: CircleXIcon,
  ClockCheck: ClockCheckIcon,
  ClockSearch: ClockSearchIcon,
  Copy: CopyIcon,
  Coral: CoralIcon,
  CornerDownLeft: CornerDownLeftIcon,
  Cpu: CpuIcon,
  CreditCard: CreditCardIcon,
  Doc: DocIcon,
  Download: DownloadIcon,
  Ellipsis: EllipsisIcon,
  ExternalLink: ExternalLinkIcon,
  Eye: EyeIcon,
  EyeOff: EyeOffIcon,
  FileCode: FileCodeIcon,
  FileOutput: FileOutputIcon,
  Flag: FlagIcon,
  Folder: FolderIcon,
  Folders: FoldersIcon,
  GitHub: GitHubIcon,
  Globe: GlobeIcon,
  Hand: HandIcon,
  Home: HomeIcon,
  Info: InfoIcon,
  Lightbulb: LightbulbIcon,
  Link: LinkIcon,
  ListFilter: ListFilterIcon,
  Lock: LockIcon,
  LogOut: LogOutIcon,
  Maximize2: Maximize2Icon,
  MemoryStick: MemoryStickIcon,
  MessageCircle: MessageCircleIcon,
  Minus: MinusIcon,
  NotepadText: NotepadTextIcon,
  PanelLeft: PanelLeftIcon,
  PanelsTopLeft: PanelsTopLeftIcon,
  Pencil: PencilIcon,
  Play: PlayIcon,
  Plug: PlugIcon,
  Plus: PlusIcon,
  RefreshCcw: RefreshCcwIcon,
  RefreshCw: RefreshCwIcon,
  Save: SaveIcon,
  Scaling: ScalingIcon,
  Search: SearchIcon,
  Settings: SettingsIcon,
  Settings2: Settings2Icon,
  SeverityCritical: SeverityCriticalIcon,
  SeverityLow: SeverityLowIcon,
  SeverityModerate: SeverityModerateIcon,
  SeveritySevere: SeveritySevereIcon,
  ShieldBan: ShieldBanIcon,
  Slack: SlackIcon,
  Sparkles: SparklesIcon,
  SquareFunction: SquareFunctionIcon,
  SunMoon: SunMoonIcon,
  ThumbsDown: ThumbsDownIcon,
  ThumbsMiddle: ThumbsMiddleIcon,
  ThumbsUp: ThumbsUpIcon,
  Timer: TimerIcon,
  Trash: TrashIcon,
  TriangleAlert: TriangleAlertIcon,
  TriangleAlertFull: TriangleAlertFullIcon,
  Upload: UploadIcon,
  UserRoundX: UserRoundXIcon,
  Users: UsersIcon,
  Waypoints: WaypointsIcon,
  Wrench: WrenchIcon,
  X: XIcon,
  Zap: ZapIcon,
} as const

export type CustomIconName = keyof typeof customIcons

// Assuming we won't have name clashes. Can revisit in the future if that'll be the case
// E.g. if we'll need our own custom "Menu" icon, which clashes with the existing Lucide "Menu" icon
export function isCustomIcon(name: IconName): name is CustomIconName {
  return name in customIcons
}
