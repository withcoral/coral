import { createThemeContract } from '@/wax/css'

/**
 * Static tokens are not affected by the theme, they are always the same.
 */
export const staticUtilities = {
  black: '#000000',
  white: '#FFFFFF',
}

/**
 * Animation utilities for consistent transitions across components.
 */
export const animation = {
  colorTransition: '0.2s ease background-color, 0.2s ease color, 0.2s ease box-shadow',
  // An element that swaps in for another fades on the same beat as a colour change.
  fadeTransition: '0.2s ease opacity',
  // Panel reveal (transitions.dev) without its fade: panels move on this ease
  // and cross-blur from `blur` to sharp.
  panel: {
    blur: 'blur(2px)',
    close: '350ms',
    ease: 'cubic-bezier(0.22, 1, 0.36, 1)',
    open: '400ms',
  },
}

const pillColorContract = {
  background: '',
  backgroundHover: '',
  color: '',
  colorHover: '',
  stroke: '',
}

const chartColorContract = {
  fill: '',
}

const typographyContract = {
  fontFamily: '',
  fontSize: '',
  fontWeight: '',
  letterSpacing: '',
  lineHeight: '',
}

const avatarFallbackColorContract = {
  background: '',
  color: '',
}

export const theme = createThemeContract({
  avatarFallback: {
    amber: avatarFallbackColorContract,
    blue: avatarFallbackColorContract,
    green: avatarFallbackColorContract,
    purple: avatarFallbackColorContract,
    red: avatarFallbackColorContract,
  },
  button: {
    bare: {
      hover: '',
    },
    destructive: {
      default: '',
      focus: '',
      hover: '',
    },
    primary: {
      default: '',
      disabled: '',
      focus: '',
      hover: '',
    },
    secondary: {
      default: '',
      focus: '',
      hover: '',
    },
  },
  chart: {
    amber: chartColorContract,
    blue: chartColorContract,
    green: chartColorContract,
    neutral: chartColorContract,
    orange: chartColorContract,
    purple: chartColorContract,
    red: chartColorContract,
  },
  content: {
    accentContent: {
      iconFill: '',
      primary: '',
      primaryReverse: '',
      secondary: '',
      tertiary: '',
    },
    code: {
      inlineBackground: '',
      inlineColor: '',
      syntax: {
        comment: '',
        constant: '',
        function: '',
        keyword: '',
        string: '',
        type: '',
      },
    },
    disabled: '',
    error: '',
    errorBackground: '',
    info: '',
    link: '',
    linkHover: '',
    placeholder: '',
    primary: '',
    secondary: '',
    selection: '',
    success: '',
    successBackground: '',
    tertiary: '',
    /** Readable text that recedes, such as navigation beside the page it frames. */
    muted: '',
    warning: '',
    warningBackground: '',
  },
  elevation: {
    e1: '',
    e2: '',
    e3: '',
    e4: '',
  },
  gradient: {
    card: '',
  },
  help: {
    componentBackground: '',
  },
  input: {
    stroke: {
      default: '',
      disabled: '',
      error: '',
      focus: '',
      hover: '',
    },
  },
  // The round plate behind a third-party logo, one step above the surface.
  logoPlate: { background: '', stroke: '' },
  pill: {
    amber: pillColorContract,
    blue: pillColorContract,
    gray: pillColorContract,
    graySubtle: pillColorContract,
    green: pillColorContract,
    mention: pillColorContract,
    orange: pillColorContract,
    purple: pillColorContract,
    red: pillColorContract,
  },
  sidebar: {
    badge: { background: '', content: '' },
    button: {
      hover: '',
      selected: '',
    },
    buttonAccent: {
      default: '',
      hover: '',
      selected: '',
    },
  },
  stroke: {
    /** A control's own outline, such as a checkbox's: at least 3:1 on the page (WCAG 1.4.11). */
    control: '',
    floating: '',
    focused: '',
    mainContent: '',
    primary: '',
    secondary: '',
  },
  surface: {
    backdrop: '',
    card: '',
    cardHover: '',
    floating: '',
    main: '',
    mainContent: '',
    onMainContent: '',
    onMainContentHover: '',
    onMainContentSubtle: '',
    skeleton: '',
  },
  segmentedControl: {
    checked: '',
    checkedStroke: '',
  },
  switch: {
    default: '',
    hover: '',
    hoverOn: '',
    on: '',
  },
  typography: {
    body: typographyContract,
    bodyLarge: typographyContract,
    bodyLargeStrong: typographyContract,
    bodySmall: typographyContract,
    bodySmallStrong: typographyContract,
    bodySmallXStrong: typographyContract,
    bodyStrong: typographyContract,
    button: typographyContract,
    buttonStrong: typographyContract,
    code: typographyContract,
    codeInline: typographyContract,
    codeInlineStrong: typographyContract,
    codeLarge: typographyContract,
    codeSmallInline: typographyContract,
    codeSmallInlineStrong: typographyContract,
    headingLarge: typographyContract,
    headingMedium: typographyContract,
    headingSmall: typographyContract,
    headingXLarge: typographyContract,
    headingXSmall: typographyContract,
  },
})

export { zIndex } from './z-index'
