import { createThemeContract } from '@vanilla-extract/css'

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
}

const pillColorContract = {
  background: '',
  backgroundHover: '',
  color: '',
  colorHover: '',
  stroke: '',
}

const causeGraphColorContract = {
  accent: '',
  background: '',
  backgroundHover: '',
  stroke: '',
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
  causeGraph: {
    amber: causeGraphColorContract,
    blue: causeGraphColorContract,
    green: causeGraphColorContract,
    orange: causeGraphColorContract,
    purple: causeGraphColorContract,
    red: causeGraphColorContract,
  },
  content: {
    accentContent: {
      primary: '',
      primaryReverse: '',
      secondary: '',
      tertiary: '',
    },
    code: {
      inlineBackground: '',
      inlineColor: '',
    },
    disabled: '',
    error: '',
    info: '',
    link: '',
    linkHover: '',
    placeholder: '',
    primary: '',
    secondary: '',
    selection: '',
    success: '',
    tertiary: '',
    warning: '',
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
  input: {
    stroke: {
      default: '',
      disabled: '',
      focus: '',
      hover: '',
    },
  },
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
    button: {
      hover: '',
      selected: '',
    },
    buttonAccent: {
      hover: '',
      selected: '',
    },
  },
  stroke: {
    floating: '',
    focused: '',
    mainContent: '',
    primary: '',
    secondary: '',
  },
  surface: {
    backdrop: '',
    card: '',
    floating: '',
    main: '',
    mainContent: '',
    onMainContent: '',
    onMainContentHover: '',
    onMainContentSubtle: '',
    skeleton: '',
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
    bodyStrong: typographyContract,
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
