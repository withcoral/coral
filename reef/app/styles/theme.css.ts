import { createTheme } from '@vanilla-extract/css'

import { fontFamily as waxFontFamily } from '@/wax/theme/font.css'

// Design tokens for the application theme

const colors = {
  black: {
    1: '#0C111D',
    2: '#141A27',
    3: '#1A202E',
    4: '#272E3E',
    5: '#383F4D',
    6: '#474D5B',
  },
  blue: {
    12: '#1A2938',
    24: '#274153',
    60: '#5089A5',
    80: '#66B1D2',
    90: '#72C5E8',
    100: '#7DD9FF',
  },
  error: '#FA6052',
  green: {
    1: '#6FE794',
    12: '#192E2D',
    24: '#264A3D',
    60: '#4EA06C',
    80: '#64CF87',
    90: '#6FE794',
    100: '#7AFFA1',
  },
  grey: {
    1: '#CECFD2',
    2: '#B4B6BA',
    3: '#94969C',
    4: '#61646C',
  },
  orange: {
    12: '#292229',
    24: '#463234',
    60: '#9E6457',
    80: '#CE806A',
    90: '#E78E73',
    100: '#FF9C7D',
  },
  coral: '#7AFFA1',
  purple: {
    12: '#201E38',
    24: '#352B53',
    60: '#7251A5',
    80: '#9467D2',
    90: '#A571E8',
    100: '#B67CFF',
  },
  sev: {
    1: '#F97B59',
    2: '#FA9275',
    3: '#FAA890',
    4: '#FBBFAC',
    100: '#94969C',
    unknown: '#B4B6BA',
  },
  state: {
    resolved: '#6FE794',
    unknown: '#B4B6BA',
    unresolved: '#FA6052',
  },
  warning: '#7C3A3A',
  white: {
    50: '#F5F5F6',
    100: '#FFFFFF',
  },
  yellow: {
    12: '#292C28',
    24: '#464734',
    60: '#9E9956',
    80: '#CEC669',
    90: '#E7DC73',
    100: '#FFF37C',
  },
} as const

const spacing = {
  0: '0px',
  25: '2px',
  50: '4px',
  75: '6px',
  100: '8px',
  150: '12px',
  200: '16px',
  250: '20px',
  400: '32px',
  600: '48px',
  800: '64px',
  1000: '80px',
} as const

const shadows = {
  dark3: '0 2px 4px rgba(0, 0, 0, 0.4)',
  dark4: '0 0 4px rgba(0, 0, 0, 0.6)',
} as const

const breakpoints = {
  lg: '1490px',
  md: '963px',
  mobile: '640px',
  sm: '0px',
  xxl: '1790px',
} as const

const fontFamily = {
  code: waxFontFamily.dmMono,
  sans: waxFontFamily.encodeSans,
} as const

const typography = {
  buttonLarge400: {
    fontSize: '14px',
    fontWeight: '400',
    lineHeight: '16.94px',
  },
  buttonLarge600: {
    fontSize: '14px',
    fontWeight: '600',
    lineHeight: '16.94px',
  },
  buttonMedium: {
    fontSize: '12px',
    fontWeight: '400',
    lineHeight: '14.52px',
  },
  buttonSmall: {
    fontSize: '12px',
    fontWeight: '400',
    lineHeight: '14.52px',
  },
  cardValues: {
    fontSize: '15px',
    fontWeight: '600',
    lineHeight: '18px',
  },
  chips: {
    fontSize: '12px',
    fontWeight: '500',
    lineHeight: '16px',
  },
  codeMedium500: {
    fontSize: '14px',
    fontWeight: '500',
    lineHeight: '20px',
  },
  graphL: {
    fontSize: '14px',
    fontWeight: '400',
    lineHeight: '20px',
  },
  graphM: {
    fontSize: '12px',
    fontWeight: '400',
    lineHeight: '16px',
  },
  incident: {
    fontSize: '20px',
    fontWeight: '600',
    lineHeight: '22px',
  },
  large400: {
    fontSize: '16px',
    fontWeight: '400',
    lineHeight: '24px',
  },
  large600: {
    fontSize: '16px',
    fontWeight: '600',
    lineHeight: '24px',
  },
  level2: {
    fontSize: '12px',
    fontWeight: '600',
    lineHeight: '16px',
  },
  level3: {
    fontSize: '11px',
    fontWeight: '600',
    lineHeight: '14px',
  },
  listControls: {
    fontSize: '12px',
    fontWeight: '400',
    lineHeight: '16px',
  },
  listLevel1_400: {
    fontSize: '14px',
    fontWeight: '400',
    lineHeight: '20px',
  },
  listLevel1_600: {
    fontSize: '14px',
    fontWeight: '600',
    lineHeight: '20px',
  },
  listTitle: {
    fontSize: '13px',
    fontWeight: '600',
    lineHeight: '17px',
  },
  medium400: {
    fontSize: '14px',
    fontWeight: '400',
    lineHeight: '20px',
  },
  medium600: {
    fontSize: '14px',
    fontWeight: '600',
    lineHeight: '20px',
  },
  searchboxDefault: {
    fontSize: '20px',
    fontWeight: '400',
    lineHeight: '22px',
  },
  searchboxLinks: {
    fontSize: '16px',
    fontWeight: '400',
    lineHeight: '22px',
  },
  searchboxResults: {
    fontSize: '16px',
    fontWeight: '600',
    lineHeight: '22px',
  },
  small400: {
    fontSize: '12px',
    fontWeight: '400',
    lineHeight: '16px',
  },
  small600: {
    fontSize: '12px',
    fontWeight: '600',
    lineHeight: '16px',
  },
  title: {
    fontSize: '20px',
    fontWeight: '600',
    lineHeight: '22px',
  },
  truncate: {
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  xsmall400: {
    fontSize: '10px',
    fontWeight: '400',
    lineHeight: '14px',
  },
  xsmall600: {
    fontSize: '10px',
    fontWeight: '600',
    lineHeight: '14px',
  },
} as const

export const [themeClass, vars] = createTheme({
  breakpoint: breakpoints,
  color: colors,
  fontFamily,
  shadow: shadows,
  space: spacing,
  typography,
})

// Export individual pieces for convenience
export { breakpoints, colors, fontFamily, shadows, spacing, typography }
