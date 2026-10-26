import { createTheme } from '@/wax/css'

import { utils } from '@/styles/utils'
import { paletteDark } from '@/wax/base/palette/palette-dark'
import { fontFamily } from '@/wax/theme/font.css'
import { theme } from '@/wax/theme/theme.css'

export const darkThemeValues = {
  avatarFallback: {
    amber: {
      background: paletteDark.Amber['04'],
      color: paletteDark.Amber['11'],
    },
    blue: {
      background: paletteDark.Blue['04'],
      color: paletteDark.Blue['11'],
    },
    green: {
      background: paletteDark.Green['04'],
      color: paletteDark.Green['11'],
    },
    purple: {
      background: paletteDark.Purple['04'],
      color: paletteDark.Purple['11'],
    },
    red: {
      background: paletteDark.Red['04'],
      color: paletteDark.Red['11'],
    },
  },
  button: {
    bare: {
      hover: paletteDark.GrayAlpha['05'],
    },
    destructive: {
      default: paletteDark.Red['09'],
      focus: paletteDark.Red['10'],
      hover: paletteDark.Red['10'],
    },
    primary: {
      default: paletteDark.CoralGreen['09'],
      disabled: paletteDark.GrayAlpha['02'],
      focus: paletteDark.CoralGreen['11'],
      hover: paletteDark.CoralGreen['12'],
    },
    secondary: {
      default: paletteDark.GrayAlpha['03'],
      focus: paletteDark.GrayAlpha['05'],
      hover: paletteDark.GrayAlpha['05'],
    },
  },
  chart: {
    amber: {
      fill: paletteDark.Amber['10'],
    },
    blue: {
      fill: paletteDark.Blue['10'],
    },
    green: {
      fill: paletteDark.Green['10'],
    },
    neutral: {
      fill: paletteDark.GrayAlpha['07'],
    },
    orange: {
      fill: paletteDark.Orange['10'],
    },
    purple: {
      fill: paletteDark.Purple['10'],
    },
    red: {
      fill: paletteDark.RedAlpha['10'],
    },
  },
  content: {
    accentContent: {
      iconFill: paletteDark.CoralGreenAlpha['03'],
      primary: paletteDark.CoralGreen['12'],
      primaryReverse: paletteDark.CoralGreen['02'],
      secondary: paletteDark.CoralGreen['11'],
      tertiary: paletteDark.CoralGreen['10'],
    },
    code: {
      inlineBackground: paletteDark.GrayAlpha['02'],
      inlineColor: paletteDark.Amber['12'],
      syntax: {
        comment: paletteDark.Gray['11'],
        constant: paletteDark.Orange['11'],
        function: paletteDark.Blue['11'],
        keyword: paletteDark.Purple['11'],
        string: paletteDark.Green['11'],
        type: paletteDark.Amber['11'],
      },
    },
    disabled: paletteDark.GrayAlpha['07'],
    error: paletteDark.RedAlpha['10'],
    errorBackground: paletteDark.RedAlpha['03'],
    info: paletteDark.BlueAlpha['10'],
    link: paletteDark.CoralGreen['11'],
    linkHover: paletteDark.CoralGreen['12'],
    placeholder: paletteDark.GrayAlpha['08'],
    primary: paletteDark.GrayAlpha['12'],
    secondary: paletteDark.GrayAlpha['11'],
    selection: paletteDark.CoralGreenAlpha['04'],
    success: paletteDark.GreenAlpha['10'],
    successBackground: paletteDark.GreenAlpha['03'],
    tertiary: paletteDark.GrayAlpha['09'],
    muted: paletteDark.GrayAlpha['11'],
    warning: paletteDark.AmberAlpha['11'],
    warningBackground: paletteDark.AmberAlpha['03'],
  },
  elevation: {
    e1: `0 4px 24px 0 ${paletteDark.BlackAlpha['03']}, 0 1px 2px 0 ${paletteDark.BlackAlpha['05']}`,
    e2: `0 2px 8px 0 ${paletteDark.BlackAlpha['07']}, 0 1px 2px 0 ${paletteDark.BlackAlpha['07']}`,
    e3: `0 4px 24px 0 ${paletteDark.BlackAlpha['09']}, 0 1px 2px 0 ${paletteDark.BlackAlpha['06']}`,
    e4: `0 32px 96px 0 ${paletteDark.BlackAlpha['10']}, 0 1px 2px 0 ${paletteDark.BlackAlpha['09']}`,
  },
  gradient: {
    card: `linear-gradient(92deg, ${utils.opacify(paletteDark.CoralGreenAlpha['01'], 20)} 0%, ${utils.opacify(paletteDark.CoralGreenAlpha['07'], 20)} 100%), ${paletteDark.GrayAlpha['02']}`,
  },
  help: {
    componentBackground: paletteDark.Gray['02'],
  },
  input: {
    stroke: {
      default: paletteDark.GrayAlpha['04'],
      disabled: paletteDark.GrayAlpha['03'],
      error: paletteDark.RedAlpha['07'],
      focus: paletteDark.CoralGreen['11'],
      hover: paletteDark.GrayAlpha['05'],
    },
  },
  logoPlate: {
    background: paletteDark.GrayAlpha['04'],
    stroke: paletteDark.GrayAlpha['05'],
  },
  pill: {
    amber: {
      background: paletteDark.AmberAlpha['02'],
      backgroundHover: paletteDark.AmberAlpha['03'],
      color: paletteDark.Amber['10'],
      colorHover: paletteDark.Amber['11'],
      stroke: paletteDark.AmberAlpha['06'],
    },
    blue: {
      background: paletteDark.BlueAlpha['02'],
      backgroundHover: paletteDark.BlueAlpha['03'],
      color: paletteDark.Blue['10'],
      colorHover: paletteDark.Blue['10'],
      stroke: paletteDark.BlueAlpha['06'],
    },
    gray: {
      background: paletteDark.GrayAlpha['02'],
      backgroundHover: paletteDark.GrayAlpha['03'],
      color: paletteDark.Gray['11'],
      colorHover: paletteDark.Gray['12'],
      stroke: paletteDark.GrayAlpha['06'],
    },
    graySubtle: {
      background: paletteDark.GrayAlpha['02'],
      backgroundHover: paletteDark.GrayAlpha['03'],
      color: paletteDark.Gray['09'],
      colorHover: paletteDark.Gray['12'],
      stroke: paletteDark.GrayAlpha['06'],
    },
    green: {
      background: paletteDark.GreenAlpha['03'],
      backgroundHover: paletteDark.GreenAlpha['04'],
      color: paletteDark.Green['10'],
      colorHover: paletteDark.Green['11'],
      stroke: paletteDark.GreenAlpha['06'],
    },
    mention: {
      background: paletteDark.CoralGreenAlpha['03'],
      backgroundHover: paletteDark.CoralGreenAlpha['04'],
      color: paletteDark.CoralGreen['11'],
      colorHover: paletteDark.CoralGreen['11'],
      stroke: paletteDark.CoralGreenAlpha['05'],
    },
    orange: {
      background: paletteDark.OrangeAlpha['02'],
      backgroundHover: paletteDark.OrangeAlpha['03'],
      color: paletteDark.Orange['11'],
      colorHover: paletteDark.Orange['11'],
      stroke: paletteDark.OrangeAlpha['06'],
    },
    purple: {
      background: paletteDark.PurpleAlpha['02'],
      backgroundHover: paletteDark.PurpleAlpha['03'],
      color: paletteDark.Purple['11'],
      colorHover: paletteDark.Purple['11'],
      stroke: paletteDark.PurpleAlpha['06'],
    },
    red: {
      background: paletteDark.RedAlpha['02'],
      backgroundHover: paletteDark.RedAlpha['03'],
      color: paletteDark.Red['10'],
      colorHover: paletteDark.Red['11'],
      stroke: paletteDark.RedAlpha['06'],
    },
  },
  sidebar: {
    // The primary button's colors: a pending action is your next step, not an error.
    badge: { background: paletteDark.CoralGreen['09'], content: paletteDark.CoralGreen['02'] },
    button: {
      hover: paletteDark.GrayAlpha['02'],
      selected: paletteDark.GrayAlpha['03'],
    },
    buttonAccent: {
      default: paletteDark.CoralGreenAlpha['02'],
      hover: paletteDark.CoralGreenAlpha['03'],
      selected: paletteDark.CoralGreenAlpha['03'],
    },
  },
  stroke: {
    control: paletteDark.GrayAlpha['09'],
    floating: paletteDark.Gray['06'],
    focused: paletteDark.CoralGreen['11'],
    mainContent: paletteDark.GrayAlpha['02'],
    primary: paletteDark.GrayAlpha['03'],
    secondary: paletteDark.GrayAlpha['02'],
  },
  surface: {
    backdrop: paletteDark.BlackAlpha['08'],
    card: paletteDark.GrayAlpha['02'],
    cardHover: paletteDark.GrayAlpha['04'],
    floating: paletteDark.Gray['04'],
    main: paletteDark.Gray['02'],
    mainContent: paletteDark.Gray['03'],
    onMainContent: paletteDark.GrayAlpha['02'],
    onMainContentHover: paletteDark.GrayAlpha['03'],
    onMainContentSubtle: paletteDark.GrayAlpha['01'],
    skeleton: paletteDark.GrayAlpha['03'],
  },
  segmentedControl: {
    checked: paletteDark.Gray['06'],
    checkedStroke: paletteDark.Gray['08'],
  },
  switch: {
    default: paletteDark.GrayAlpha['04'],
    hover: paletteDark.GrayAlpha['05'],
    hoverOn: paletteDark.CoralGreen['07'],
    on: paletteDark.CoralGreen['08'],
  },
  typography: {
    body: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '13px',
      fontWeight: '400',
      letterSpacing: '0.325px',
      lineHeight: '1.65',
    },
    bodyLarge: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '15px',
      fontWeight: '400',
      letterSpacing: '0.225px',
      lineHeight: '1.6',
    },
    bodyLargeStrong: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '15px',
      fontWeight: '500',
      letterSpacing: '0.225px',
      lineHeight: '1.6',
    },
    bodySmall: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '11.5px',
      fontWeight: '400',
      letterSpacing: '0.23px',
      lineHeight: '1.55',
    },
    bodySmallStrong: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '11.5px',
      fontWeight: '500',
      letterSpacing: '0.23px',
      lineHeight: '1.55',
    },
    bodySmallXStrong: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '11.5px',
      fontWeight: '700',
      letterSpacing: '0.23px',
      lineHeight: '1.55',
    },
    bodyStrong: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '13px',
      fontWeight: '500',
      letterSpacing: '0.13px',
      lineHeight: '1.65',
    },
    button: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '13px',
      fontWeight: '400',
      letterSpacing: '0.325px',
      lineHeight: '1.45',
    },
    buttonStrong: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '13px',
      fontWeight: '500',
      letterSpacing: '0.325px',
      lineHeight: '1.45',
    },
    code: {
      fontFamily: fontFamily.dmMono,
      fontSize: '13px',
      fontWeight: '400',
      letterSpacing: '0',
      lineHeight: '1.65',
    },
    codeInline: {
      fontFamily: fontFamily.dmMono,
      fontSize: '13px',
      fontWeight: '400',
      letterSpacing: '0',
      lineHeight: '1.65',
    },
    codeInlineStrong: {
      fontFamily: fontFamily.dmMono,
      fontSize: '13px',
      fontWeight: '500',
      letterSpacing: '0',
      lineHeight: '1.65',
    },
    codeLarge: {
      fontFamily: fontFamily.dmMono,
      fontSize: '15px',
      fontWeight: '400',
      letterSpacing: '0',
      lineHeight: '1.65',
    },
    codeSmallInline: {
      fontFamily: fontFamily.dmMono,
      fontSize: '11.5px',
      fontWeight: '400',
      letterSpacing: '0',
      lineHeight: '1.55',
    },
    codeSmallInlineStrong: {
      fontFamily: fontFamily.dmMono,
      fontSize: '11.5px',
      fontWeight: '500',
      letterSpacing: '0',
      lineHeight: '1.55',
    },
    headingLarge: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '19px',
      fontWeight: '500',
      letterSpacing: '0',
      lineHeight: '1.45',
    },
    headingMedium: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '17px',
      fontWeight: '500',
      letterSpacing: '0',
      lineHeight: '1.5',
    },
    headingSmall: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '15px',
      fontWeight: '500',
      letterSpacing: '0',
      lineHeight: '1.5',
    },
    headingXLarge: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '23px',
      fontWeight: '500',
      letterSpacing: '-0.23px',
      lineHeight: '1.35',
    },
    headingXSmall: {
      fontFamily: fontFamily.encodeSans,
      fontSize: '13px',
      fontWeight: '500',
      letterSpacing: '0.325px',
      lineHeight: '1.65',
    },
  },
}

export const darkTheme = createTheme(theme, darkThemeValues)
