import { createTheme } from '@/wax/css'

import { utils } from '@/styles/utils'
import { paletteLight } from '@/wax/base/palette/palette-light'
import { fontFamily } from '@/wax/theme/font.css'
import { staticUtilities, theme } from '@/wax/theme/theme.css'

export const lightThemeValues = {
  avatarFallback: {
    amber: {
      background: paletteLight.Amber['04'],
      color: paletteLight.Amber['11'],
    },
    blue: {
      background: paletteLight.Blue['04'],
      color: paletteLight.Blue['11'],
    },
    green: {
      background: paletteLight.Green['04'],
      color: paletteLight.Green['11'],
    },
    purple: {
      background: paletteLight.Purple['04'],
      color: paletteLight.Purple['11'],
    },
    red: {
      background: paletteLight.Red['04'],
      color: paletteLight.Red['11'],
    },
  },
  button: {
    bare: {
      hover: paletteLight.GrayAlpha['04'],
    },
    destructive: {
      default: paletteLight.Red['09'],
      focus: paletteLight.Red['10'],
      hover: paletteLight.Red['10'],
    },
    primary: {
      default: paletteLight.CoralGreen['09'],
      disabled: paletteLight.GrayAlpha['02'],
      focus: paletteLight.CoralGreen['10'],
      hover: paletteLight.CoralGreen['10'],
    },
    secondary: {
      default: paletteLight.GrayAlpha['03'],
      focus: paletteLight.GrayAlpha['05'],
      hover: paletteLight.GrayAlpha['04'],
    },
  },
  chart: {
    amber: {
      fill: paletteLight.Amber['10'],
    },
    blue: {
      fill: paletteLight.Blue['10'],
    },
    green: {
      fill: paletteLight.Green['10'],
    },
    neutral: {
      fill: paletteLight.GrayAlpha['07'],
    },
    orange: {
      fill: paletteLight.Orange['10'],
    },
    purple: {
      fill: paletteLight.Purple['10'],
    },
    red: {
      fill: paletteLight.RedAlpha['10'],
    },
  },
  content: {
    accentContent: {
      iconFill: paletteLight.CoralGreenAlpha['03'],
      primary: paletteLight.CoralGreen['12'],
      primaryReverse: paletteLight.CoralGreen['12'],
      secondary: paletteLight.CoralGreen['11'],
      tertiary: paletteLight.CoralGreen['10'],
    },
    code: {
      inlineBackground: paletteLight.GrayAlpha['02'],
      inlineColor: paletteLight.Amber['11'],
      syntax: {
        comment: paletteLight.Gray['11'],
        constant: paletteLight.Orange['11'],
        function: paletteLight.Blue['11'],
        keyword: paletteLight.Purple['11'],
        string: paletteLight.Green['11'],
        type: paletteLight.Amber['11'],
      },
    },
    disabled: paletteLight.GrayAlpha['07'],
    error: paletteLight.RedAlpha['11'],
    errorBackground: paletteLight.RedAlpha['03'],
    info: paletteLight.BlueAlpha['10'],
    link: paletteLight.CoralGreen['11'],
    linkHover: paletteLight.CoralGreen['12'],
    placeholder: paletteLight.GrayAlpha['07'],
    primary: paletteLight.GrayAlpha['12'],
    secondary: paletteLight.GrayAlpha['12'],
    selection: paletteLight.CoralGreenAlpha['04'],
    success: paletteLight.GreenAlpha['11'],
    successBackground: paletteLight.GreenAlpha['03'],
    tertiary: paletteLight.GrayAlpha['09'],
    muted: paletteLight.GrayAlpha['11'],
    warning: paletteLight.AmberAlpha['11'],
    warningBackground: paletteLight.AmberAlpha['03'],
  },
  elevation: {
    e1: `0 4px 24px 0 ${paletteLight.BlackAlpha['01']}, 0 1px 2px 0 ${paletteLight.BlackAlpha['02']}`,
    e2: `0 2px 8px 0 ${paletteLight.BlackAlpha['03']}, 0 1px 2px 0 ${paletteLight.BlackAlpha['03']}`,
    e3: `0 4px 24px 0 ${paletteLight.BlackAlpha['05']}, 0 1px 2px 0 ${paletteLight.BlackAlpha['03']}`,
    e4: `0 32px 96px 0 ${paletteLight.BlackAlpha['08']}, 0 1px 2px 0 ${paletteLight.BlackAlpha['03']}`,
  },
  gradient: {
    card: `linear-gradient(92deg, ${utils.opacify(paletteLight.CoralGreenAlpha['01'], 20)} 0%, ${utils.opacify(paletteLight.CoralGreenAlpha['07'], 20)} 100%), ${staticUtilities.white}`,
  },
  help: {
    componentBackground: paletteLight.Gray['02'],
  },
  input: {
    stroke: {
      default: paletteLight.GrayAlpha['04'],
      disabled: paletteLight.GrayAlpha['03'],
      error: paletteLight.RedAlpha['07'],
      focus: paletteLight.CoralGreen['10'],
      hover: paletteLight.GrayAlpha['05'],
    },
  },
  logoPlate: {
    background: staticUtilities.white,
    stroke: paletteLight.GrayAlpha['04'],
  },
  pill: {
    amber: {
      background: paletteLight.AmberAlpha['03'],
      backgroundHover: paletteLight.AmberAlpha['04'],
      color: paletteLight.Amber['11'],
      colorHover: paletteLight.Amber['11'],
      stroke: paletteLight.AmberAlpha['06'],
    },
    blue: {
      background: paletteLight.BlueAlpha['03'],
      backgroundHover: paletteLight.BlueAlpha['04'],
      color: paletteLight.Blue['11'],
      colorHover: paletteLight.Blue['11'],
      stroke: paletteLight.BlueAlpha['06'],
    },
    gray: {
      background: paletteLight.GrayAlpha['03'],
      backgroundHover: paletteLight.GrayAlpha['04'],
      color: paletteLight.Gray['11'],
      colorHover: paletteLight.Gray['12'],
      stroke: paletteLight.GrayAlpha['06'],
    },
    graySubtle: {
      background: paletteLight.GrayAlpha['03'],
      backgroundHover: paletteLight.GrayAlpha['04'],
      color: paletteLight.Gray['09'],
      colorHover: paletteLight.Gray['12'],
      stroke: paletteLight.GrayAlpha['06'],
    },
    green: {
      background: paletteLight.GreenAlpha['03'],
      backgroundHover: paletteLight.GreenAlpha['04'],
      color: paletteLight.Green['11'],
      colorHover: paletteLight.Green['11'],
      stroke: paletteLight.GreenAlpha['06'],
    },
    mention: {
      background: paletteLight.CoralGreenAlpha['03'],
      backgroundHover: paletteLight.CoralGreenAlpha['04'],
      color: paletteLight.CoralGreen['11'],
      colorHover: paletteLight.CoralGreen['11'],
      stroke: paletteLight.CoralGreenAlpha['05'],
    },
    orange: {
      background: paletteLight.OrangeAlpha['03'],
      backgroundHover: paletteLight.OrangeAlpha['04'],
      color: paletteLight.OrangeAlpha['11'],
      colorHover: paletteLight.OrangeAlpha['11'],
      stroke: paletteLight.OrangeAlpha['06'],
    },
    purple: {
      background: paletteLight.PurpleAlpha['03'],
      backgroundHover: paletteLight.PurpleAlpha['04'],
      color: paletteLight.Purple['11'],
      colorHover: paletteLight.Purple['11'],
      stroke: paletteLight.PurpleAlpha['06'],
    },
    red: {
      background: paletteLight.RedAlpha['03'],
      backgroundHover: paletteLight.RedAlpha['04'],
      color: paletteLight.Red['11'],
      colorHover: paletteLight.Red['11'],
      stroke: paletteLight.RedAlpha['06'],
    },
  },
  sidebar: {
    // The primary button's colors: a pending action is your next step, not an error.
    badge: { background: paletteLight.CoralGreen['09'], content: paletteLight.CoralGreen['12'] },
    button: {
      hover: paletteLight.GrayAlpha['02'],
      selected: paletteLight.GrayAlpha['03'],
    },
    buttonAccent: {
      default: paletteLight.CoralGreenAlpha['03'],
      hover: paletteLight.CoralGreenAlpha['03'],
      selected: paletteLight.CoralGreenAlpha['03'],
    },
  },
  stroke: {
    control: paletteLight.GrayAlpha['09'],
    floating: paletteLight.GrayAlpha['03'],
    focused: paletteLight.CoralGreen['10'],
    mainContent: paletteLight.GrayAlpha['04'],
    primary: paletteLight.GrayAlpha['03'],
    secondary: paletteLight.GrayAlpha['03'],
  },
  surface: {
    backdrop: paletteLight.BlackAlpha['08'],
    card: staticUtilities.white,
    cardHover: paletteLight.GrayAlpha['01'],
    floating: staticUtilities.white,
    main: paletteLight.Gray['03'],
    mainContent: paletteLight.Gray['01'],
    onMainContent: paletteLight.GrayAlpha['02'],
    onMainContentHover: paletteLight.GrayAlpha['04'],
    onMainContentSubtle: paletteLight.GrayAlpha['01'],
    skeleton: paletteLight.GrayAlpha['03'],
  },
  segmentedControl: {
    checked: staticUtilities.white,
    checkedStroke: paletteLight.GrayAlpha['04'],
  },
  switch: {
    default: paletteLight.GrayAlpha['04'],
    hover: paletteLight.GrayAlpha['05'],
    hoverOn: paletteLight.CoralGreen['07'],
    on: paletteLight.CoralGreen['08'],
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

export const lightTheme = createTheme(theme, lightThemeValues)
