export type ColorScale = Record<Scale, string>

export interface PaletteValues {
  Amber: ColorScale
  AmberAlpha: ColorScale
  BlackAlpha: ColorScale
  Blue: ColorScale
  BlueAlpha: ColorScale
  Gray: ColorScale
  GrayAlpha: ColorScale
  Green: ColorScale
  GreenAlpha: ColorScale
  Orange: ColorScale
  OrangeAlpha: ColorScale
  PhoebeGreen: ColorScale
  PhoebeGreenAlpha: ColorScale
  Purple: ColorScale
  PurpleAlpha: ColorScale
  Red: ColorScale
  RedAlpha: ColorScale
}

export type Scale = '01' | '02' | '03' | '04' | '05' | '06' | '07' | '08' | '09' | '10' | '11' | '12'
