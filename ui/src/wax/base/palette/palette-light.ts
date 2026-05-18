import { utils } from '@/styles/utils'
import { PaletteValues } from '@/wax/base/palette/types'

const Gray: PaletteValues['Gray'] = {
  '01': '#FCFCFC',
  '02': '#F9F9F9',
  '03': '#F0F0F0',
  '04': '#E8E8E8',
  '05': '#E0E0E0',
  '06': '#D9D9D9',
  '07': '#CECECE',
  '08': '#BBBBBB',
  '09': '#8D8D8D',
  '10': '#828282',
  '11': '#646464',
  '12': '#202020',
}

const GrayAlpha: PaletteValues['GrayAlpha'] = {
  '01': utils.opacify('#000000', 1.18),
  '02': utils.opacify('#000000', 2.35),
  '03': utils.opacify('#000000', 5.88),
  '04': utils.opacify('#000000', 9.02),
  '05': utils.opacify('#000000', 12.16),
  '06': utils.opacify('#000000', 14.9),
  '07': utils.opacify('#000000', 19.22),
  '08': utils.opacify('#000000', 26.67),
  '09': utils.opacify('#000000', 44.71),
  '10': utils.opacify('#000000', 49.02),
  '11': utils.opacify('#000000', 60.78),
  '12': utils.opacify('#000000', 87.45),
}

const PhoebeGreen: PaletteValues['PhoebeGreen'] = {
  '01': '#F9FEFA',
  '02': '#F2FCF4',
  '03': '#DAFCE2',
  '04': '#C2F9CF',
  '05': '#A9F2BB',
  '06': '#8BE8A4',
  '07': '#61D987',
  '08': '#00C75C',
  '09': '#00D65C',
  '10': '#00C75C',
  '11': '#298046',
  '12': '#21442A',
}

const PhoebeGreenAlpha: PaletteValues['PhoebeGreenAlpha'] = {
  '01': utils.opacify('#00D52B', 2.35),
  '02': utils.opacify('#00C528', 5.1),
  '03': utils.opacify('#00EB38', 14.51),
  '04': utils.opacify('#00E637', 23.92),
  '05': utils.opacify('#00D936', 33.73),
  '06': utils.opacify('#00CD37', 45.49),
  '07': utils.opacify('#00C23E', 61.96),
  '08': '#00C75C',
  '09': '#00D65C',
  '10': '#00C75C',
  '11': '#008733',
  '12': utils.opacify('#00401B', 98.04),
}

const Purple: PaletteValues['Purple'] = {
  '01': '#FEFCFF',
  '02': '#FFF8FF',
  '03': '#FCEFFC',
  '04': '#F9E5F9',
  '05': '#F3D9F4',
  '06': '#EBC8ED',
  '07': '#DFAFE3',
  '08': '#CF91D8',
  '09': '#AB4ABA',
  '10': '#A43CB4',
  '11': '#9C2BAD',
  '12': '#340C3B',
}

const PurpleAlpha: PaletteValues['PurpleAlpha'] = {
  '01': utils.opacify('#B74BFF', 1.57),
  '02': utils.opacify('#B52AFB', 4.31),
  '03': utils.opacify('#BC43FE', 12.94),
  '04': utils.opacify('#B74BFF', 17.65),
  '05': utils.opacify('#B74BFF', 23.53),
  '06': utils.opacify('#B74BFF', 32.94),
  '07': utils.opacify('#B74BFF', 44.31),
  '08': utils.opacify('#B74BFF', 60),
  '09': utils.opacify('#B74BFF', 75),
  '10': utils.opacify('#CB81FF', 85),
  '11': utils.opacify('#9C2BAD', 93.33),
  '12': utils.opacify('#340C3B', 97),
}

const Blue: PaletteValues['Blue'] = {
  '01': '#FBFDFF',
  '02': '#F4FAFF',
  '03': '#E6F4FE',
  '04': '#D5EFFF',
  '05': '#C2E5FF',
  '06': '#ACD8FC',
  '07': '#8EC8F6',
  '08': '#5EB1EF',
  '09': '#0090FF',
  '10': '#0588F0',
  '11': '#0D74CE',
  '12': '#113264',
}

const BlueAlpha: PaletteValues['BlueAlpha'] = {
  '01': utils.opacify('#0080FF', 1.57),
  '02': utils.opacify('#008CFF', 4.31),
  '03': utils.opacify('#008FF5', 9.8),
  '04': utils.opacify('#009EFF', 16.47),
  '05': utils.opacify('#0093FF', 23.92),
  '06': utils.opacify('#0088F6', 32.55),
  '07': utils.opacify('#0083EB', 44.31),
  '08': utils.opacify('#0084E6', 63.14),
  '09': '#0090FF',
  '10': utils.opacify('#0086F0', 98.04),
  '11': utils.opacify('#006DCB', 94.9),
  '12': utils.opacify('#002359', 93.33),
}

const Amber: PaletteValues['Amber'] = {
  '01': '#FEFDFB',
  '02': '#FEFBE9',
  '03': '#FFF7C2',
  '04': '#FFEE9C',
  '05': '#FBE577',
  '06': '#F3D673',
  '07': '#E9C162',
  '08': '#E2A336',
  '09': '#FFC53D',
  '10': '#FFBA18',
  '11': '#AB6400',
  '12': '#4F3422',
}

const AmberAlpha: PaletteValues['AmberAlpha'] = {
  '01': utils.opacify('#C08000', 1.57),
  '02': utils.opacify('#F4D100', 8.63),
  '03': utils.opacify('#FFDE00', 23.92),
  '04': utils.opacify('#FFD400', 38.82),
  '05': utils.opacify('#F8CF00', 53.33),
  '06': utils.opacify('#EAB500', 54.9),
  '07': utils.opacify('#DC9B00', 61.57),
  '08': utils.opacify('#DA8A00', 78.82),
  '09': '#FFB300',
  '10': '#FFB300',
  '11': '#AB6400',
  '12': '#341500',
}

const Red: PaletteValues['Red'] = {
  '01': '#FFFCFC',
  '02': '#FFF7F7',
  '03': '#FEEBEC',
  '04': '#FFDBDC',
  '05': '#FFCDCE',
  '06': '#FDBDBE',
  '07': '#F4A9AA',
  '08': '#EB8E90',
  '09': '#E5484D',
  '10': '#DC3E42',
  '11': '#CE2C31',
  '12': '#641723',
}

const RedAlpha: PaletteValues['RedAlpha'] = {
  '01': utils.opacify('#FF0000', 1.18),
  '02': utils.opacify('#FF0000', 3.14),
  '03': utils.opacify('#F3000D', 7.84),
  '04': utils.opacify('#FF0008', 14.12),
  '05': utils.opacify('#FF0006', 19.61),
  '06': utils.opacify('#F80004', 25.88),
  '07': utils.opacify('#DF0003', 33.73),
  '08': utils.opacify('#D20005', 44.31),
  '09': utils.opacify('#DB0007', 71.76),
  '10': utils.opacify('#D10005', 75.69),
  '11': utils.opacify('#C40006', 82.75),
  '12': utils.opacify('#55000D', 90.98),
}

const Green: PaletteValues['Green'] = {
  '01': '#FBFEFC',
  '02': '#F4FBF6',
  '03': '#E6F6EB',
  '04': '#D6F1DF',
  '05': '#C4E8D1',
  '06': '#ADDDC0',
  '07': '#8ECEAA',
  '08': '#5BB98B',
  '09': '#30A46C',
  '10': '#2B9A66',
  '11': '#218358',
  '12': '#193B2D',
}

const GreenAlpha: PaletteValues['GreenAlpha'] = {
  '01': utils.opacify('#00DE45', 1.96),
  '02': utils.opacify('#29F99D', 4.31),
  '03': utils.opacify('#22FF99', 11.76),
  '04': utils.opacify('#11FF99', 17.65),
  '05': utils.opacify('#2BFFA2', 23.53),
  '06': utils.opacify('#44FFAA', 29.41),
  '07': utils.opacify('#50FDAC', 36.86),
  '08': utils.opacify('#54FFAD', 45.1),
  '09': utils.opacify('#44FFA4', 61.96),
  '10': utils.opacify('#43FEA4', 67.06),
  '11': utils.opacify('#46FEA5', 83.14),
  '12': utils.opacify('#BBFFD7', 94.12),
}

const Orange: PaletteValues['Orange'] = {
  '01': '#FEFCFB',
  '02': '#FEF8F4',
  '03': '#FFF1E7',
  '04': '#FFE8D7',
  '05': '#FFDCC3',
  '06': '#FFCCA7',
  '07': '#FFB381',
  '08': '#FA934E',
  '09': '#F76808',
  '10': '#ED5F00',
  '11': '#BD4B00',
  '12': '#451E11',
}

const OrangeAlpha: PaletteValues['OrangeAlpha'] = {
  '01': utils.opacify('#C04305', 1.57),
  '02': utils.opacify('#E86005', 4.31),
  '03': utils.opacify('#FF6C03', 9.41),
  '04': utils.opacify('#FF6E00', 15.69),
  '05': utils.opacify('#FF6B01', 23.53),
  '06': utils.opacify('#FF6B01', 34.51),
  '07': utils.opacify('#FF6601', 49.41),
  '08': utils.opacify('#F86300', 69.41),
  '09': utils.opacify('#F76300', 96.86),
  '10': utils.opacify('#ED5B00', 97.65),
  '11': utils.opacify('#BC4800', 97.65),
  '12': utils.opacify('#380E00', 93.33),
}

const BlackAlpha: PaletteValues['BlackAlpha'] = {
  '01': utils.opacify('#000000', 1.2),
  '02': utils.opacify('#000000', 2.7),
  '03': utils.opacify('#000000', 4.7),
  '04': utils.opacify('#000000', 7.1),
  '05': utils.opacify('#000000', 9),
  '06': utils.opacify('#000000', 11.4),
  '07': utils.opacify('#000000', 14.1),
  '08': utils.opacify('#000000', 22),
  '09': utils.opacify('#000000', 43.9),
  '10': utils.opacify('#000000', 47.8),
  '11': utils.opacify('#000000', 56.5),
  '12': utils.opacify('#000000', 91),
}

export const paletteLight: PaletteValues = {
  Amber,
  AmberAlpha,
  BlackAlpha,
  Blue,
  BlueAlpha,
  Gray,
  GrayAlpha,
  Green,
  GreenAlpha,
  Orange,
  OrangeAlpha,
  PhoebeGreen,
  PhoebeGreenAlpha,
  Purple,
  PurpleAlpha,
  Red,
  RedAlpha,
}
