import { utils } from '@/styles/utils'
import type { PaletteValues } from '@/wax/base/palette/types'

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

const CoralGreen: PaletteValues['CoralGreen'] = {
  '01': '#FBFEFA',
  '02': '#F5FBF4',
  '03': '#E3FADE',
  '04': '#D1F5C9',
  '05': '#BEEDB3',
  '06': '#AAE19D',
  '07': '#8FD180',
  '08': '#69BD54',
  '09': '#81D66D',
  '10': '#77CC63',
  '11': '#398125',
  '12': '#25411E',
}

const CoralGreenAlpha: PaletteValues['CoralGreenAlpha'] = {
  '01': utils.opacify('#33CC00', 1.96),
  '02': utils.opacify('#18A300', 4.31),
  '03': utils.opacify('#27D900', 12.94),
  '04': utils.opacify('#26D000', 21.18),
  '05': utils.opacify('#25C300', 29.8),
  '06': utils.opacify('#25C300', 29.8),
  '07': utils.opacify('#1FA300', 49.8),
  '08': utils.opacify('#209D00', 67.06),
  '09': utils.opacify('#23B800', 57.25),
  '10': utils.opacify('#21AC00', 61.18),
  '11': utils.opacify('#176C00', 85.49),
  '12': utils.opacify('#082800', 88.24),
}

const Purple: PaletteValues['Purple'] = {
  '01': '#FEFCFE',
  '02': '#FDFAFF',
  '03': '#F9F1FE',
  '04': '#F3E7FC',
  '05': '#EDDBF9',
  '06': '#E3CCF4',
  '07': '#D3B4ED',
  '08': '#BE93E4',
  '09': '#8E4EC6',
  '10': '#8445BC',
  '11': '#793AAF',
  '12': '#2B0E44',
}

const PurpleAlpha: PaletteValues['PurpleAlpha'] = {
  '01': utils.opacify('#AB05AB', 1.2),
  '02': utils.opacify('#9B05FF', 2),
  '03': utils.opacify('#9200ED', 5.5),
  '04': utils.opacify('#8002E0', 9.5),
  '05': utils.opacify('#8001D5', 14.2),
  '06': utils.opacify('#7500C8', 20),
  '07': utils.opacify('#6B01C2', 29.5),
  '08': utils.opacify('#6600BF', 42.4),
  '09': utils.opacify('#5C00AD', 69.5),
  '10': utils.opacify('#5700A3', 73),
  '11': utils.opacify('#510097', 76.3),
  '12': utils.opacify('#1F0039', 94.6),
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
  CoralGreen,
  CoralGreenAlpha,
  Gray,
  GrayAlpha,
  Green,
  GreenAlpha,
  Orange,
  OrangeAlpha,
  Purple,
  PurpleAlpha,
  Red,
  RedAlpha,
}
