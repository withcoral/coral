import { style } from '@vanilla-extract/css'

export const header = style({
  alignItems: 'center',
  gap: 10,
  marginBlockEnd: 14,
  paddingBlockEnd: 0,
})

export const dialogContent = style({
  display: 'flex',
  flexDirection: 'column',
})

/** The steps are portalled popups, so the form must not take a row of its own. */
export const stepForm = style({
  display: 'contents',
})

export const fieldGroup = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 14,
})

export const fieldItem = style({
  gap: 6,
})

export const authPanelStack = style({
  display: 'grid',
})

export const authPanel = style({
  display: 'flex',
  flexDirection: 'column',
  gap: 14,
  gridArea: '1 / 1',
  minWidth: 0,
})

export const oauthDevicePanel = style([
  authPanel,
  {
    display: 'grid',
    gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
    '@media': {
      'screen and (max-width: 600px)': {
        gridTemplateColumns: 'minmax(0, 1fr)',
      },
    },
  },
])

export const authPanelHidden = style({
  pointerEvents: 'none',
  visibility: 'hidden',
})

export const importError = style({
  marginBlockStart: 14,
})
