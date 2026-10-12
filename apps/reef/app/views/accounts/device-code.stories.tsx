import type { Meta, StoryObj } from '@storybook/react-vite'
import { Typography } from '@/wax/components'
import { DeviceCodeDialog, DeviceCodePanel } from './device-code'

const meta = {
  title: 'Reef/Accounts/Device code',
  component: DeviceCodeDialog,
  args: { code: 'ABCD-EFGH', formId: 'sign-in', pending: false, triggerLabel: 'Continue sign-in' },
} satisfies Meta<typeof DeviceCodeDialog>

export default meta
type Story = StoryObj<typeof meta>

/** The service issued a code: copy it and open the sign-in page. */
export const Issued: Story = {}

/** The code is on its way. The panel holds its size, so the code does not move the dialog. */
export const Requesting: Story = { args: { code: '' } }

/** A submission is in flight, so the sign-in page cannot open twice. */
export const Pending: Story = { args: { pending: true } }

/** Every panel state side by side, at the dialog's width, to compare their heights. */
export const PanelVariations: Story = {
  render: () => (
    <div style={{ display: 'grid', gap: 24, width: 440 }}>
      {[
        ['Requesting', ''],
        ['Short code', 'WDJB-MJHT'],
        ['Long code', 'ABCD-EFGH-IJKL'],
      ].map(([label, code]) => (
        <section key={label} style={{ display: 'grid', gap: 8 }}>
          <Typography.BodySmallStrong as="h3">{label}</Typography.BodySmallStrong>
          <DeviceCodePanel code={code} />
        </section>
      ))}
    </div>
  ),
}
