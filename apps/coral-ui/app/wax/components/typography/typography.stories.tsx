import type { Meta, StoryObj } from '@storybook/react-vite'

import { Typography } from '@/wax/components'

const meta = {
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  title: 'Wax/Typography',
} satisfies Meta

export default meta
type Story = StoryObj<typeof meta>

export const AllVariants: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Typography.HeadingXLarge>Heading XLarge</Typography.HeadingXLarge>
      <Typography.HeadingLarge>Heading Large</Typography.HeadingLarge>
      <Typography.HeadingMedium>Heading Medium</Typography.HeadingMedium>
      <Typography.HeadingSmall>Heading Small</Typography.HeadingSmall>
      <Typography.HeadingXSmall>Heading XSmall</Typography.HeadingXSmall>
      <Typography.Body>Body text</Typography.Body>
      <Typography.BodyStrong>Body Strong text</Typography.BodyStrong>
      <Typography.BodyLarge>Body Large text</Typography.BodyLarge>
      <Typography.BodyLargeStrong>Body Large Strong text</Typography.BodyLargeStrong>
      <Typography.BodySmall>Body Small text</Typography.BodySmall>
      <Typography.BodySmallStrong>Body Small Strong text</Typography.BodySmallStrong>
      <Typography.ButtonStrong>Button Strong text</Typography.ButtonStrong>
      <Typography.Code>Code text</Typography.Code>
      <Typography.CodeLarge>Code Large text</Typography.CodeLarge>
      <Typography.CodeInline>Code Inline text</Typography.CodeInline>
      <Typography.CodeInlineStrong>Code Inline Strong text</Typography.CodeInlineStrong>
      <Typography.CodeSmallInline>Code Small Inline text</Typography.CodeSmallInline>
      <Typography.CodeSmallInlineStrong>
        Code Small Inline Strong text
      </Typography.CodeSmallInlineStrong>
    </div>
  ),
}

export const Headings: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Typography.HeadingXLarge>Heading XLarge - 24px / 500</Typography.HeadingXLarge>
      <Typography.HeadingLarge>Heading Large - 20px / 500</Typography.HeadingLarge>
      <Typography.HeadingMedium>Heading Medium - 18px / 500</Typography.HeadingMedium>
      <Typography.HeadingSmall>Heading Small - 16px / 500</Typography.HeadingSmall>
      <Typography.HeadingXSmall>Heading XSmall - 14px / 500</Typography.HeadingXSmall>
    </div>
  ),
}

export const BodyText: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Typography.BodyLarge>Body Large - 16px / 400</Typography.BodyLarge>
      <Typography.BodyLargeStrong>Body Large Strong - 16px / 500</Typography.BodyLargeStrong>
      <Typography.Body>Body - 14px / 400</Typography.Body>
      <Typography.BodyStrong>Body Strong - 14px / 500</Typography.BodyStrong>
      <Typography.BodySmall>Body Small - 12px / 400</Typography.BodySmall>
      <Typography.BodySmallStrong>Body Small Strong - 12px / 500</Typography.BodySmallStrong>
    </div>
  ),
}

export const CodeVariants: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Typography.CodeLarge>Code Large - 16px / 400</Typography.CodeLarge>
      <Typography.Code>Code - 14px / 400</Typography.Code>
      <Typography.CodeInline>Code Inline - 14px / 400</Typography.CodeInline>
      <Typography.CodeInlineStrong>Code Inline Strong - 14px / 700</Typography.CodeInlineStrong>
      <Typography.CodeSmallInline>Code Small Inline - 12px / 400</Typography.CodeSmallInline>
      <Typography.CodeSmallInlineStrong>
        Code Small Inline Strong - 12px / 700
      </Typography.CodeSmallInlineStrong>
    </div>
  ),
}

export const SpecialVariants: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Typography.ButtonStrong>Button Strong - 14px / 500</Typography.ButtonStrong>
    </div>
  ),
}

export const ColorVariants: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Typography.Body variant="primary">Body - primary</Typography.Body>
      <Typography.Body variant="secondary">Body - secondary</Typography.Body>
      <Typography.Body variant="tertiary">Body - tertiary</Typography.Body>
      <Typography.Body variant="disabled">Body - disabled</Typography.Body>
      <Typography.Body variant="placeholder">Body - placeholder</Typography.Body>
      <Typography.HeadingLarge variant="secondary">
        Heading Large - secondary
      </Typography.HeadingLarge>
      <Typography.HeadingLarge variant="tertiary">Heading Large - tertiary</Typography.HeadingLarge>
      <Typography.Code>Code - default (code.inline)</Typography.Code>
      <Typography.Code variant="primary">Code - primary override</Typography.Code>
    </div>
  ),
}

export const AsPolymorphicElement: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <Typography.HeadingXLarge as="h1">HeadingXLarge as &lt;h1&gt;</Typography.HeadingXLarge>
      <Typography.HeadingLarge as="h2">HeadingLarge as &lt;h2&gt;</Typography.HeadingLarge>
      <Typography.HeadingMedium as="h3">HeadingMedium as &lt;h3&gt;</Typography.HeadingMedium>
      <Typography.Body as="p">Body as &lt;p&gt;</Typography.Body>
      <Typography.Body as="div">Body as &lt;div&gt;</Typography.Body>
      <Typography.BodyLarge as="a" href="https://example.com">
        BodyLarge as &lt;a&gt; with href
      </Typography.BodyLarge>
      <Typography.Code as="code">Code as &lt;code&gt;</Typography.Code>
      <Typography.BodySmallStrong as="label" htmlFor="example-input">
        BodySmallStrong as &lt;label&gt;
      </Typography.BodySmallStrong>
    </div>
  ),
}

const longText =
  'This is a very long text that should be truncated when the truncate prop is enabled and the container is not wide enough to display the entire content.'

export const Truncation: Story = {
  render: () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16, width: 250 }}>
      <div>
        <Typography.BodySmallStrong>Without truncate:</Typography.BodySmallStrong>
        <Typography.Body>{longText}</Typography.Body>
      </div>
      <div>
        <Typography.BodySmallStrong>With truncate:</Typography.BodySmallStrong>
        <Typography.Body truncate>{longText}</Typography.Body>
      </div>
      <div>
        <Typography.BodySmallStrong>HeadingLarge with truncate:</Typography.BodySmallStrong>
        <Typography.HeadingLarge truncate>{longText}</Typography.HeadingLarge>
      </div>
      <div>
        <Typography.BodySmallStrong>Code with truncate:</Typography.BodySmallStrong>
        <Typography.Code truncate>{longText}</Typography.Code>
      </div>
    </div>
  ),
}
