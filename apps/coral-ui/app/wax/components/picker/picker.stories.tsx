import type { Meta, StoryObj } from '@storybook/react-vite'

import { useState } from 'react'

import {
  buildSeverityPickerOptions,
  DETECTION_SOURCE_PICKER_OPTIONS,
  STATUS_PICKER_OPTIONS,
} from '@/wax/components/picker/default-options'

import { Picker } from './picker'

const meta: Meta<typeof Picker> = {
  component: Picker,
  title: 'Wax/Picker',
}

export default meta
type Story = StoryObj<typeof Picker>

const mockOrgSettings = {
  severity_levels: [
    { aliases: ['P1', 'Critical'], label: 'SEV1', order: 0 },
    { aliases: ['P2', 'High'], label: 'SEV2', order: 1 },
    { aliases: ['P3', 'Medium'], label: 'SEV3', order: 2 },
    { aliases: ['P4', 'Low'], label: 'SEV4', order: 3 },
  ],
}

export const Default: Story = {
  render: function Render() {
    const [status, setStatus] = useState('OPEN')
    const [severity, setSeverity] = useState('SEV2')
    const [detectionSource, setDetectionSource] = useState('INTERNAL')
    const severityOptions = buildSeverityPickerOptions(mockOrgSettings)

    return (
      <div style={{ display: 'flex', gap: '24px' }}>
        <Picker onChange={setStatus} options={STATUS_PICKER_OPTIONS} value={status} />
        <Picker onChange={setSeverity} options={severityOptions} value={severity} />
        <Picker
          onChange={setDetectionSource}
          options={DETECTION_SOURCE_PICKER_OPTIONS}
          value={detectionSource}
        />
      </div>
    )
  },
}

export const WithLabel: Story = {
  render: function Render() {
    const [status, setStatus] = useState('OPEN')
    const [severity, setSeverity] = useState('SEV2')
    const severityOptions = buildSeverityPickerOptions(mockOrgSettings)

    return (
      <div style={{ display: 'flex', gap: '24px' }}>
        <Picker
          label="Status"
          onChange={setStatus}
          options={STATUS_PICKER_OPTIONS}
          value={status}
        />
        <Picker
          label="Severity"
          onChange={setSeverity}
          options={severityOptions}
          value={severity}
        />
      </div>
    )
  },
}

/** Different severity level configurations */
export const SeverityConfigurations: Story = {
  render: function Render() {
    const [severity3, setSeverity3] = useState('Critical')
    const [severity4, setSeverity4] = useState('SEV1')
    const [severity6, setSeverity6] = useState('P2')

    const threeLevel = {
      severity_levels: [
        { aliases: [], label: 'Critical', order: 0 },
        { aliases: [], label: 'Major', order: 1 },
        { aliases: [], label: 'Minor', order: 2 },
      ],
    }
    const fourLevel = {
      severity_levels: [
        { aliases: [], label: 'SEV0', order: 0 },
        { aliases: [], label: 'SEV1', order: 1 },
        { aliases: [], label: 'SEV2', order: 2 },
        { aliases: [], label: 'SEV3', order: 3 },
      ],
    }
    const sixLevel = {
      severity_levels: [
        { aliases: [], label: 'P1', order: 0 },
        { aliases: [], label: 'P2', order: 1 },
        { aliases: [], label: 'P3', order: 2 },
        { aliases: [], label: 'D1', order: 3 },
        { aliases: [], label: 'D2', order: 4 },
        { aliases: [], label: 'D3', order: 5 },
      ],
    }

    return (
      <div style={{ display: 'flex', gap: '24px' }}>
        <Picker
          onChange={setSeverity3}
          options={buildSeverityPickerOptions(threeLevel)}
          value={severity3}
        />
        <Picker
          onChange={setSeverity4}
          options={buildSeverityPickerOptions(fourLevel)}
          value={severity4}
        />
        <Picker
          onChange={setSeverity6}
          options={buildSeverityPickerOptions(sixLevel)}
          value={severity6}
        />
      </div>
    )
  },
}

/** A row of pickers reading as a labelled form, with one option set unavailable. */
export const LabelOutsideAndDisabled: Story = {
  render: function Render() {
    const [client, setClient] = useState('claude-code')

    return (
      <div style={{ display: 'flex', gap: '16px', width: '540px' }}>
        <Picker
          fullWidth
          label="MCP client"
          labelPlacement="outside"
          onChange={setClient}
          options={[
            { label: 'Claude Code', value: 'claude-code' },
            { label: 'Codex', value: 'codex' },
            { label: 'Cursor', value: 'cursor' },
          ]}
          value={client}
        />
        <Picker
          disabled
          fullWidth
          label="Workspace"
          labelPlacement="outside"
          options={[{ label: 'production', value: 'production' }]}
          value="production"
        />
      </div>
    )
  },
}
