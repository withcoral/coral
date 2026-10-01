import type { Meta, StoryObj } from '@storybook/react-vite'

import { BarChart } from '../bar-chart'
import { LineChart } from '../line-chart'
import { Sparkline } from '../sparkline'

// --- BarChart ---

const barMeta: Meta<typeof BarChart> = {
  component: BarChart,
  title: 'Wax/Charts/BarChart',
}

export default barMeta

type BarStory = StoryObj<typeof BarChart>

export const BarBasic: BarStory = {
  args: {
    data: [
      { label: 'Mon', value: 120 },
      { label: 'Tue', value: 200 },
      { label: 'Wed', value: 150 },
      { label: 'Thu', value: 80 },
      { label: 'Fri', value: 240 },
    ],
  },
}

export const BarWithColors: BarStory = {
  args: {
    data: [
      { color: 'green', label: 'Healthy', value: 842 },
      { color: 'amber', label: 'Degraded', value: 123 },
      { color: 'red', label: 'Error', value: 45 },
    ],
  },
}

export const BarManyBars: BarStory = {
  args: {
    data: [
      { label: 'Jan', value: 142 },
      { label: 'Feb', value: 215 },
      { label: 'Mar', value: 98 },
      { label: 'Apr', value: 176 },
      { label: 'May', value: 63 },
      { label: 'Jun', value: 231 },
      { label: 'Jul', value: 189 },
      { label: 'Aug', value: 112 },
      { label: 'Sep', value: 247 },
      { label: 'Oct', value: 84 },
      { label: 'Nov', value: 155 },
      { label: 'Dec', value: 203 },
    ],
    width: 600,
  },
}

// --- LineChart ---

export const LineSingleSeries: StoryObj<typeof LineChart> = {
  args: {
    labels: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
    series: [{ color: 'blue', data: [120, 200, 150, 300, 250, 180, 220], label: 'Requests' }],
  },
  render: (args) => <LineChart {...args} />,
}

export const LineMultiSeries: StoryObj<typeof LineChart> = {
  args: {
    labels: ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00'],
    series: [
      { color: 'blue', data: [45, 52, 120, 180, 150, 90], label: 'p50' },
      { color: 'amber', data: [120, 140, 350, 500, 400, 200], label: 'p95' },
      { color: 'red', data: [200, 250, 800, 1200, 900, 350], label: 'p99' },
    ],
  },
  render: (args) => <LineChart {...args} />,
}

export const LineWithLabels: StoryObj<typeof LineChart> = {
  args: {
    labels: ['Q1', 'Q2', 'Q3', 'Q4'],
    series: [{ color: 'green', data: [10, 25, 18, 32], label: 'Revenue' }],
  },
  render: (args) => <LineChart {...args} />,
}

// --- Sparkline ---

export const SparklineDefault: StoryObj<typeof Sparkline> = {
  args: {
    data: [10, 15, 8, 22, 18, 25, 20, 30, 28, 35],
  },
  render: (args) => <Sparkline {...args} />,
}

export const SparklineColors: StoryObj<typeof Sparkline> = {
  render: () => (
    <div style={{ alignItems: 'center', display: 'flex', gap: 16 }}>
      <Sparkline color="blue" data={[10, 15, 8, 22, 18, 25, 20]} />
      <Sparkline color="green" data={[5, 12, 8, 15, 20, 18, 25]} />
      <Sparkline color="red" data={[30, 25, 28, 20, 15, 18, 10]} />
      <Sparkline color="amber" data={[10, 20, 15, 25, 20, 30, 25]} />
    </div>
  ),
}

export const SparklineVaryingData: StoryObj<typeof Sparkline> = {
  render: () => (
    <div style={{ alignItems: 'center', display: 'flex', gap: 16 }}>
      <Sparkline data={[1, 1, 1, 5, 20, 50, 100]} width={120} />
      <Sparkline color="red" data={[100, 80, 60, 40, 20, 10, 5]} width={120} />
    </div>
  ),
}
