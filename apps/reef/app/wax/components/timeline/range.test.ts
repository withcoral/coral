import { expect, it } from 'vitest'
import {
  clampView,
  focusView,
  fullView,
  isFullView,
  minSpan,
  panView,
  viewTicks,
  zoomView,
} from './range'

it('keeps the anchor in place when it zooms', () => {
  const view = zoomView({ from: 0, to: 100 }, 100, 0.5, 25)
  expect(view).toEqual({ from: 12.5, to: 62.5 })
  // The anchor is a quarter of the way along before and after.
  expect((25 - view.from) / (view.to - view.from)).toBe(0.25)
})

it('zooms around the centre without an anchor and stops at the whole axis', () => {
  expect(zoomView({ from: 40, to: 60 }, 100, 0.5)).toEqual({ from: 45, to: 55 })
  expect(zoomView({ from: 40, to: 60 }, 100, 100)).toEqual(fullView(100))
})

it('never zooms past the shortest span', () => {
  const view = zoomView({ from: 0, to: 100 }, 100, 0)
  expect(view.to - view.from).toBeCloseTo(minSpan(100))
  expect(view.to).toBeGreaterThan(view.from)
})

it('pans until either end and keeps its span', () => {
  expect(panView({ from: 10, to: 30 }, 100, 50)).toEqual({ from: 60, to: 80 })
  expect(panView({ from: 10, to: 30 }, 100, 500)).toEqual({ from: 80, to: 100 })
  expect(panView({ from: 10, to: 30 }, 100, -500)).toEqual({ from: 0, to: 20 })
})

it('clamps a view that left the axis after the axis changed', () => {
  expect(clampView({ from: 150, to: 250 }, 120)).toEqual({ from: 20, to: 120 })
  expect(clampView({ from: -10, to: 500 }, 120)).toEqual({ from: 0, to: 120 })
})

it('shows a target with padding, and a point target with a width', () => {
  expect(focusView({ from: 40, to: 60 }, 100, 0.25)).toEqual({ from: 35, to: 65 })
  const point = focusView({ from: 50, to: 50 }, 100)
  expect(point.to).toBeGreaterThan(point.from)
  expect((point.from + point.to) / 2).toBeCloseTo(50)
  // Padding never leaves the axis.
  expect(focusView({ from: 0, to: 100 }, 100)).toEqual(fullView(100))
})

it('tells a full view from a zoomed one', () => {
  expect(isFullView(fullView(100), 100)).toBe(true)
  expect(isFullView({ from: 0, to: 99 }, 100)).toBe(false)
})

it('spaces ticks evenly across the view', () => {
  expect(viewTicks({ from: 10, to: 50 }, 5)).toEqual([10, 20, 30, 40, 50])
})
