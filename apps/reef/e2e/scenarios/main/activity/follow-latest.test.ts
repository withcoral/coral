import { test } from 'vitest'
import { followLatest } from './follow-latest.ts'

test(
  'Live mode follows the newest record of a running Run and highlights its stored source line',
  followLatest,
)
