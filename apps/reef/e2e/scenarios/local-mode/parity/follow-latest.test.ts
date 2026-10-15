import { test } from 'vitest'
import { followLatest } from '../../main/activity/follow-latest.ts'

test(
  'Local live mode follows the newest record of a running Run and highlights its stored source line',
  followLatest,
)
