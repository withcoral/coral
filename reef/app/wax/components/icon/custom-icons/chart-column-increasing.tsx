import type { LucideProps } from 'lucide-react'

export function ChartColumnIncreasingIcon({
  color = 'currentColor',
  size = 24,
  ...props
}: LucideProps) {
  return (
    <svg
      fill="none"
      height={size}
      viewBox="0 0 18 18"
      width={size}
      xmlns="http://www.w3.org/2000/svg"
      {...props}
    >
      <path
        d="M9.75 12.75V6.75M13.5 12.75V3.75M2.25 2.25V14.25C2.25 14.648 2.408 15.029 2.689 15.311C2.971 15.592 3.352 15.75 3.75 15.75H15.75M6 12.75V10.5"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
