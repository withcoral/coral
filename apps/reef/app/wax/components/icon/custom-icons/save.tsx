import type { LucideProps } from 'lucide-react'

export function SaveIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M11.4 2.25a1.5 1.5 0 0 1 1.05 0.45l2.85 2.85a1.5 1.5 0 0 1 0.45 1.05V14.25a1.5 1.5 0 0 1 -1.5 1.5H3.75a1.5 1.5 0 0 1 -1.5 -1.5V3.75a1.5 1.5 0 0 1 1.5 -1.5 zM12.75 15.75v-5.25a0.75 0.75 0 0 0 -0.75 -0.75H6a0.75 0.75 0 0 0 -0.75 0.75v5.25M5.25 2.25v3a0.75 0.75 0 0 0 0.75 0.75h5.25"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
