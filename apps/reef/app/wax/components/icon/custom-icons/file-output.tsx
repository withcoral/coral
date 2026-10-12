import type { LucideProps } from 'lucide-react'

export function FileOutputIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M3.17 15.694A1.5 1.5 0 0 0 4.5 16.5h9a1.5 1.5 0 0 0 1.5 -1.5V6a1.8 1.8 0 0 0 -0.529 -1.28l-2.691 -2.691A1.8 1.8 0 0 0 10.5 1.5H4.5a1.5 1.5 0 0 0 -1.5 1.5v2.345M10.5 1.5v3.75a0.75 0.75 0 0 0 0.75 0.75h3.75M3.75 8.25l-2.25 2.25M3.75 12.75l-2.25 -2.25h7.5"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
