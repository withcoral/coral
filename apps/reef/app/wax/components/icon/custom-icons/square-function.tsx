import type { LucideProps } from 'lucide-react'

export function SquareFunctionIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M3.75 2.25H14.25A1.5 1.5 0 0 1 15.75 3.75V14.25A1.5 1.5 0 0 1 14.25 15.75H3.75A1.5 1.5 0 0 1 2.25 14.25V3.75A1.5 1.5 0 0 1 3.75 2.25ZM6.75 12.75c1.5 0 2.1 -0.75 2.1 -2.1V7.5c0 -1.5 0.75 -2.475 2.4 -2.25M6.75 8.4h4.275"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
