import type { LucideProps } from 'lucide-react'

export function BanIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M1.5 9A7.5 7.5 0 1 0 16.5 9A7.5 7.5 0 1 0 1.5 9ZM3.697 3.697 14.303 14.303"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
