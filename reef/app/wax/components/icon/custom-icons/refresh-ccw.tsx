import type { LucideProps } from 'lucide-react'

export function RefreshCcwIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M15.75 9C15.75 7.20979 15.0388 5.4929 13.773 4.22703C12.5071 2.96116 10.7902 2.25 9 2.25C7.11296 2.2571 5.30173 2.99342 3.945 4.305L2.25 6M2.25 6V2.25M2.25 6H6M2.25 9C2.25 10.7902 2.96116 12.5071 4.22703 13.773C5.4929 15.0388 7.20979 15.75 9 15.75C10.887 15.7429 12.6983 15.0066 14.055 13.695L15.75 12M15.75 12H12M15.75 12L15.75 15.75"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
