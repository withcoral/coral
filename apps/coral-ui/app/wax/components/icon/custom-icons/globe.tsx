import type { LucideProps } from 'lucide-react'

export function GlobeIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M16.5 9C16.5 13.142 13.142 16.5 9 16.5M16.5 9C16.5 4.858 13.142 1.5 9 1.5M16.5 9H1.5M9 16.5C4.858 16.5 1.5 13.142 1.5 9M9 16.5C7.074 14.478 6 11.792 6 9C6 6.208 7.074 3.522 9 1.5M9 16.5C10.926 14.478 12 11.792 12 9C12 6.208 10.926 3.522 9 1.5M1.5 9C1.5 4.858 4.858 1.5 9 1.5"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
