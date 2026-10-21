import type { LucideProps } from 'lucide-react'

export function ShieldBanIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M15 9.75C15 13.5 12.375 15.375 9.255 16.463C9.091 16.518 8.914 16.515 8.752 16.455C5.625 15.375 3 13.5 3 9.75V4.5C3 4.086 3.336 3.75 3.75 3.75C5.25 3.75 7.125 2.85 8.43 1.71C8.749 1.438 9.251 1.438 9.57 1.71C10.883 2.858 12.75 3.75 14.25 3.75C14.664 3.75 15 4.086 15 4.5V9.75ZM5.625 6L12.375 12"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
