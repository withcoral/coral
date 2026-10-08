import type { LucideProps } from 'lucide-react'

export function PlugIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M9 16.5V12.75M11.25 6V1.5M12.75 6C12.949 6 13.14 6.079 13.28 6.22C13.421 6.36 13.5 6.551 13.5 6.75V9.75C13.5 10.546 13.184 11.309 12.621 11.871C12.059 12.434 11.296 12.75 10.5 12.75H7.5C6.704 12.75 5.941 12.434 5.379 11.871C4.816 11.309 4.5 10.546 4.5 9.75V6.75C4.5 6.551 4.579 6.36 4.72 6.22C4.86 6.079 5.051 6 5.25 6H12.75ZM6.75 6V1.5"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
