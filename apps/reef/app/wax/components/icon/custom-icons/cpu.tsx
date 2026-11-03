import type { LucideProps } from 'lucide-react'

export function CpuIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M4.5 3H13.5C14.33 3 15 3.67 15 4.5V13.5C15 14.33 14.33 15 13.5 15H4.5C3.67 15 3 14.33 3 13.5V4.5C3 3.67 3.67 3 4.5 3ZM6.75 6H11.25C11.66 6 12 6.34 12 6.75V11.25C12 11.66 11.66 12 11.25 12H6.75C6.34 12 6 11.66 6 11.25V6.75C6 6.34 6.34 6 6.75 6ZM5.25 1.5V3M9 1.5V3M12.75 1.5V3M5.25 15V16.5M9 15V16.5M12.75 15V16.5M1.5 5.25H3M1.5 9H3M1.5 12.75H3M15 5.25H16.5M15 9H16.5M15 12.75H16.5"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
