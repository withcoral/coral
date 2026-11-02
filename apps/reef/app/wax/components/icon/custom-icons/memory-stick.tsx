import type { LucideProps } from 'lucide-react'

export function MemoryStickIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M3 4.5H15C15.83 4.5 16.5 5.17 16.5 6V10.5C16.5 11.33 15.83 12 15 12H3C2.17 12 1.5 11.33 1.5 10.5V6C1.5 5.17 2.17 4.5 3 4.5ZM6 7.5V9M9 7.5V9M12 7.5V9M3 12V13.5M6 12V13.5M9 12V13.5M12 12V13.5M15 12V13.5M1.5 8.25H2.625M15.375 8.25H16.5"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
