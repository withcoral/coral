import type { LucideProps } from 'lucide-react'

// One path: a translucent stroke must not darken where the arrow meets its stem.
export function ArchiveRestoreIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M2.25 2.25H15.75A0.75 0.75 0 0 1 16.5 3V5.25A0.75 0.75 0 0 1 15.75 6H2.25A0.75 0.75 0 0 1 1.5 5.25V3A0.75 0.75 0 0 1 2.25 2.25ZM3 6V14.25A1.5 1.5 0 0 0 4.5 15.75H6M15 6V14.25A1.5 1.5 0 0 1 13.5 15.75H12M6.75 11.25L9 9L11.25 11.25M9 9V15.75"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
