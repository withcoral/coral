import type { LucideProps } from 'lucide-react'

export function CreditCardIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M1.5 7.5H16.5M4.5 10.5H6M3 3.75H15C15.828 3.75 16.5 4.422 16.5 5.25V12.75C16.5 13.578 15.828 14.25 15 14.25H3C2.172 14.25 1.5 13.578 1.5 12.75V5.25C1.5 4.422 2.172 3.75 3 3.75Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
