import type { LucideProps } from 'lucide-react'

export function UserRoundXIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M1.5 15.75V14.25C1.5 12.593 2.843 11.25 4.5 11.25H9.75M12.75 12L16.5 15.75M16.5 12L12.75 15.75M9.75 5.25C9.75 6.907 8.407 8.25 6.75 8.25C5.093 8.25 3.75 6.907 3.75 5.25C3.75 3.593 5.093 2.25 6.75 2.25C8.407 2.25 9.75 3.593 9.75 5.25Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
