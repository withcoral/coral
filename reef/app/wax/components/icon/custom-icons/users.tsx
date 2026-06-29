import type { LucideProps } from 'lucide-react'

export function UsersIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M12 15.75V14.25C12 13.454 11.684 12.691 11.121 12.129C10.559 11.566 9.796 11.25 9 11.25H4.5C3.704 11.25 2.941 11.566 2.379 12.129C1.816 12.691 1.5 13.454 1.5 14.25V15.75M12 2.346C12.643 2.513 13.213 2.888 13.62 3.414C14.027 3.94 14.247 4.585 14.247 5.25C14.247 5.915 14.027 6.56 13.62 7.086C13.213 7.612 12.643 7.987 12 8.154M16.5 15.75V14.25C16.499 13.585 16.278 12.94 15.871 12.414C15.464 11.889 14.894 11.514 14.25 11.348M9.75 5.25C9.75 6.907 8.407 8.25 6.75 8.25C5.093 8.25 3.75 6.907 3.75 5.25C3.75 3.593 5.093 2.25 6.75 2.25C8.407 2.25 9.75 3.593 9.75 5.25Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
