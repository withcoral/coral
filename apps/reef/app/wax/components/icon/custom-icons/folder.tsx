import type { LucideProps } from 'lucide-react'

export function FolderIcon({ color = 'currentColor', size = 24, ...props }: LucideProps) {
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
        d="M15 15.375C15.398 15.375 15.779 15.217 16.061 14.936C16.342 14.654 16.5 14.273 16.5 13.875V6.375C16.5 5.977 16.342 5.596 16.061 5.314C15.779 5.033 15.398 4.875 15 4.875H9.075C8.824 4.877 8.577 4.817 8.355 4.699C8.134 4.581 7.945 4.41 7.808 4.2L7.2 3.3C7.063 3.093 6.877 2.922 6.659 2.805C6.44 2.687 6.196 2.625 5.947 2.625H3C2.602 2.625 2.221 2.783 1.939 3.064C1.658 3.346 1.5 3.727 1.5 4.125V13.875C1.5 14.273 1.658 14.654 1.939 14.936C2.221 15.217 2.602 15.375 3 15.375H15Z"
        stroke={color}
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.25"
      />
    </svg>
  )
}
