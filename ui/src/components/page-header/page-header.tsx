import type { ReactNode } from "react";
import { Typography } from "@/wax/components/typography";
import * as styles from "./page-header.css";

interface PageHeaderProps {
  title: ReactNode;
  children?: ReactNode;
}

export function PageHeader({ title, children }: PageHeaderProps) {
  return (
    <header className={styles.header}>
      <div className={styles.title}>
        {typeof title === "string" ? (
          <Typography.BodyStrong as="span" variant="secondary">{title}</Typography.BodyStrong>
        ) : title}
      </div>
      {children ? <div className={styles.actions}>{children}</div> : null}
    </header>
  );
}

export function PageHeaderButtonGroup({ children }: { children: ReactNode }) {
  return <div className={styles.buttonGroup}>{children}</div>;
}
