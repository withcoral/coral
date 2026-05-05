import * as styles from './placeholder.css'

export function Placeholder() {
  return (
    <section className={styles.placeholder} aria-label="Placeholder">
      <div className={styles.card}>
        <h1 className={styles.title}>Placeholder</h1>
        <p className={styles.message}>Coral UI coming soon</p>
      </div>
    </section>
  )
}
