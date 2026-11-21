// Marks a feature or a config key that only the Enterprise Edition enables.
// It matches Reef's Enterprise badge: an icon and text carry the meaning, not
// colour. It links to the Editions page, which says how to turn it on.
export const EnterpriseBadge = () => (
  <a href="/reference/editions" className="not-prose" style={{ borderBottom: 'none' }}>
    <Badge color="purple" icon="gem">
      Enterprise
    </Badge>
  </a>
)
