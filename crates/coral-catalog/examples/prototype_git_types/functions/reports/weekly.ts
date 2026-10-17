/** Calls other shared functions through coral.shared. */
export default async function (input: { owner: string; repo: string }) {
  const summary = await coral.shared.github.repos.summary(input);
  const issues = await coral.shared.linear.myIssues({ limit: 10 });
  // Wrong: summary has no `watchers` field, and myIssues takes no `team`.
  await coral.shared.linear.myIssues({ team: "ENG" });
  return { stars: summary.stars, watchers: summary.watchers, issues: issues.length };
}
