/** Summarize one repository: stars, open issues and default branch. */
export default async function (input: { owner: string; repo: string }) {
  // No status check: a non-success response throws coral.OperationError.
  const { data: repo } = await coral.providers.github.repos.get(input);
  return {
    name: repo.full_name,
    stars: repo.stargazers_count,
    openIssues: repo.open_issues_count,
    defaultBranch: repo.default_branch,
  };
}
