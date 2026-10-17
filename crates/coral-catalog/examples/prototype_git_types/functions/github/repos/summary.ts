/** Summarize one repository: stars, open issues and default branch. */
export default async function (input: { owner: string; repo: string }) {
  const result = await coral.providers.github.repos.get(input);
  if (result.metadata.status !== 200) {
    throw new Error(`GitHub returned ${result.metadata.status}`);
  }
  const repo = result.data;
  return {
    name: repo.full_name,
    stars: repo.stargazers_count,
    openIssues: repo.open_issues_count,
    defaultBranch: repo.default_branch,
  };
}
