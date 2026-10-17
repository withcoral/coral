/** Create or update a file, and say which happened. */
export default async function (input: { owner: string; repo: string; path: string; content: string; message: string }) {
  const result = await coral.providers.github.repos.createOrUpdateFileContents({
    owner: input.owner,
    repo: input.repo,
    path: input.path,
    body: { content: input.content, message: input.message },
  });
  // Two success statuses: checking `status` narrows the result.
  const outcome: "created" | "updated" = result.status === 201 ? "created" : "updated";
  return { outcome, sha: result.data.commit.sha };
}
