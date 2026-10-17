/** Deliberately wrong, to see what tsc catches. Each line should be one error. */
export default async function (input: { owner: string }) {
  const { data } = await coral.providers.github.repos.get({ owner: input.owner, repo: "x" });
  const stars: number = data.stargazer_count;            // 1. misspelled field
  const name: number = data.full_name;                   // 2. wrong type for a real field
  data.topics?.map((topic) => topic.toFixed());          // 3. topics are strings
  await coral.providers.github.repos.get({ owner: input.owner }); // 4. missing `repo`
  await coral.providers.github.repos.get({ owner: "o", repo: "r", ownr: "o" }); // 5. misspelled input
  await coral.providers.github.repos.getEverything({ owner: input.owner }); // 6. no such operation
  const write = await coral.providers.github.repos.createOrUpdateFileContents({
    owner: "o", repo: "r", path: "p", body: { content: "c", message: "m" },
  });
  if (write.status === 404) return null;                 // 7. 404 is not a success status
  return { stars, name };
}
