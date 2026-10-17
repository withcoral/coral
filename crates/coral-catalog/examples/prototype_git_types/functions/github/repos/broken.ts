/** Deliberately wrong, to see what tsc catches. */
export default async function (input: { owner: string }) {
  // 1. Missing required input `repo`.
  const result = await coral.providers.github.repos.get({ owner: input.owner });
  // 2. Reading a field before narrowing on status: data is a union of
  //    FullRepository and BasicError.
  const branch: string = result.data.default_branch;
  // 3. Misspelled field after narrowing.
  if (result.metadata.status === 200) {
    const stars: number = result.data.stargazer_count;
    // 4. Wrong type for a real field.
    const name: number = result.data.full_name;
    return { branch, stars, name };
  }
  // 5. Operation that does not exist.
  await coral.providers.github.repos.getEverything({ owner: input.owner });
  return null;
}
