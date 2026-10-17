/** True when the repository exists; a 404 is an answer, not a failure. */
export default async function (input: { owner: string; repo: string }): Promise<boolean> {
  try {
    await coral.providers.github.repos.get(input);
    return true;
  } catch (error) {
    if (error instanceof coral.OperationError && error.status === 404) return false;
    throw error;
  }
}
