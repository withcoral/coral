// A recipe's {{input.name}} reference, shared by the format, the parser and the
// configuration step. It carries no zod, so browser code can import it.

/** Every {{input.name}} reference, for matchAll or replace; group 1 is the input name. */
export const inputReference = /\{\{input\.([a-z][a-z0-9_]{0,63})\}\}/g
