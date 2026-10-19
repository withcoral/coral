-- Each Provider row now stores its full Provider Call Root in `namespace`:
-- `coral.providers.<namespace>` for a Provider an Owner adds, `coral.workspace`
-- for the Built-in Provider. Every row written before then holds only the
-- namespace of a Provider an Owner added, so prefix it.
UPDATE provider_records
SET namespace = 'coral.providers.' || namespace;
