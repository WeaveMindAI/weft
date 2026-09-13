-- Throws away what is in version_run.verdict. Ship this in a later release than the one 
-- that stopped reading the column, so the old pods do not fall over.
ALTER TABLE version_run DROP COLUMN verdict;
