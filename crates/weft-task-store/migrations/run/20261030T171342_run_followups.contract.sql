-- Fails if any row still holds a null here.
ALTER TABLE trigger_bake ALTER COLUMN source_version SET NOT NULL;

-- Throws away what is in run.cost_micro_usd.
ALTER TABLE run DROP COLUMN cost_micro_usd;
