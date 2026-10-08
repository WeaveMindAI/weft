-- Written by hand: what this changes is which worker a row names, which no
-- generated migration sees. A run parked whole on a wait no longer names
-- the worker that parked it (`weft_task_store::tasks::release_execution_id_owner`),
-- so the sweep for runs whose worker went away (`lost_runs`) can tell it
-- from a run still driven when its worker died. Every run already parked
-- lets go of its worker here, the way it would have when it parked.

UPDATE execution ec SET owner_replica = NULL
WHERE ec.ended_at_unix IS NULL
  AND ec.owner_replica IS NOT NULL
  AND EXISTS (SELECT 1 FROM signal parked WHERE parked.execution_id = ec.execution_id AND parked.is_resume)
  AND NOT EXISTS (SELECT 1 FROM task t WHERE t.execution_id = ec.execution_id AND t.status = 'claimed');
