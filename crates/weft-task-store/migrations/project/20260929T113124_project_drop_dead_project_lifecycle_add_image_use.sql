-- Throws away what is in project.accepting_fires. Ship this in a later release than the one 
-- that stopped reading the column, so the old instances do not fall over.
ALTER TABLE project DROP COLUMN accepting_fires;

-- Throws away what is in project.activating_ts_execution_id. Ship this in a later release than the one 
-- that stopped reading the column, so the old instances do not fall over.
ALTER TABLE project DROP COLUMN activating_ts_execution_id;

-- Throws away what is in project.activation_program. Ship this in a later release than the one 
-- that stopped reading the column, so the old instances do not fall over.
ALTER TABLE project DROP COLUMN activation_program;

-- Throws away what is in project.activation_version. Ship this in a later release than the one 
-- that stopped reading the column, so the old instances do not fall over.
ALTER TABLE project DROP COLUMN activation_version;

-- Throws away what is in project.deactivated_by_health. Ship this in a later release than the one 
-- that stopped reading the column, so the old instances do not fall over.
ALTER TABLE project DROP COLUMN deactivated_by_health;

-- Throws away what is in project.drain_deadline_unix. Ship this in a later release than the one 
-- that stopped reading the column, so the old instances do not fall over.
ALTER TABLE project DROP COLUMN drain_deadline_unix;

-- Throws away what is in project.fires_deadline_unix. Ship this in a later release than the one 
-- that stopped reading the column, so the old instances do not fall over.
ALTER TABLE project DROP COLUMN fires_deadline_unix;

-- Throws away what is in project.fires_visible_to_consumers. Ship this in a later release than the one 
-- that stopped reading the column, so the old instances do not fall over.
ALTER TABLE project DROP COLUMN fires_visible_to_consumers;
