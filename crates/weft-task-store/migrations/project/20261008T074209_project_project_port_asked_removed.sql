-- Throws away what is in project.api_port_asked. Ship this in a later release than the one 
-- that stopped reading the column, so replicas still on the old release do not fall over.
ALTER TABLE project DROP COLUMN api_port_asked;
