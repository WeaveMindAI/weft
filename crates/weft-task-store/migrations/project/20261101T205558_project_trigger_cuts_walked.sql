-- Added nullable: fill it before the contract part makes it NOT NULL.
ALTER TABLE project_definition ADD COLUMN trigger_cuts_walked boolean;
