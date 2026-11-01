-- Fails if any row still holds a null here.
ALTER TABLE project_definition ALTER COLUMN trigger_cuts_walked SET NOT NULL;
