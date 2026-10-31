-- The rows already here hold 1; the contract part drops the default.
ALTER TABLE version_build ADD COLUMN inputs_version integer DEFAULT 1 NOT NULL;
