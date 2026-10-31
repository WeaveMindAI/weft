ALTER TABLE trigger_activation ADD COLUMN activating_listened boolean DEFAULT false NOT NULL;

ALTER TABLE trigger_activation ADD COLUMN activating_since_unix bigint;

ALTER TABLE trigger_activation ADD COLUMN activating_step text;

ALTER TABLE trigger_activation ADD COLUMN driver_task uuid;
