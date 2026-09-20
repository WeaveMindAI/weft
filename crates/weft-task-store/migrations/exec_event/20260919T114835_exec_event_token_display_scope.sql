ALTER TABLE signal_token ADD COLUMN all_displays boolean DEFAULT false NOT NULL;

ALTER TABLE signal_token ADD COLUMN allowed_displays text[] DEFAULT '{}'::text[] NOT NULL;

ALTER TABLE signal_token ADD CONSTRAINT signal_token_all_displays_not_null NOT NULL all_displays;

ALTER TABLE signal_token ADD CONSTRAINT signal_token_allowed_displays_not_null NOT NULL allowed_displays;
