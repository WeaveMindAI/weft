CREATE TABLE install_domain_door (
    refusals integer NOT NULL,
    refused text,
    served text[] NOT NULL,
    singleton boolean DEFAULT true NOT NULL
);

ALTER TABLE install_domain_door ADD CONSTRAINT install_domain_door_pkey PRIMARY KEY (singleton);

ALTER TABLE install_domain_door ADD CONSTRAINT install_domain_door_refusals_not_null NOT NULL refusals;

ALTER TABLE install_domain_door ADD CONSTRAINT install_domain_door_served_not_null NOT NULL served;

ALTER TABLE install_domain_door ADD CONSTRAINT install_domain_door_singleton_check CHECK (singleton);

ALTER TABLE install_domain_door ADD CONSTRAINT install_domain_door_singleton_not_null NOT NULL singleton;
