CREATE TABLE image_claim (
    claim_id uuid NOT NULL,
    holder_until bigint NOT NULL,
    image_ref text NOT NULL
);

ALTER TABLE image_claim ADD CONSTRAINT image_claim_claim_id_not_null NOT NULL claim_id;

ALTER TABLE image_claim ADD CONSTRAINT image_claim_holder_until_not_null NOT NULL holder_until;

ALTER TABLE image_claim ADD CONSTRAINT image_claim_image_ref_not_null NOT NULL image_ref;

ALTER TABLE image_claim ADD CONSTRAINT image_claim_pkey PRIMARY KEY (image_ref, claim_id);
