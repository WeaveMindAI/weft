-- Fails if any row still holds a null here.
ALTER TABLE access_grant ALTER COLUMN exclusive SET NOT NULL;

-- Fails if any row still holds a null here.
ALTER TABLE access_grant ALTER COLUMN spec SET NOT NULL;

ALTER TABLE access_grant ALTER COLUMN spec_version DROP DEFAULT;

CREATE UNIQUE INDEX access_grant_one_exclusive ON access_grant USING btree (tenant_id, service, identity, client_id, instance_id, project_id) NULLS NOT DISTINCT WHERE ((published_by_node IS NULL) AND (client_id IS NOT NULL) AND exclusive);

-- Throws away what is in access_grant.spec_json.
ALTER TABLE access_grant DROP COLUMN spec_json;

-- Throws away what is in access_grant.spec_json_version.
ALTER TABLE access_grant DROP COLUMN spec_json_version;

DROP INDEX IF EXISTS access_grant_exclusive;
