-- Written by hand (a route's stored address gained the project's id after
-- the tenant, which changes values in rows and no generated migration
-- sees): `/<tenant>/<path>` becomes `/<tenant>/<project id>/<path>`, so
-- each project has its own path space on the install's shared address.
UPDATE signal
SET mount_path = '/' || tenant_id || '/' || project_id::text || substr(mount_path, length(tenant_id) + 2)
WHERE mount_path IS NOT NULL
  AND mount_path NOT LIKE '/' || tenant_id || '/' || project_id::text || '%';
