DROP TRIGGER storage_sweep_notify_on_insert ON storage_sweep;
CREATE TRIGGER storage_sweep_notify_on_insert AFTER INSERT ON storage_sweep FOR EACH STATEMENT EXECUTE FUNCTION storage_sweep_notify();
