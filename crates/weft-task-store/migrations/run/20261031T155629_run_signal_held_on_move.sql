DROP TRIGGER signal_held_on_release ON signal;
CREATE TRIGGER signal_held_on_release AFTER UPDATE OF held_by ON signal FOR EACH ROW WHEN ((new.holds AND (old.held_by IS NOT NULL) AND (new.held_by IS DISTINCT FROM old.held_by))) EXECUTE FUNCTION signal_held_notify();
