CREATE TRIGGER trigger_activation_held_on_status AFTER UPDATE OF status ON trigger_activation FOR EACH ROW WHEN ((new.status IS DISTINCT FROM old.status)) EXECUTE FUNCTION signal_held_notify();
