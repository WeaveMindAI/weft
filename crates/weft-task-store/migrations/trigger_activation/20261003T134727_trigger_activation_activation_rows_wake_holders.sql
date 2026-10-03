CREATE TRIGGER trigger_activation_held_on_row AFTER INSERT OR DELETE ON trigger_activation FOR EACH ROW EXECUTE FUNCTION signal_held_notify();
