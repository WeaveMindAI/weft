-- Written by hand: what this changes lives INSIDE stored JSON, which no
-- generated migration sees. A run spec no longer carries the run's length
-- (`run_class`): long runs are gone, and a run that must last past the
-- platform's cap pauses instead.

UPDATE version_run SET spec = spec - 'run_class' WHERE spec ? 'run_class';
