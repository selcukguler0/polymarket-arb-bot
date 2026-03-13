-- Add run_id columns so period/equity history can be linked back to a run manifest.

ALTER TABLE period_results ADD COLUMN run_id TEXT;
ALTER TABLE equity_curve ADD COLUMN run_id TEXT;
