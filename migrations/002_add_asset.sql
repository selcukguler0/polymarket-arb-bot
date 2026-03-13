-- Add asset column to period_results and equity_curve for multi-asset support.
-- Existing rows default to 'BTC' (the only asset before this migration).

ALTER TABLE period_results ADD COLUMN asset TEXT NOT NULL DEFAULT 'BTC';
ALTER TABLE equity_curve ADD COLUMN asset TEXT NOT NULL DEFAULT 'BTC';
