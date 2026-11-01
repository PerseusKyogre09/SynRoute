ALTER TABLE workspaces ADD COLUMN uncertainty_code TEXT;
ALTER TABLE workspaces ADD COLUMN uncertainty_detail TEXT;

CREATE INDEX workspaces_uncertain_idx
    ON workspaces(status, updated_at)
    WHERE status IN ('preparing', 'removing', 'uncertain');
