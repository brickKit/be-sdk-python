-- Destruction approvals (P15.29): of one unit, or standing for a class until withdrawn.
CREATE TABLE be_tier_approval (
    id          uuid PRIMARY KEY,
    scope_kind  text NOT NULL CHECK (scope_kind IN ('unit', 'class')),
    scope       text NOT NULL,                       -- a unit id or a class
    approved_by text NOT NULL,
    reason      text NOT NULL,
    approved_at timestamptz NOT NULL DEFAULT now(),
    withdrawn_at timestamptz,
    withdrawn_by text
);
CREATE INDEX be_tier_approval_live ON be_tier_approval (scope_kind, scope) WHERE withdrawn_at IS NULL;
