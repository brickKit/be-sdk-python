-- Document number series (protocol 14, "document numbers"): one row per series scope; the next number is taken in
-- the issuing transaction, so a rollback gives it back and continuous series leave no gaps.
CREATE TABLE be_doc_series (
    series      text PRIMARY KEY,
    last_number bigint NOT NULL CHECK (last_number >= 0),
    gapless     boolean NOT NULL
);
