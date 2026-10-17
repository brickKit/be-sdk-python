-- The PostgreSQL queue transport (chapter 06, P6.16-P6.23), in the bus database's own schema. Messages are read in
-- (transaction ID, sequence) order and only below pg_snapshot_xmin: every transaction under it has ended and no new
-- one can get a smaller ID, so a reader never skips a row that commits later.
CREATE TABLE be_bus_message (
    seq          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    xid          xid8 NOT NULL DEFAULT pg_current_xact_id(),
    msg_id       text,
    subject      text NOT NULL,
    headers      jsonb NOT NULL,
    data         bytea NOT NULL,
    published_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX be_bus_message_id ON be_bus_message (msg_id) WHERE msg_id IS NOT NULL;
CREATE INDEX be_bus_message_order ON be_bus_message (xid, seq);
CREATE INDEX be_bus_message_age ON be_bus_message (published_at);

CREATE TABLE be_bus_stream (
    name       text PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE be_bus_subscription (
    durable     text PRIMARY KEY,
    streams     text[] NOT NULL,
    filters     text[] NOT NULL,
    max_deliver integer NOT NULL,
    ack_wait_ms integer NOT NULL,
    backoff_ms  integer[] NOT NULL,
    pos_xid     xid8 NOT NULL DEFAULT '0',
    pos_seq     bigint NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- one row per message a subscription still has to settle; deleted when acknowledged or given up
CREATE TABLE be_bus_delivery (
    durable     text NOT NULL REFERENCES be_bus_subscription ON DELETE CASCADE,
    seq         bigint NOT NULL,
    attempts    integer NOT NULL DEFAULT 0,
    in_flight   boolean NOT NULL DEFAULT false,
    not_before  timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz,
    lease_token uuid,
    PRIMARY KEY (durable, seq)
);
CREATE INDEX be_bus_delivery_due ON be_bus_delivery (durable, not_before) WHERE NOT in_flight;
CREATE INDEX be_bus_delivery_lease ON be_bus_delivery (durable, lease_until) WHERE in_flight;
