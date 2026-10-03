-- be-protocol 1.0 reference DDL: authorization projection (P6.12)
--
-- Created only in components that declare `resources` in assembly.yaml. Holds direct tuples only;
-- subject-side expansion happens at query time (P6.4).

CREATE TABLE IF NOT EXISTS besdk_authz_acl (
    rtype      TEXT        NOT NULL,                     -- resource type, <domain>.<name>.<aggregate>
    rid        TEXT        NOT NULL,                     -- resource id
    relation   TEXT        NOT NULL,                     -- viewer, editor, …
    subject    TEXT        NOT NULL,                     -- user:<sub> | role:<code> | dept:<path> | dept_tree:<path>
    expires_at TIMESTAMPTZ,
    revision   BIGINT      NOT NULL,                     -- provider revision of the last change
    PRIMARY KEY (rtype, rid, relation, subject)
);

CREATE INDEX IF NOT EXISTS besdk_authz_acl_subject ON besdk_authz_acl (rtype, subject, relation);

CREATE TABLE IF NOT EXISTS besdk_authz_cursor (
    scope      TEXT        PRIMARY KEY,                  -- the pulled type set, e.g. 'erp.sales.order'
    revision   BIGINT      NOT NULL,                     -- watermark: every change <= revision applied
    rebuilt_at TIMESTAMPTZ                               -- last rebuild from a ReadTuples snapshot (after 410)
);
