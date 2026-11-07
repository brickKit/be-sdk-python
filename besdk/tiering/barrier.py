"""The write barrier (be-protocol P15.13): every insert, update or delete of a class's root or child row checks, in
the writer's own transaction, whether the row's aggregate is a member of a unit that is no longer open, and fails with
SQLSTATE BE001 (RECORD_SEALED) if so, unless the transaction is the SDK's own work on that unit (it sets
be.tier_unit, transaction-local). It is a trigger, so it holds whichever entry the write comes from. A child's trigger
first takes FOR SHARE on its root row (a child below another child finds it up the parent chain): a seal holds FOR
UPDATE on the roots it seals, so the two serialize. The function keeps the search path it was created under (the
component's schema, at its migration). The same trigger as be-sdk-go's, so a schema migrated by either SDK holds the
same barrier."""
from __future__ import annotations

from besdk.tiering.classes import Class, ident
from besdk.tiering.rows import INT2, INT4, INT8, TEXT, UUID

FUNCTION = """CREATE OR REPLACE FUNCTION be_tier_barrier() RETURNS trigger LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  cls text := TG_ARGV[0];
  kind text := TG_ARGV[1];
  root_table text := TG_ARGV[2];
  root_key text := TG_ARGV[3];
  key_type text := TG_ARGV[4];
  col text := TG_ARGV[5];
  lookup text := coalesce(TG_ARGV[6], '');  -- a child below another child: its root key up the parent chain
  rid text;
  st text;
  uid text;
BEGIN
  FOREACH rid IN ARRAY ARRAY[
      CASE WHEN TG_OP <> 'INSERT' THEN to_jsonb(OLD) ->> col END,
      CASE WHEN TG_OP <> 'DELETE' THEN to_jsonb(NEW) ->> col END] LOOP
    CONTINUE WHEN rid IS NULL;
    IF lookup <> '' THEN
      EXECUTE lookup INTO rid USING rid;
      CONTINUE WHEN rid IS NULL;  -- no parent row: the foreign key refuses the write
    END IF;
    IF kind = 'child' THEN
      EXECUTE format('SELECT 1 FROM %I WHERE %I = CAST($1 AS ' || key_type || ') FOR SHARE', root_table, root_key) USING rid;
    END IF;
    SELECT u.state, u.id::text INTO st, uid FROM be_tier_member m JOIN be_tier_unit u ON u.id = m.unit
      WHERE m.class = cls AND m.root_id = rid;
    -- the SDK's own work on this unit (release, destruction, erasure) names it in the transaction
    IF st IS NOT NULL AND st <> 'open' AND coalesce(current_setting('be.tier_unit', true), '') <> uid THEN
      RAISE EXCEPTION USING ERRCODE = 'BE001', MESSAGE = 'the record belongs to a sealed archive unit',
        DETAIL = cls || '/' || rid;
    END IF;
  END LOOP;
  RETURN COALESCE(NEW, OLD);
END $$"""

_SQL_TYPES = {UUID: "uuid", TEXT: "text", INT2: "smallint", INT4: "integer", INT8: "bigint"}


def _literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def statements(classes: list[Class]) -> list[str]:
    """The statements that install the barrier (idempotent): the function, then for every tierable class a trigger
    on its root and on each child. Raises ValueError for a root key of a type a key cannot have."""
    out = [FUNCTION]
    for c in classes:
        if not c.tierable():
            continue
        key_type = _SQL_TYPES.get(c.root.columns[c.root.pk])
        if key_type is None:
            raise ValueError(f"{c.name}: tiering: a root key of type {c.root.columns[c.root.pk]}")
        name = ident("be_tier_barrier_" + c.name.replace(".", "_").replace("-", "_"))

        def trigger(table: str, kind: str, col: str, lookup: str = "") -> list[str]:
            args = [c.name, kind, c.root.name, c.root.pk, key_type, col, *([lookup] if lookup else [])]
            t = ident(table)
            return [f"DROP TRIGGER IF EXISTS {name} ON {t}",
                    f"CREATE TRIGGER {name} BEFORE INSERT OR UPDATE OR DELETE ON {t} FOR EACH ROW "
                    f"EXECUTE FUNCTION be_tier_barrier({', '.join(_literal(a) for a in args)})"]

        out += trigger(c.root.name, "root", c.root.pk)
        for ch in c.children:
            lookup = ""
            if (parent := c.child(ch.parent)) is not None:
                lookup = (f"SELECT {c.root_expr(parent, 'p')} FROM {ident(parent.name)} p "
                          f"WHERE p.{ident(parent.table.pk)}::text = $1")
            out += trigger(ch.name, "child", ch.fk, lookup)
    return out
