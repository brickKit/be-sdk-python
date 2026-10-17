"""The only way a module reaches its database (protocol 14, "database"; be-protocol chapter 05): one transaction
entry that switches to the member's role and schema with SET LOCAL and sets per-transaction timeouts; pools per
database target, owned by the launcher or the shell; start checks of the durable settings; the migrate entry."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from besdk.config import Key, Secret, Type, Values


def serve_keys() -> list[Key]:
    """The shared connection keys of chapter 02 that serve requests. The owner role and the direct migration address
    are not among them: the service process never parses the owner's credentials (P5.11)."""
    return [Key("PG_HOST", Type.STRING, required=True), Key("PG_PORT", Type.INTEGER, default=5432),
            Key("PG_DATABASE", Type.STRING, required=True), Key("PG_USER", Type.STRING, required=True),
            Key("PG_PASSWORD", Type.STRING, required=True, secret=True), Key("PG_SCHEMA", Type.STRING, required=True),
            Key("PG_POOLER_PREPARED", Type.BOOLEAN, default=True),
            Key("PG_STATEMENT_TIMEOUT", Type.DURATION, default=timedelta(seconds=5)),
            Key("PG_LOCK_TIMEOUT", Type.DURATION, default=timedelta(seconds=2)),
            Key("PG_IDLE_IN_TX_TIMEOUT", Type.DURATION, default=timedelta(seconds=10)),
            Key("PG_POOL_FOREGROUND", Type.INTEGER, default=8), Key("PG_POOL_BACKGROUND", Type.INTEGER, default=4)]


def migrate_keys() -> list[Key]:
    """What the migrate entry reads: the serve keys plus the direct address and the owner role."""
    return [*serve_keys(), Key("PG_MIGRATION_HOST", Type.STRING), Key("PG_MIGRATION_PORT", Type.INTEGER),
            Key("PG_OWNER_USER", Type.STRING), Key("PG_OWNER_PASSWORD", Type.STRING, secret=True)]


@dataclass(frozen=True)
class Target:
    """A database: members with the same target share one physical pool in a shell (protocol 14)."""

    host: str
    port: int
    database: str

    def __str__(self) -> str:
        return f"{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class Timeouts:
    """Set in every transaction (never per role: after SET ROLE PostgreSQL ignores ALTER ROLE ... SET)."""

    statement: float
    lock: float
    idle_in_tx: float


@dataclass(frozen=True)
class Settings:
    target: Target
    user: str  # the member's runtime role (the login role when running alone)
    password: Secret | None  # re-read from its file when rotated
    schema: str
    prepared: bool  # false behind a transaction-mode pooler without protocol-level prepared statements
    timeouts: Timeouts
    foreground: int  # the most connections each side may hold
    background: int


def settings_from(v: Values) -> Settings:
    return Settings(Target(v.string("PG_HOST"), v.int("PG_PORT"), v.string("PG_DATABASE")), v.string("PG_USER"),
                    v.secret("PG_PASSWORD"), v.string("PG_SCHEMA"), v.bool("PG_POOLER_PREPARED"),
                    Timeouts(v.duration("PG_STATEMENT_TIMEOUT").total_seconds(), v.duration("PG_LOCK_TIMEOUT").total_seconds(),
                             v.duration("PG_IDLE_IN_TX_TIMEOUT").total_seconds()),
                    v.int("PG_POOL_FOREGROUND"), v.int("PG_POOL_BACKGROUND"))
