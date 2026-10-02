"""be-sdk-python：Python 横切基础库（总纲 §4 SOP-L 十四项能力）。

顶层导出保持与 ``be-sdk-go`` 的 ``package besdk`` 扁平访问方式对应
——``besdk.pg_dsn(...)`` 对应 Go 的 ``besdk.PGDSN(...)``，
``besdk.with_tx(...)`` 对应 ``besdk.WithTx(...)``，以此类推。两份 SDK
的公开 API 要同名、同参数顺序、同语义（总纲 §3.5.1）。
"""

from besdk.authz import AUTHENTICATED, PUBLIC, PermKey, delete, get, patch, post, put, require_permission
from besdk.client import system_client, user_client
from besdk.connection import nats_url, pg_dsn
from besdk.events import Event, consume
from besdk.fastapi_app import new_fastapi_app
from besdk.module import Module
from besdk.outbox import publish_outbox, start_outbox_pump
from besdk.query import Query, list_window
from besdk.runtime import Config, Runtime
from besdk.scope import NO_DEPT_PATH, ScopeFilter, scope_from_claims, scope_of
from besdk.shell import (
    ShellModuleConfig,
    init_shell_authz,
    new_shell_runtime,
    serve_extra_port,
    serve_http,
)
from besdk.standalone import bootstrap, run_standalone
from besdk.tx import with_tx

__all__ = [
    "AUTHENTICATED",
    "NO_DEPT_PATH",
    "PUBLIC",
    "Config",
    "Event",
    "Module",
    "PermKey",
    "Query",
    "Runtime",
    "ScopeFilter",
    "ShellModuleConfig",
    "bootstrap",
    "consume",
    "delete",
    "get",
    "nats_url",
    "init_shell_authz",
    "list_window",
    "new_fastapi_app",
    "new_shell_runtime",
    "patch",
    "pg_dsn",
    "post",
    "publish_outbox",
    "put",
    "require_permission",
    "run_standalone",
    "scope_from_claims",
    "scope_of",
    "serve_extra_port",
    "serve_http",
    "start_outbox_pump",
    "system_client",
    "user_client",
    "with_tx",
]
