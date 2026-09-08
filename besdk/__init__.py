"""be-sdk-python：Python 横切基础库（总纲 §4 SOP-L 十四项能力）。

顶层导出保持与 ``be-sdk-go`` 的 ``package besdk`` 扁平访问方式对应
——``besdk.endpoint(...)`` 对应 Go 的 ``besdk.Endpoint(...)``，
``besdk.with_tx(...)`` 对应 ``besdk.WithTx(...)``，以此类推。两份 SDK
的公开 API 要同名、同参数顺序、同语义（总纲 §3.5.1）。
"""

from besdk.authz import PUBLIC, PermKey, delete, get, patch, post, put, require_permission
from besdk.client import system_client, user_client
from besdk.endpoint import endpoint, must_endpoint, storage_endpoint
from besdk.events import Event, consume
from besdk.fastapi_app import new_fastapi_app
from besdk.module import Module
from besdk.outbox import publish_outbox, start_outbox_pump
from besdk.query import Query, list_window
from besdk.runtime import Config, Runtime
from besdk.scope import ScopeFilter, scope_of
from besdk.standalone import bootstrap, run_standalone
from besdk.tx import with_tx

__all__ = [
    "PUBLIC",
    "Config",
    "Event",
    "Module",
    "PermKey",
    "Query",
    "Runtime",
    "ScopeFilter",
    "bootstrap",
    "consume",
    "delete",
    "endpoint",
    "get",
    "list_window",
    "must_endpoint",
    "new_fastapi_app",
    "patch",
    "post",
    "publish_outbox",
    "put",
    "require_permission",
    "run_standalone",
    "scope_of",
    "start_outbox_pump",
    "storage_endpoint",
    "system_client",
    "user_client",
    "with_tx",
]
