"""两种调用身份——对应 be-sdk-go 的 client.go。

⚠️ **一处与 Go 版的必要差异，不是随意分叉**：Go 的 ``UserClient(ctx, dep,
extra)`` 从 ``ctx`` 里用 ``metadata.FromIncomingContext`` 隐式取出当前
gRPC/HTTP 请求携带的 Authorization——这依赖 Go 的 ``context.Context``
在整条调用链上被显式传递的约定。FastAPI 的请求上下文不是通过一个
显式传递的 ``ctx`` 参数携带的，而是从 ``Request`` 对象或依赖注入拿。
**所以 Python 版把 auth token 变成显式参数**，调用方（多半是一个
FastAPI 依赖）负责从 ``Request.headers`` 取出来传进来。这条差异写在
这里，不是漏读 Go 版的签名。
"""

from __future__ import annotations

import grpc
from grpc.aio import Channel, ClientCallDetails, UnaryUnaryClientInterceptor

from besdk.endpoint import endpoint

_AUTH_HEADER_KEY = "authorization"


class _ForwardAuthInterceptor(UnaryUnaryClientInterceptor):
    """把调用方传入的 auth 值附到每一次出站调用的 metadata 上。"""

    def __init__(self, auth: str) -> None:
        self._auth = auth

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        if self._auth:
            metadata = list(client_call_details.metadata or [])
            metadata.append((_AUTH_HEADER_KEY, self._auth))
            client_call_details = ClientCallDetails(
                method=client_call_details.method,
                timeout=client_call_details.timeout,
                metadata=metadata,
                credentials=client_call_details.credentials,
                wait_for_ready=client_call_details.wait_for_ready,
            )
        return await continuation(client_call_details, request)


def user_client(auth: str, dep: str, extra: str = "") -> Channel:
    """拨一条到 ``dep`` 的 gRPC 连接，把 ``auth``（调用方请求里的
    Authorization）透传给下游——下游按调用者身份做数据权限过滤
    （设计书 §14.2.3）。

    ⚠️ 只许出现在用户请求路径上。查内部批量数据、后台任务、事件 handler
    一律用 :func:`system_client`——这两个名字的区别就是安全边界（导读
    第 21 条：这是"悄悄读到别人数据"的第三条路径，用错了不报错，返回的
    数据只是"多了一些"）。
    """
    target, ok = endpoint(dep, extra)
    if not ok:
        raise RuntimeError(f"besdk.user_client: 依赖 {dep} 的地址未注入")
    return grpc.aio.insecure_channel(
        target, interceptors=[_ForwardAuthInterceptor(auth)]
    )


def system_client(dep: str, extra: str = "") -> Channel:
    """拨一条到 ``dep`` 的 gRPC 连接，不透传任何调用者身份——下游会把它
    当成组件自身发起的调用，数据权限被绕过（设计书 §14.2.6）。只许出现
    在 ``Module.start`` 与事件 handler 里，``make gates`` 扫用户请求路径
    上的误用。
    """
    target, ok = endpoint(dep, extra)
    if not ok:
        raise RuntimeError(f"besdk.system_client: 依赖 {dep} 的地址未注入")
    return grpc.aio.insecure_channel(target)
