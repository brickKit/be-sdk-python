"""依赖地址变量名推导。取值走 Config.endpoint（besdk/runtime.py）。"""


def env_name(dep: str, extra: str = "") -> str:
    """"mdm/customer" -> MDM_CUSTOMER_ENDPOINT；("integration/im-dingtalk","grpc") -> INTEGRATION_IM_DINGTALK_GRPC_ENDPOINT"""
    p = dep.replace("/", "_").replace("-", "_").upper()
    return f"{p}_ENDPOINT" if not extra else f"{p}_{extra.upper()}_ENDPOINT"
