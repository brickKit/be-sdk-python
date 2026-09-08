# be-sdk-python 不是 brickKit 组件，但仍按总纲 §I 的 9 个门禁目标写——
# 一致性帮 AI：新开会话看到一份陌生 Makefile，不用先猜它和组件仓库的
# Makefile 是不是同一套规矩。对照 be-sdk-go 的 Makefile 逐条抄。
.PHONY: check-version test image migrate-idempotent dag-check contract-check \
        import-scan smoke module-check all

check-version: ## N/A：非组件仓库
	@echo "N/A：非组件仓库，没有 component.yaml"

test: ## pytest -v（含 asyncio 测试）
	python -m pytest -v

image: ## N/A：纯横切库
	@echo "N/A：纯横切库，没有可执行文件，不产出部署镜像"

migrate-idempotent: ## N/A：非组件仓库
	@echo "N/A：非组件仓库，没有迁移"

dag-check: ## 包依赖图无环（Python 没有编译期强制，用 import 探测循环 import）
	@python -c "import besdk" && echo "✓ besdk 包本身无循环 import"

contract-check: ## N/A：非组件仓库
	@echo "N/A：非组件仓库，没有 contracts/"

import-scan: ## ⚠️ 铁律六白名单本体：不许依赖任何组件仓库
	@bad=$$(grep -rlE "from (mdm|erp|crm|infra|integration|hrm|prj|ana)[_.]" besdk/ 2>/dev/null || true); \
	if [ -n "$$bad" ]; then \
		echo "✗ be-sdk-python 不许依赖任何组件仓库：$$bad"; exit 1; \
	fi; \
	echo "✓ 零组件依赖"

smoke: ## N/A：非组件仓库
	@echo "N/A：非组件仓库，没有 brickkit up 的对象"

module-check: ## N/A：非组件仓库
	@echo "N/A：非组件仓库，没有 module.new 契约"

all: check-version test image migrate-idempotent dag-check contract-check import-scan smoke module-check

help: ## 列出全部目标
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
