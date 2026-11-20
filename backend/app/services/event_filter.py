"""事件过滤领域服务：对变更事件执行黑白名单过滤。

上游：change_event + format_adapter 产出已适配事件 → event_filter 过滤
下游：route_dispatcher 消费过滤后的事件进行路由分发
审计：audit_trail 记录过滤操作
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.matching import FILTER_VALUE_FIELDS
from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class EventFilterService(BaseModuleService):
    MODULE = "event_filter"
    ENTITY = "过滤规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "过滤类型", "匹配表达式", "是否白名单", "优先级", "生效范围", "备注说明"]
    REQUIRED_FIELDS = ["规则名称", "过滤类型", "匹配表达式"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用规则": "草稿", "停用规则": "已启用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def apply_filter(self, event: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """对单个变更事件应用过滤规则。

        支持白名单(放行匹配)和黑名单(拦截匹配)模式，
        按优先级排序评估多条规则。

        放行时把本次判定登记为共用匹配数据，路由入口按事件身份取用同一份
        证据，不再从事件里另行抽取取值。
        """
        rule_name = config.get("规则名称", "")
        filter_type = config.get("过滤类型", "黑名单")
        expression = config.get("匹配表达式", "")
        is_whitelist = config.get("是否白名单", "否") == "是"
        priority = int(config.get("优先级", "0"))
        scope = config.get("生效范围", "全部")

        data = pipeline_bus.stage_match_data(event)
        matched = data.values.contains(expression, FILTER_VALUE_FIELDS)

        if matched and scope != "全部":
            if scope == "指定表" and expression not in data.values.source_table:
                matched = False
            elif scope == "指定操作" and expression not in data.values.operation:
                matched = False

        passed = matched if is_whitelist else not matched

        pipeline_bus.filter_rules.append({
            "name": rule_name,
            "type": filter_type,
            "expression": expression,
            "is_whitelist": is_whitelist,
            "priority": priority,
            "enabled": config.get("status") == "已启用",
        })

        data.mark_filter(
            passed=passed,
            matched=matched,
            matched_expression=expression if matched else "",
            rule_name=rule_name,
            filter_type=filter_type,
            scope=scope,
        )

        if not passed:
            pipeline_bus.pipeline_metrics["filtered"] += 1
            pipeline_bus.write_audit(
                "事件过滤", rule_name,
                f"事件被过滤规则 {rule_name} 拦截",
                "event_filter", "已过滤"
            )

        # 被拦截的事件不会进入路由，无需保留原始事件快照。
        if not passed:
            pipeline_bus.match_store.discard(data)

        return {
            "event_id": event.get("事件ID", ""),
            "rule": rule_name,
            "filter_type": filter_type,
            "matched": matched,
            "passed": passed,
            "applied_at": datetime.now().isoformat(),
        }

    def batch_filter(self, events: list[dict[str, Any]], configs: list[dict[str, Any]]) -> dict[str, Any]:
        """批量过滤事件流。"""
        enabled_rules = sorted(
            [c for c in configs if c.get("status") == "已启用"],
            key=lambda c: int(c.get("优先级", "0")), reverse=True
        )
        passed: list[dict[str, Any]] = []
        blocked = 0
        for event in events:
            is_pass = True
            for rule in enabled_rules:
                filter_result = self.apply_filter(event, rule)
                if not filter_result["passed"]:
                    is_pass = False
                    break
            if is_pass:
                passed.append(event)
            else:
                blocked += 1
        return {
            "total": len(events),
            "passed": len(passed),
            "blocked": blocked,
            "pass_rate_pct": round(len(passed) / max(len(events), 1) * 100, 1),
        }

    def compute_filter_effectiveness(self, rule_name: str) -> dict[str, Any]:
        """计算过滤规则有效性。"""
        total_filtered = pipeline_bus.pipeline_metrics["filtered"]
        total_input = pipeline_bus.pipeline_metrics["total_input"]
        return {
            "rule": rule_name,
            "total_filtered": total_filtered,
            "total_input": total_input,
            "filter_rate_pct": round(total_filtered / max(total_input, 1) * 100, 1),
        }

    def validate_expression(self, expression: str, sample_event: dict[str, Any]) -> dict[str, Any]:
        """验证过滤表达式。"""
        event_str = str(sample_event)
        matched = expression in event_str
        return {
            "expression": expression,
            "syntax_valid": len(expression) > 0,
            "matches_sample": matched,
            "sample_used": sample_event.get("来源表", "none"),
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用规则":
            pipeline_bus.filter_rules.append({
                "name": entry.get("规则名称"),
                "type": entry.get("过滤类型"),
                "expression": entry.get("匹配表达式"),
                "is_whitelist": entry.get("是否白名单", "否") == "是",
                "priority": int(entry.get("优先级", "0")),
                "enabled": True,
            })
            pipeline_bus.write_audit(
                "过滤启用", entry.get("规则名称", ""),
                f"过滤规则 {entry.get('规则名称')} 已启用",
                "event_filter", "已启用"
            )
        return entry, msg