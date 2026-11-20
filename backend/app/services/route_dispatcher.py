"""路由分发领域服务：按规则将变更事件路由到下游消费通道。

上游：dedup_window(去重) → event_filter(过滤) → route_dispatcher(分发)
消费：下游Kafka/MQ消费端
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.matching import ROUTE_VALUE_FIELDS, EventMatchData
from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class RouteDispatcherService(BaseModuleService):
    MODULE = "route_dispatcher"
    ENTITY = "路由规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "匹配条件", "目标通道", "分发策略", "是否广播", "权重", "备用目标"]
    REQUIRED_FIELDS = ["规则名称", "匹配条件", "目标通道"]
    STATUS_ORDER = ["草稿", "已启用", "已暂停"]
    ACTION_RULES = {"启用规则": "草稿", "暂停规则": "已启用"}
    NEGATIVE_ACTIONS = []

    def route_event(self, event: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
        """对单个事件执行路由分发。

        根据规则匹配条件和分发策略，将事件定向到目标通道。
        支持广播、加权分发和备通道降级。

        判定取值和原始事件都来自过滤入口登记的共用匹配数据：路由读到的
        始终是过滤放行当时的事件快照，即使同一自然键的后一笔事件已在
        队列里等待，也不会覆盖本笔的镜像。
        """
        data = pipeline_bus.acquire_match_data(event)
        return self._route_with_data(data, config)

    def resume_route(self, data: EventMatchData, config: dict[str, Any]) -> dict[str, Any]:
        """按已有匹配数据继续中断的路由，已完成的步骤不重复执行。

        若上次中断在审计之前，本次补写这一条审计；否则沿用首次结果。
        """
        step = self._route_step_key(config)
        if step in data.step_results and step in data.audited_steps:
            return data.step_results[step]
        return self._route_with_data(data, config)

    def _route_with_data(self, data: EventMatchData, config: dict[str, Any]) -> dict[str, Any]:
        condition = config.get("匹配条件", "")
        target = config.get("目标通道", "default_sink")
        strategy = config.get("分发策略", "round_robin")
        broadcast = config.get("是否广播", "否") == "是"
        weight = int(config.get("权重", "1"))
        backup = config.get("备用目标", "")

        if condition and not data.values.contains(condition, ROUTE_VALUE_FIELDS):
            data.route_failures += 1
            # 条件不匹配是终局判定；数据归位后可复用，不再占用待路由队列。
            pipeline_bus.complete_match_data(data)
            return {"routed": False, "reason": "condition_mismatch"}

        event_id = data.display_event_id
        route_result: dict[str, Any] = {
            "routed": True,
            "event_id": event_id,
            "rule": config.get("规则名称"),
            "strategy": strategy,
            "broadcast": broadcast,
            "routed_at": datetime.now().isoformat(),
        }

        if broadcast:
            route_result["targets"] = [target]
        else:
            route_result["targets"] = [target]
            route_result["weight"] = weight

        if backup:
            route_result["backup_target"] = backup

        # 以下副作用按进度标记续做：路由中断后继续时只补做未完成的步骤，
        # 因此登记顺序、指标计数和审计次数与一次性路由一致。
        # 多条规则命中同一事件时，每条规则仍各自登记、计数和审计一次。
        step = self._route_step_key(config)
        if step not in data.completed_steps:
            data.completed_steps.add(step)
            data.step_dispatched_count += 1
            pipeline_bus.routing_table.append({
                "name": config.get("规则名称"),
                "condition": condition,
                "target": target,
                "enabled": config.get("status") == "已启用",
            })
            pipeline_bus.pipeline_metrics["routed"] += 1
        if step not in data.audited_steps:
            pipeline_bus.write_audit(
                "路由分发", target,
                f"事件 {event_id} 路由到 {target}",
                "route_dispatcher", "已路由"
            )
            # 审计写成功后才记进度：中断在写审计之前时，继续路由会补写这一条。
            data.audited_steps.add(step)

        data.routed = True
        data.step_results[step] = route_result
        pipeline_bus.complete_match_data(data)
        return route_result

    def _route_step_key(self, config: dict[str, Any]) -> tuple[Any, ...]:
        """一条路由规则在单次事件上的副作用单位。"""
        return (
            config.get("规则名称"),
            config.get("目标通道", "default_sink"),
            config.get("匹配条件", ""),
            config.get("是否广播", "否"),
            config.get("权重", "1"),
            config.get("备用目标", ""),
        )

    def resume_event_route(self, event: dict[str, Any],
                           config: dict[str, Any]) -> dict[str, Any]:
        """路由中断后按同一份共用匹配数据继续。

        取回的是过滤当时登记的原事件快照；已发送过的规则沿用首次结果，
        不重复登记、计数或审计，因此表达式取值范围、发送顺序和审计次数
        与一次性路由一致。未命中的规则依旧判不匹配、不产生副作用。
        """
        data = pipeline_bus.acquire_match_data(event)
        condition = config.get("匹配条件", "")
        if condition and not data.values.contains(condition, ROUTE_VALUE_FIELDS):
            return {"routed": False, "reason": "condition_mismatch"}
        return self.resume_route(data, config)

    def batch_route(self, events: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
        """批量路由事件。

        按策略对事件流执行批量分发，支持负载均衡模式。
        """
        strategy = config.get("分发策略", "round_robin")
        targets = config.get("目标通道", "default").split(",")
        weight = int(config.get("权重", "1"))
        broadcast = config.get("是否广播", "否") == "是"

        by_target: dict[str, int] = {}
        for i, event in enumerate(events):
            if broadcast:
                target = targets[0]
            else:
                idx = (i // weight) % len(targets)
                target = targets[idx]
            by_target[target] = by_target.get(target, 0) + 1

        pipeline_bus.write_audit(
            "批量路由", f"{len(events)}条事件",
            f"批量路由 {len(events)} 条事件到 {len(by_target)} 个目标",
            "route_dispatcher", "已路由"
        )
        return {
            "total_routed": len(events),
            "by_target": by_target,
            "strategy": strategy,
            "broadcast": broadcast,
        }

    def compute_route_distribution(self, configs: list[dict[str, Any]]) -> dict[str, Any]:
        """计算路由分发比例的预期分布。"""
        total_weight = sum(int(c.get("权重", "1")) for c in configs if c.get("status") == "已启用")
        dist: dict[str, Any] = {}
        for c in configs:
            if c.get("status") == "已启用":
                w = int(c.get("权重", "1"))
                name = c.get("规则名称", "")
                dist[name] = {
                    "target": c.get("目标通道"),
                    "weight": w,
                    "share_pct": round(w / max(total_weight, 1) * 100, 1),
                }
        return {"total_weight": total_weight, "distribution": dist}

    def find_fallback_route(self, event: dict[str, Any], primary_target: str) -> dict[str, Any]:
        """查找备选路由。"""
        fallback = None
        for rule in pipeline_bus.routing_table:
            if rule.get("target") == primary_target:
                pass
            elif rule.get("enabled"):
                fallback = rule
                break
        if fallback:
            return {"found": True, "fallback": fallback["target"], "condition": fallback["condition"]}
        return {"found": False}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "启用规则":
            target = entry.get("目标通道", "")
            pipeline_bus.routing_table.append({
                "name": entry.get("规则名称"),
                "condition": entry.get("匹配条件"),
                "target": target,
                "enabled": True,
            })
            pipeline_bus.write_audit(
                "路由启用", target,
                f"路由规则 {entry.get('规则名称')} 已启用，指向 {target}",
                "route_dispatcher", "已启用"
            )
        return entry, msg