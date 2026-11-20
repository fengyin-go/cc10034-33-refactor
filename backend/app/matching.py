"""共用匹配判定数据：事件过滤与路由分发共用的一份待判定证据。

过滤入口在放行事件时登记一条 :class:`EventMatchData`，路由入口按事件身份
（``事件ID``）取回同一条数据。事件过滤和路由分发不再各自从事件里重复抽取
表名、操作类型和完整事件文本，也不再各自保存一份"待判定事件"。

两条约束：

1. **按事件身份关联**。判定证据以事件 ID 为键，而不是以来源表或自然键为键；
   同一行连续发生的两笔变更即使自然键相同，也各持有独立的证据。
2. **原始事件保留到路由完成**。登记时对事件做快照，路由读到的是过滤放行
   当时的事件内容；后到的同自然键事件不会挤掉前一笔实际用于过滤的镜像。
"""
from __future__ import annotations

import copy
import itertools
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final

# 事件对象里可能承载身份的字段，按优先级读取。
EVENT_ID_KEYS: Final[tuple[str, ...]] = ("事件ID", "event_id", "eventId")
# 字典事件沿用旧入口读取的字段名，不做额外回退，保持表达式取值范围不变。
LEGACY_TABLE_KEYS: Final[tuple[str, ...]] = ("来源表",)
LEGACY_OPERATION_KEYS: Final[tuple[str, ...]] = ("操作类型",)
LEGACY_DATABASE_KEYS: Final[tuple[str, ...]] = ("来源数据库",)

# 各入口沿用的表达式取值范围：值元组的顺序就是旧实现的判定顺序。
FILTER_VALUE_FIELDS: Final[tuple[str, ...]] = ("event", "table", "operation")
ROUTE_VALUE_FIELDS: Final[tuple[str, ...]] = ("event", "table")
PIPELINE_VALUE_FIELDS: Final[tuple[str, ...]] = ("table", "operation", "database")


def _lookup(source: Mapping[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = source.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def detect_event_id(event: Any) -> str | None:
    """读取事件身份；没有显式事件 ID 时返回 ``None``（调用方退回直算）。"""
    if isinstance(event, Mapping):
        for key in EVENT_ID_KEYS:
            value = event.get(key)
            if value not in (None, ""):
                return str(value)
        return None
    value = getattr(event, "event_id", None)
    if value not in (None, ""):
        return str(value)
    return None


def snapshot_event(event: Any) -> Any:
    """保留事件登记当时的内容快照，避免后续镜像覆盖影响判定。"""
    try:
        return copy.deepcopy(event)
    except Exception:  # pragma: no cover - 仅防御不可深拷贝的自定义事件对象
        try:
            return copy.copy(event)
        except Exception:
            return event


@dataclass(frozen=True)
class MatchingValues:
    """表达式的取值来源：一处抽取，过滤和路由共同消费。"""

    event_text: str
    source_table: str
    operation: str
    database: str

    def value(self, field: str) -> str:
        if field == "event":
            return self.event_text
        if field == "table":
            return self.source_table
        if field == "operation":
            return self.operation
        if field == "database":
            return self.database
        return ""

    def contains(self, expression: str, fields: tuple[str, ...]) -> bool:
        """按给定取值范围做子串匹配，保持各入口原有的判定范围。"""
        return any(expression in self.value(field) for field in fields)


def build_matching_values(event: Any) -> MatchingValues:
    """从事件抽取匹配表达式需要读取的值。

    字典事件沿用 ``str(event)`` 作为完整事件文本，保持过滤/路由旧实现的
    表达式取值范围不变；变更记录对象则取其来源表、操作类型和来源库。
    """
    if isinstance(event, Mapping):
        return MatchingValues(
            event_text=str(event),
            source_table=_lookup(event, LEGACY_TABLE_KEYS),
            operation=_lookup(event, LEGACY_OPERATION_KEYS),
            database=_lookup(event, LEGACY_DATABASE_KEYS),
        )
    return MatchingValues(
        event_text=str(event),
        source_table=str(getattr(event, "source_table", "") or ""),
        operation=str(getattr(event, "operation", "") or ""),
        database=str(getattr(event, "source_db", "") or ""),
    )


def event_identity(event: Any) -> str:
    """事件身份键：优先显式事件 ID，没有时退回对象身份。"""
    event_id = detect_event_id(event)
    if event_id is not None:
        return f"event:{event_id}"
    return f"object:{id(event)}"


class MatchDataStore:
    """按事件身份暂存匹配判定数据，直到路由完成。

    同一事件 ID 可以有多条待路由数据（重复投递或重放）：路由入口按原对象
    取回自己登记的那条，因此后到的同自然键事件不会挤掉前一笔仍在等待路由的
    事件。已放行但路由中断的数据保留在待办队列里，继续路由时读到的仍是
    过滤当时的那份原始事件镜像。
    """

    MAX_COMPLETED = 256

    def __init__(self) -> None:
        self._pending: dict[str, list[EventMatchData]] = {}
        self._completed: dict[str, list[EventMatchData]] = {}
        self.staged_total = 0
        self.completed_total = 0

    def stage(self, event: Any) -> EventMatchData:
        """过滤入口登记待判定事件；同一对象重复登记时沿用原快照。"""
        identity = event_identity(event)
        for existing in self._pending.get(identity, []):
            if existing.origin is event:
                return existing
        data = EventMatchData(
            event_id=detect_event_id(event),
            identity=identity,
            event=snapshot_event(event),
            values=build_matching_values(event),
            origin=event,
        )
        self._pending.setdefault(identity, []).append(data)
        self.staged_total += 1
        return data

    def acquire(self, event: Any) -> EventMatchData:
        """路由入口取用共享数据：同一对象优先取回自己登记的那条。"""
        identity = event_identity(event)
        for entry in itertools.chain(
                self._pending.get(identity, ()), reversed(self._completed.get(identity, ()))):
            if entry.origin is event:
                return entry
        return self.stage(event)

    def complete(self, data: EventMatchData) -> None:
        """路由完成后归位：移出待办，保留有限份已完成快照供后续规则复用。"""
        pending = self._pending.get(data.identity)
        if pending:
            remaining = [item for item in pending if item is not data]
            if remaining:
                self._pending[data.identity] = remaining
            else:
                self._pending.pop(data.identity, None)
        done = self._completed.setdefault(data.identity, [])
        if any(item is data for item in done):
            return  # 重复完成不重复记账
        done.append(data)
        del done[:-self.MAX_COMPLETED]
        self.completed_total += 1

    def discard(self, data: EventMatchData) -> None:
        """丢弃不会被路由消费的匹配数据（如被过滤器拦截的事件）。"""
        pending = self._pending.get(data.identity)
        if not pending:
            return
        remaining = [item for item in pending if item is not data]
        if remaining:
            self._pending[data.identity] = remaining
        else:
            self._pending.pop(data.identity, None)

    def pending_for(self, event: Any) -> list[EventMatchData]:
        return list(self._pending.get(event_identity(event), ()))

    @property
    def pending_count(self) -> int:
        return sum(len(items) for items in self._pending.values())


@dataclass
class EventMatchData:
    """一条待判定事件的共享匹配数据。

    过滤入口填写判定结论，路由入口读取原始事件与表达式取值并记录进度。
    路由可能中断，进度字段保证重试时不会重复登记规则、重复计数或重复审计。
    """

    event_id: str | None
    identity: str
    event: Any
    values: MatchingValues
    # 登记时的原事件对象，用于按对象身份取回自己那条数据（不进快照）
    origin: Any = field(default=None, repr=False, compare=False, hash=False)
    # 过滤判定
    filter_passed: bool | None = None
    filter_matched: bool = False
    matched_expression: str = ""
    rule_name: str = ""
    filter_type: str = ""
    scope: str = ""
    # 路由进度（中断后继续时按已完成步骤续做）
    completed_steps: set[tuple[Any, ...]] = field(default_factory=set)
    audited_steps: set[tuple[Any, ...]] = field(default_factory=set)
    step_results: dict[tuple[Any, ...], dict[str, Any]] = field(default_factory=dict)
    step_dispatched_count: int = 0
    routed: bool = False
    route_failures: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def mark_filter(
        self,
        *,
        passed: bool,
        matched: bool,
        matched_expression: str = "",
        rule_name: str = "",
        filter_type: str = "",
        scope: str = "",
    ) -> None:
        self.filter_passed = passed
        self.filter_matched = matched
        self.matched_expression = matched_expression
        self.rule_name = rule_name
        self.filter_type = filter_type
        self.scope = scope

    @property
    def display_event_id(self) -> str:
        """沿用旧输出口径：只认 ``事件ID`` / ``event_id``。"""
        if isinstance(self.event, Mapping):
            return str(self.event.get("事件ID", "") or "")
        return str(getattr(self.event, "event_id", "") or "")
