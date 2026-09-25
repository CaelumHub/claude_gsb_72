"""Runtime-tunable node parameters with range validation and an audit trail.

A :class:`SettingsStore` owns the values of every parameter registered in
:data:`backend.config.TUNABLE_PARAMS`.  Values live in the node's ``cfg`` dict
so existing code keeps reading them via ``cfg.get(...)`` — the store only
validates, applies and records changes.

Guarantees:

* **Validation before persistence** — a batch update is checked as a whole;
  any out-of-range / non-numeric value raises :class:`SettingsError` and *no*
  value in the batch is applied or written to disk.
* **Durable** — current values and the change history are written atomically
  (see :func:`backend.storage.atomic_write_json`) so they survive restarts.
* **Audit trail** — every accepted change appends a record (timestamp, source,
  old value, new value), queryable via the API.
"""

import math
import threading
import time

from .config import TUNABLE_PARAMS
from .storage import atomic_write_json, read_json

# How many change records to keep (newest retained).
MAX_HISTORY_ENTRIES = 500


class SettingsError(ValueError):
    """Raised when a parameter update fails validation.

    The message is safe to surface directly to the UI.
    """


def _coerce(key, raw):
    """Convert a JSON/HTTP-submitted ``raw`` value to the declared type."""
    spec = TUNABLE_PARAMS[key]
    kind = spec["type"]
    # bool is a subclass of int in Python; never silently accept it as a number.
    if isinstance(raw, bool):
        raise SettingsError(f"参数 {spec['label']} 的值必须是数字")
    if kind == "int":
        if isinstance(raw, float):
            if not raw.is_integer():
                raise SettingsError(f"参数 {spec['label']} 必须是整数")
            value = int(raw)
        elif isinstance(raw, int):
            value = raw
        elif isinstance(raw, str) and raw.strip().lstrip("-").isdigit():
            value = int(raw.strip())
        else:
            raise SettingsError(f"参数 {spec['label']} 必须是整数")
    else:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            raise SettingsError(f"参数 {spec['label']} 必须是数字")
    if not math.isfinite(value):
        raise SettingsError(f"参数 {spec['label']} 不能是无穷大或 NaN")
    return value


def validate_value(key, raw):
    """Return the coerced value when ``raw`` is legal for ``key``."""
    if key not in TUNABLE_PARAMS:
        raise SettingsError(f"未知参数：{key}")
    spec = TUNABLE_PARAMS[key]
    value = _coerce(key, raw)
    lo, hi = spec["min"], spec["max"]
    if value < lo or value > hi:
        raise SettingsError(
            f"参数 {spec['label']} 的合法范围为 {lo:g} ~ {hi:g}{spec.get('unit', '')}，"
            f"提交值 {value:g} 已超出范围")
    return value


class SettingsStore:
    """Validates, applies, persists and audits runtime parameter changes."""

    def __init__(self, cfg, path):
        self.cfg = cfg
        self.path = path
        self._lock = threading.RLock()
        data = read_json(path, {}) or {}
        self.history = list(data.get("history", []))[-MAX_HISTORY_ENTRIES:]
        self._load_values(data.get("values", {}))

    # ------------------------------------------------------------------ #
    # Loading / persistence
    # ------------------------------------------------------------------ #
    def _load_values(self, saved):
        """Overlay persisted values onto cfg, ignoring any invalid legacy value."""
        for key, spec in TUNABLE_PARAMS.items():
            default = spec["default"]
            value = default
            if key in saved:
                try:
                    value = validate_value(key, saved[key])
                except SettingsError:
                    # A corrupt/out-of-range saved value must not block startup.
                    value = default
            self.cfg[spec["cfg_key"]] = value

    def _persist(self):
        atomic_write_json(self.path, {
            "values": {key: self.cfg[spec["cfg_key"]]
                       for key, spec in TUNABLE_PARAMS.items()},
            "history": self.history,
        })

    # ------------------------------------------------------------------ #
    # Read views
    # ------------------------------------------------------------------ #
    def describe(self):
        """Return each parameter's current/default value plus range metadata."""
        out = []
        for key, spec in TUNABLE_PARAMS.items():
            current = self.cfg[spec["cfg_key"]]
            default = spec["default"]
            out.append({
                "key": key,
                "label": spec["label"],
                "description": spec["description"],
                "type": spec["type"],
                "min": spec["min"],
                "max": spec["max"],
                "step": spec.get("step"),
                "unit": spec.get("unit", ""),
                "default": default,
                "current": current,
                "is_default": current == default,
            })
        return out

    def get(self, key):
        return self.cfg[TUNABLE_PARAMS[key]["cfg_key"]]

    def changes(self, limit=100):
        """Return change records, newest first."""
        limit = max(1, min(int(limit or 100), MAX_HISTORY_ENTRIES))
        return list(reversed(self.history[-limit:]))

    # ------------------------------------------------------------------ #
    # Mutations
    # ------------------------------------------------------------------ #
    def update(self, values, source="ui"):
        """Validate and apply a batch of parameter changes atomically.

        ``values`` maps external parameter keys to submitted raw values.
        Unknown keys are rejected.  Returns the list of applied
        ``{key, old, new}`` dicts.  Either all changes apply or none do.
        """
        if not isinstance(values, dict):
            raise SettingsError("参数表必须是键值对象")
        unknown = [k for k in values if k not in TUNABLE_PARAMS]
        if unknown:
            raise SettingsError("未知参数：" + ", ".join(sorted(unknown)))

        # Coerce + range-check everything before touching any live value.
        checked = {key: validate_value(key, raw)
                   for key, raw in values.items()}

        applied = []
        with self._lock:
            for key, new_value in checked.items():
                spec = TUNABLE_PARAMS[key]
                old_value = self.cfg.get(spec["cfg_key"], spec["default"])
                if new_value == old_value:
                    continue
                applied.append({
                    "key": key, "label": spec["label"],
                    "old": old_value, "new": new_value,
                    "unit": spec.get("unit", ""), "action": "update",
                })
            if not applied:
                return []
            now = time.time()
            for change in applied:
                spec = TUNABLE_PARAMS[change["key"]]
                self.cfg[spec["cfg_key"]] = change["new"]
                self.history.append({
                    "time": now, "source": str(source)[:40],
                    "action": "update",
                    "key": change["key"], "label": change["label"],
                    "old": change["old"], "new": change["new"],
                    "unit": change["unit"],
                })
            self.history = self.history[-MAX_HISTORY_ENTRIES:]
            self._persist()
        return applied

    def reset_defaults(self, source="ui"):
        """Restore every tunable parameter to its built-in default.

        Mirrors :meth:`update`: validation-free (defaults are always legal),
        one persisted history record per actually-changed parameter.
        """
        applied = []
        now = time.time()
        with self._lock:
            for key, spec in TUNABLE_PARAMS.items():
                old_value = self.cfg.get(spec["cfg_key"], spec["default"])
                if old_value == spec["default"]:
                    continue
                applied.append({
                    "key": key, "label": spec["label"],
                    "old": old_value, "new": spec["default"],
                    "unit": spec.get("unit", ""), "action": "reset",
                })
                self.cfg[spec["cfg_key"]] = spec["default"]
                self.history.append({
                    "time": now, "source": str(source)[:40],
                    "action": "reset",
                    "key": key, "label": spec["label"],
                    "old": old_value, "new": spec["default"],
                    "unit": spec.get("unit", ""),
                })
            if applied:
                self.history = self.history[-MAX_HISTORY_ENTRIES:]
                self._persist()
        return applied
