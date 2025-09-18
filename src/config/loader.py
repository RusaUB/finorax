from __future__ import annotations

import os
import json
from typing import Any, Dict


def load_base_config(path: str = "config/base.yaml") -> Dict[str, Any]:
    """Load configuration from YAML. If PyYAML is not installed, attempt JSON; otherwise return {}.

    Returns an empty dict if file is missing or cannot be parsed.
    """
    if not os.path.exists(path):
        return {}
    try:
        import yaml  
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            if not isinstance(data, dict):
                return {}
            return data
    except Exception:
        # Fallback: try parse as JSON
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    return {}
                return data
        except Exception:
            return {}


def conf_get(obj: Dict[str, Any] | None, path: str, default: Any = None) -> Any:
    """Deep get from config dict using dot-separated path."""
    if not obj:
        return default
    cur: Any = obj
    for part in path.split('.'):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def env_or_value(env_name: str | None, value: Any | None, default: Any | None = None) -> Any:
    if env_name:
        v = os.environ.get(env_name)
        if v is not None:
            return v
    return value if value is not None else default