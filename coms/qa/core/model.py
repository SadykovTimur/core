from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Dict
from uuid import UUID

from dataclasses_json import DataClassJsonMixin, config
from dataclasses_json.api import Json

__all__ = ["Model", "config"]


def _asdict(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {_asdict(k): _asdict(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_asdict(o) for o in obj]
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, Decimal):
        return str(obj)

    return obj


@dataclass
class Model(DataClassJsonMixin):
    def to_dict(self, encode_json: bool = True) -> Dict[str, Json]:
        dct = super().to_dict(encode_json=encode_json)
        return _asdict(dct)
