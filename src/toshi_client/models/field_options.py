from abc import ABC
from dataclasses import dataclass
from typing import Optional


@dataclass
class Options(ABC):
    stored: bool
    indexed: Optional[bool] = None
