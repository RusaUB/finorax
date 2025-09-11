from typing import List
from dataclasses import dataclass, field

@dataclass(frozen=True)
class CoverageProfile:
    profile_key: str
    name: str
    description: str = ""
    categories: List[str] = field(default_factory=list)

@dataclass(frozen=True)
class Agent:
    agent_id: str
    name: str
    coverage_profile_key: CoverageProfile
    is_active: bool = True