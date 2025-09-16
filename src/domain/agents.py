from dataclasses import dataclass

@dataclass(frozen=True)
class CoverageProfile:
    profile_key: str
    name: str
    role: str = ""

@dataclass(frozen=True)
class Agent:
    agent_id: str
    name: str
    coverage_profile_key: CoverageProfile
    is_active: bool = True