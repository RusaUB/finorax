from dataclasses import dataclass
from typing import Protocol

from src.domain.rounds import RoundEvaluation


@dataclass
class SaveRoundResult:
    inserted_round: int
    updated_round: int
    inserted_scores: int
    updated_scores: int
    total_scores: int


class RoundRepository(Protocol):
    def save_evaluation(self, evaluation: RoundEvaluation) -> SaveRoundResult: ...