import logging
from src.application.ports import EventFactorizerPort, EventFactorDTO
from src.domain.events import Event
from src.utils.base import extract_json_block

from openai import OpenAI

class DeepseekClient(EventFactorizerPort):
    def __init__(self, model: str, api_key: str, base_url: str = "https://api.deepseek.com"):
        self.model = model
        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self._log = logging.getLogger(__name__)

    def factorize(self, event: Event, max_tokens: int = 256, agent_role: str | None = None, indicators_context: str | None = None) -> EventFactorDTO:
        self._log.info("LLM: factorize start", extra={"event_id": event.event_id, "asset": (event.asset.symbol if getattr(event, 'asset', None) else None), "max_tokens": max_tokens})
        system_prompt = (
            "You are an AI assistant that analyzes an event and returns a JSON object. "
            "Return ONLY a single valid JSON object with exactly three keys: 'factor', 'zi_score', and 'confidence'. "
            "- 'factor': a concise summary (1–2 sentences) of the driver and its likely effect on the asset's price. Always reference the asset explicitly. "
            "- 'zi_score': an integer in [-2,-1,0,1,2] indicating expected price impact (2=strong positive, 1=moderate positive, 0=neutral, -1=moderate negative, -2=strong negative). "
            "- 'confidence': a discrete integer in [0..10] representing how confident you are in your assessment given the agent role and any provided indicators snapshot (0=very uncertain, 10=very certain). "
            "Calibrate confidence using: directness/relevance to the asset, clarity/magnitude of the driver, source credibility and recency, and alignment/consensus of indicators if provided. Under ambiguity or missing signals, lower confidence. "
            "Do not include any text outside the JSON."
        )
        # Add agent role and indicators snapshot to the system prompt if provided
        extra_lines = []
        if agent_role:
            extra_lines.append(f"Agent role: {agent_role}")
        if indicators_context:
            extra_lines.append(f"Indicators snapshot: {indicators_context}")
        if extra_lines:
            system_prompt = system_prompt + "\n\n" + "\n".join(extra_lines)

        categories = ", ".join(event.categories or [])
        asset_symbol = event.asset.symbol if getattr(event, "asset", None) else ""
        asset_line = f"Asset: {asset_symbol}\n" if asset_symbol else ""
        user_prompt = (
            f"Title: {event.title}\n"
            f"OccurredAt(UTC): {event.occurred_at.isoformat()}\n"
            f"{asset_line}"
            f"Categories: {categories}\n\n"
            f"Content:\n{event.content}"
        )
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        resp = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            stream=False,
            max_tokens=max_tokens
        )

        content = resp.choices[0].message.content if getattr(resp, "choices", None) else ""
        parsed = extract_json_block(content)

        factor = parsed.get("factor")
        zi_score = parsed.get("zi_score")
        confidence = parsed.get("confidence")

        if not isinstance(factor, str) or not factor.strip():
            print("LLM output missing valid 'factor' string")
            factor = None

        if isinstance(zi_score, (int, float, str)) and str(zi_score).strip() != "":
            try:
                zi_score = int(float(zi_score))
            except Exception:
                self._log.warning("LLM: zi_score not integer", extra={"event_id": event.event_id, "zi_score": zi_score})
                zi_score = None
        else:
            zi_score = None

        if isinstance(zi_score, int) and (zi_score < -2 or zi_score > 2):
            self._log.warning("LLM: zi_score out of range", extra={"event_id": event.event_id, "zi_score": zi_score})
            zi_score = None

        # Parse and bound 'confidence' to [0..10]
        if isinstance(confidence, (int, float, str)) and str(confidence).strip() != "":
            try:
                confidence = int(float(confidence))
            except Exception:
                self._log.warning("LLM: confidence not integer", extra={"event_id": event.event_id, "confidence": confidence})
                confidence = None
        else:
            confidence = None

        if isinstance(confidence, int) and (confidence < 0 or confidence > 10):
            self._log.warning("LLM: confidence out of range", extra={"event_id": event.event_id, "confidence": confidence})
            confidence = None

        result = EventFactorDTO(factor=(factor or "").strip(), zi_score=zi_score, confidence=confidence)
        self._log.info("LLM: factorize done", extra={"event_id": event.event_id, "has_factor": bool(result.factor), "zi_score": result.zi_score, "confidence": result.confidence})
        return result
