from typing import Dict, Any
import json
import re

def extract_json_block(text: str) -> Dict[str, Any]:
    if not text:
        raise ValueError("Empty response from LLM")

    fenced = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", text, flags=re.IGNORECASE)
    if fenced:
        return json.loads(fenced.group(1))

    text = re.sub(r"```[a-zA-Z]*", "", text)

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return json.loads(text[start : end + 1])

    return json.loads(text)