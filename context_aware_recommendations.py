import os
import json
import requests
from typing import List, Union, Dict, Optional

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"


def _normalize_modes(transport_modes: Union[str, List[str]]) -> str:
    if isinstance(transport_modes, list):
        return ",".join(str(m).strip().lower() for m in transport_modes if str(m).strip())
    return str(transport_modes).strip().lower()


def _one_sentence(text: Optional[str]) -> Optional[str]:
    if not text:
        return None

    text = str(text).strip()

    for sep in [". ", "! ", "? "]:
        if sep in text:
            first = text.split(sep)[0].strip()
            if not first.endswith((".", "!", "?")):
                first += "."
            return first

    if not text.endswith((".", "!", "?")):
        text += "."

    return text


def _hour_from_hhmm(time_hhmm: str) -> int:
    try:
        hour = int(time_hhmm.split(":")[0])
    except Exception as e:
        raise ValueError(f"Invalid time_hhmm format: {time_hhmm}") from e

    if hour < 0 or hour > 23:
        raise ValueError(f"Invalid hour in time_hhmm: {time_hhmm}")

    return hour


def _detect_attraction(destination_name: str) -> Optional[str]:
    destination = destination_name.lower()

    attraction_map = {
        "ferry building": "The Ferry Building is a popular waterfront destination with shops and food options nearby.",
        "pier 39": "Pier 39 is a well-known waterfront attraction with shops, restaurants, and sea lions nearby.",
        "golden gate bridge": "The Golden Gate Bridge is one of San Francisco’s most iconic landmarks.",
        "union square": "Union Square is a major shopping and hotel district in central San Francisco.",
        "palace of fine arts": "The Palace of Fine Arts is a famous scenic landmark worth visiting nearby.",
        "coit tower": "Coit Tower is a popular hilltop landmark with panoramic city views.",
        "alamo square": "Alamo Square is known for its scenic park and the Painted Ladies nearby.",
        "fisherman's wharf": "Fisherman's Wharf is a popular waterfront area with food and tourist attractions nearby.",
        "chinatown": "Chinatown is one of San Francisco’s most iconic neighborhoods for food and culture.",
        "golden gate park": "Golden Gate Park is one of San Francisco’s best-known green spaces with multiple attractions nearby."
    }

    for key, message in attraction_map.items():
        if key in destination:
            return message

    return None


def _detect_safety_warning(
    destination_name: str,
    time_hhmm: str,
    transport_modes: str
) -> Optional[str]:
    destination = destination_name.lower()
    modes = transport_modes.lower()
    hour = _hour_from_hhmm(time_hhmm)

    risky_areas = [
        "tenderloin",
        "civic center",
        "mid-market"
    ]

    is_night = hour >= 22 or hour <= 5
    includes_walking = "walk" in modes
    destination_is_sensitive = any(area in destination for area in risky_areas)

    if is_night and includes_walking and destination_is_sensitive:
        return "This trip includes late-night walking toward an area with variable street conditions."

    return None


def _rewrite_with_claude(
    api_key: str,
    destination_name: str,
    time_hhmm: str,
    transport_modes: str,
    attraction: Optional[str],
    safety_warning: Optional[str]
) -> Dict[str, Optional[str]]:
    prompt = f"""
You are an assistant inside a transit app for San Francisco.

Rewrite the following fields in a calm, professional, user-friendly way.

Rules:
1. Keep each value as exactly ONE short sentence.
2. Do not invent new facts.
3. Preserve the original meaning.
4. If a field is null, keep it null.
5. Return ONLY valid JSON.

Input:
{json.dumps({
    "destination_name": destination_name,
    "time_hhmm": time_hhmm,
    "transport_modes": transport_modes,
    "attraction": attraction,
    "safety_warning": safety_warning
}, ensure_ascii=False)}

Output JSON:
{{
  "attraction": "string or null",
  "safety_warning": "string or null"
}}
""".strip()

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }

    payload = {
        "model": "claude-sonnet-4-6",
        "max_tokens": 180,
        "temperature": 0.2,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    }

    response = requests.post(
        ANTHROPIC_URL,
        headers=headers,
        json=payload,
        timeout=25
    )

    if response.status_code != 200:
        return {
            "attraction": attraction,
            "safety_warning": safety_warning
        }

    data = response.json()

    try:
        text_output = data["content"][0]["text"]
        parsed = json.loads(text_output)
    except Exception:
        return {
            "attraction": attraction,
            "safety_warning": safety_warning
        }

    return {
        "attraction": _one_sentence(parsed.get("attraction")),
        "safety_warning": _one_sentence(parsed.get("safety_warning"))
    }


def get_context_recommendations(
    destination_name: str,
    time_hhmm: str,
    transport_modes: Union[str, List[str]]
) -> Dict[str, Optional[str]]:
    """
    Genera recomendaciones contextuales:
    - attraction (si aplica)
    - safety_warning (si aplica)

    Params:
        destination_name: nombre del destino (Google Maps)
        time_hhmm: hora en formato HH:MM
        transport_modes: lista o string ("walk,bus")

    Returns:
        {
            "attraction": str | None,
            "safety_warning": str | None
        }
    """

    api_key = os.environ.get("API_CLAUDE_KEY")
    modes_str = _normalize_modes(transport_modes)

    attraction = _detect_attraction(destination_name)
    safety_warning = _detect_safety_warning(destination_name, time_hhmm, modes_str)

    if not api_key:
        return {
            "attraction": attraction,
            "safety_warning": safety_warning
        }

    try:
        return _rewrite_with_claude(
            api_key=api_key,
            destination_name=destination_name,
            time_hhmm=time_hhmm,
            transport_modes=modes_str,
            attraction=attraction,
            safety_warning=safety_warning
        )
    except Exception:
        return {
            "attraction": attraction,
            "safety_warning": safety_warning
        }


def run():
    result = get_context_recommendations(
        destination_name="Tenderloin, San Francisco",
        time_hhmm="23:00",
        transport_modes="walk,bus,walk"
    )
    print(result)