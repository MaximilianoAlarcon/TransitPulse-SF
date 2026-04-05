import os
import requests
from typing import List, Union, Dict

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"


def get_context_recommendations(
    destination_name: str,
    time_hhmm: str,
    transport_modes: Union[str, List[str]]
) -> Dict[str, Union[str, None]]:
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
    if not api_key:
        raise ValueError("API_CLAUDE_KEY not found in environment variables")

    # Normalizar transport_modes
    if isinstance(transport_modes, list):
        modes_str = ",".join(transport_modes)
    else:
        modes_str = transport_modes

        prompt = f"""
        You are an assistant inside a transit app for San Francisco.

        Your job is to generate helpful contextual recommendations.

        Instructions:
        1. Suggest ONE interesting attraction or place near the destination ONLY if relevant.
        2. Provide ONE safety tip ONLY if relevant (based on time, walking, or area context).
        3. Each message MUST be exactly ONE short sentence.
        4. Be calm, professional, and not alarmist.
        5. Do NOT include multiple sentences.
        6. If something is not relevant, return null.

        Rules:
        - attraction: return ONE short sentence only if the destination is clearly associated with a known attraction, landmark, park, waterfront, museum, shopping area, or notable neighborhood.
        - safety_warning: return ONE short sentence only if the trip includes walking at night, or if the destination is an area that is widely known for variable street conditions at night.
        - If not applicable, return null.
        - Output ONLY valid JSON.

        JSON:
        {{
        "attraction": "string or null",
        "safety_warning": "string or null"
        }}

        Examples:

        Input:
        - Destination: Ferry Building
        - Time: 18:30
        - Transport modes: walk,tram,walk
        Output:
        {{"attraction":"The Ferry Building is a popular waterfront destination with shops and food options nearby.","safety_warning":null}}

        Input:
        - Destination: Tenderloin
        - Time: 23:00
        - Transport modes: walk
        Output:
        {{"attraction":null,"safety_warning":"This trip includes late-night walking toward an area with variable street conditions."}}

        Now, process this input

        Input:
        - Destination: {destination_name}
        - Time: {time_hhmm}
        - Transport modes: {modes_str}

        """

    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }

    payload = {
        "model": "claude-sonnet-4-6",
        "max_tokens": 200,
        "temperature": 0.5,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ]
    }

    response = requests.post(ANTHROPIC_URL, headers=headers, json=payload)

    if response.status_code != 200:
        raise Exception(f"Claude API error: {response.text}")

    data = response.json()

    # Extraer texto generado
    try:
        text_output = data["content"][0]["text"]
    except Exception:
        raise Exception("Unexpected Claude response format")

    # Intentar parsear JSON
    import json
    try:
        parsed = json.loads(text_output)
    except json.JSONDecodeError:
        # fallback por si Claude devuelve texto extra
        return {
            "attraction": None,
            "safety_warning": None
        }

    return {
        "attraction": parsed.get("attraction"),
        "safety_warning": parsed.get("safety_warning")
    }


def run():
    result = get_context_recommendations(
        destination_name="Tenderloin, San Francisco",
        time_hhmm="23:00",
        transport_modes="walk,bus,walk"
    )

    print(result)