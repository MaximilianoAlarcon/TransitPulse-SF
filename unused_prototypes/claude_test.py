import anthropic
import os
import json

client = anthropic.Anthropic(
    api_key=os.environ.get("API_CLAUDE_KEY"),
    timeout=30.0,
    max_retries=2,
)

try:
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=50,
        messages=[{"role": "user", "content": "Say hello"}],
    )
    print(response)
except Exception as e:
    print(type(e).__name__, str(e))

def rewrite_text(text: str) -> dict:
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=300,
        system="""
        You are a transit UX writing assistant.

        Your task is to rewrite a trip itinerary in clear, friendly, professional English for end users.

        Important rules:
        - Do not invent any facts.
        - Use only the data provided.
        - Do not change route names, stop names, times, payment methods, or travel durations.
        - If some information is missing, omit it naturally.
        - Keep the wording concise and easy to scan on mobile.
        - Sound helpful, calm, and confident.
        - Do not use marketing language.
        - Do not mention that you are an AI.
        - If the trip has multiple legs, explain them in order.
        - If payment information is provided, explain it clearly and naturally.
        - If the payment timing is provided, explicitly say whether the rider pays before boarding or on board.
        - If the trip includes walking, mention it briefly and naturally.
        - If there is a transfer, make it clear where it happens.
        - Never add warnings, delays, prices, or accessibility claims unless explicitly provided.

        Return ONLY valid JSON with this schema:

        {
          "headline": "string",
          "trip_summary": "string",
          "payment_explanation": "string",
          "steps": [
            {
              "title": "string",
              "description": "string"
            }
          ],
          "closing_note": "string"
        }
        """,
        messages=[
            {"role": "user", "content": text}
        ]
    )

    raw_text = "".join(
        block.text for block in response.content if hasattr(block, "text")
    ).strip()

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        print("⚠️ JSON inválido:", raw_text)
        return {
            "headline": "",
            "trip_summary": raw_text,
            "payment_explanation": "",
            "steps": [],
            "closing_note": ""
        }


def run():
    # Ejemplos de uso
    samples = [
        """
            Destination: Oakland

            Duration: 25 min

            Start time: 18:27

            End time: 18:52

            Path

            18:27 - 18:33 🚶

            Walk from Origin to Civic Center / UN Plaza for 5 min

            18:33 - 18:48 🚇

            Take the subway Millbrae/SF Int'l Airport SFO to Richmond : Red-N from Civic Center / UN Plaza  to 12th Street / Oakland City Center for 15 min

            The ticket is paid before boarding the transport.

            Payment method:

            Credit/Debit Card

            18:48 - 18:52 🚶

            Walk from 12th Street / Oakland City Center to Destination for 4 min
        """,
        """
        Destionation: Chinatown San Francisco

        Duration: 24 min

        Start time: 18:43

        End time: 19:07

        Path

        18:43 - 19:07 🚶

        Walk from Origin to Destination for 24 min
        """,
        """
        Destination: Palo Alto Airport
        
        Duration: 40 min

        Start time: 18:45

        End time: 19:25

        Path

        18:45 - 19:25 🚗

        Drive from Origin to Destination for 40 min
        """
    ]
    for sample in samples:
        response = rewrite_text(sample)
        print(response)