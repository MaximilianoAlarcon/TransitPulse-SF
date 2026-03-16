import anthropic
import os

client = anthropic.Anthropic(api_key=os.environ.get("API_CLAUDE_KEY"))

def transform_input_address(address: str) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=200,
        system="""
You are an expert assistant on the San Francisco public transportation system (Muni, BART, Golden Gate Transit).
Your only task is to receive a location or description from the user and return the EXACT name of the nearest or most relevant public transportation stop.
Rules:
- Respond ONLY with the stop name, without explanations or additional text.
- Use the official stop name as it appears in Google Maps.
- If there are several reasonable options, choose the most well-known or busiest one.
- If you cannot confidently identify a stop, respond: UNKNOWN

Examples:
golden gate bridge → Golden Gate Bridge Toll Plaza
ferry building → Ferry Building Terminal
airport → SFO BART Station
caltrain → Caltrain 4th & King Station
alcatraz → Pier 33 Ferry Terminal
AT&T park → 4th St & King St
    """,
        messages=[
            {"role": "user", "content": address}
        ]
    )
    return response.content[0].text.strip()