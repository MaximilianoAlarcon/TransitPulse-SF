from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI()

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
        <head>
            <title>TransitPulse SF</title>
        </head>
        <body>
            <h1>TransitPulse SF 🚍</h1>
            <p>API is running</p>
        </body>
    </html>
    """