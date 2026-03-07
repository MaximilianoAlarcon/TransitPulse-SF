from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def home():
    return {
        "project": "TransitPulse SF",
        "status": "running"
    }