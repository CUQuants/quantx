from fastapi import FastAPI
from .routes.main_router import main_router

app = FastAPI(title="QuantX Manual Agent API", version="1.0.0")
app.include_router(main_router)

@app.get("/healthz")
def health():
    return {"ok": True}
