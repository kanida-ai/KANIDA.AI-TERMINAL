"""Standalone runner for the Agent Builder backend (local testing before dropping into the main app).
Run:  AGENT_SQLITE_FALLBACK=../db/kanida.db uvicorn app:app --port 8010
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from agent_builder.router import router as agent_builder_router

app = FastAPI(title="KANIDA Agent Builder", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(agent_builder_router, prefix="/api", tags=["Builder"])

@app.get("/")
def root(): return {"service": "kanida-agent-builder", "docs": "/docs"}
