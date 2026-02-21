"""
main.py — SchemaLens AI FastAPI Backend Entry Point
"""

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Import routes
from backend.routes import schema, quality, chat, export, watcher, connect
from backend.db import load_all_csvs


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load all CSVs into SQLite on startup."""
    print("\n" + "="*60)
    print("  SchemaLens AI — Backend Starting Up")
    print("="*60)
    print("📂 Loading Olist dataset into SQLite...")
    load_all_csvs()
    print("="*60 + "\n")
    yield
    print("\nSchemaLens AI — Shutting down.")


app = FastAPI(
    title="SchemaLens AI",
    description="Turning Raw Schema into Business Intelligence, Automatically.",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow frontend to call backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API routes
app.include_router(schema.router)
app.include_router(quality.router)
app.include_router(chat.router)
app.include_router(export.router)
app.include_router(watcher.router)
app.include_router(connect.router)


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "SchemaLens AI"}


# Serve frontend static files
frontend_dir = Path(__file__).parent.parent / "frontend"
if frontend_dir.exists():
    # Serve app.js at root level so index.html's src="app.js" resolves correctly
    @app.get("/app.js")
    def serve_appjs():
        return FileResponse(str(frontend_dir / "app.js"), media_type="application/javascript")

    # Also expose as /static/* for direct links
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

    @app.get("/")
    def serve_frontend():
        return FileResponse(str(frontend_dir / "index.html"))
