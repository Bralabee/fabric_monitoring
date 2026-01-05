"""
FastAPI Main Application

Interactive guide API for USF Fabric Monitoring System.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.api import scenarios, search, progress
from app.content.loader import load_all_scenarios


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load scenarios on startup."""
    app.state.scenarios = load_all_scenarios()
    yield


app = FastAPI(
    title="USF Fabric Monitoring Guide",
    description="Interactive step-by-step guide for Microsoft Fabric Monitoring & Governance System",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS configuration for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(scenarios.router, prefix="/api/scenarios", tags=["Scenarios"])
app.include_router(search.router, prefix="/api/search", tags=["Search"])
app.include_router(progress.router, prefix="/api/progress", tags=["Progress"])


@app.get("/")
async def root():
    """Health check and welcome endpoint."""
    return {
        "message": "USF Fabric Monitoring Interactive Guide API",
        "version": "1.0.0",
        "docs": "/docs",
        "status": "healthy",
    }


@app.get("/api/health")
async def health_check():
    """Health check endpoint for monitoring."""
    return {"status": "healthy", "service": "guide-api"}
