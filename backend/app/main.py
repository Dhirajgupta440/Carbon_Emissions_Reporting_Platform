from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .database import SessionLocal, create_tables
from .routers import analytics, emissions
from .utils import seed_sample_data


app = FastAPI(
    title="Carbon Emissions Reporting Platform",
    description="Prototype GHG reporting platform for Scope 1 and Scope 2 reporting.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup_event():
    create_tables()
    db = SessionLocal()
    try:
        seed_sample_data(db)
    finally:
        db.close()


app.include_router(emissions.router)
app.include_router(analytics.router)


@app.get("/")
def root():
    return {
        "message": "Carbon Emissions Reporting Platform API is running.",
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {"status": "healthy"}
