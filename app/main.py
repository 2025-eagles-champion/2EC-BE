from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.router import graph, analytics
import os

app = FastAPI(
    title="Blockchain Transaction Analytics API",
    description="PySpark 기반 블록체인 트랜잭션 분석 API",
    version="0.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(graph.router)
app.include_router(analytics.router)

@app.get("/", tags=["Root"])
async def root():
    return {
        "message": "Welcome to Blockchain Transaction Analytics API",
        "version": "0.1.0",
        "docs_url": "/docs"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)