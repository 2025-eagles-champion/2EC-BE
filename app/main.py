from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from . import health, graph, analytics

# FastAPI 인스턴스 생성
app = FastAPI(
    title="Blockchain Transaction Analytics API",
    description="PySpark 기반 블록체인 트랜잭션 분석 API",
    version="0.1.0"
)

# CORS 미들웨어 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 프로덕션에서는 특정 오리진만 허용하도록 변경
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 포함
app.include_router(health.router)
app.include_router(graph.router)
app.include_router(analytics.router)

@app.get("/", tags=["Root"])
async def root():
    return {
        "message": "Welcome to Blockchain Transaction Analytics API",
        "version": "0.1.0",
        "docs_url": "/docs"
    }
