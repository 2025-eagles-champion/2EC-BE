from fastapi import APIRouter
import psutil
import time
from ..spark_processor import SparkProcessor

router = APIRouter(tags=["Health"])

@router.get("/health")
async def health_check():
    # 시스템 상태 확인
    cpu_usage = psutil.cpu_percent()
    memory_info = psutil.virtual_memory()
    
    # Spark 연결 테스트
    start_time = time.time()
    try:
        processor = SparkProcessor()
        # 간단한 Spark 작업 실행
        processor.spark.sql("SELECT 1").collect()
        spark_status = "healthy"
        spark_response_time = round((time.time() - start_time) * 1000)  # ms로 변환
        processor.close()
    except Exception as e:
        spark_status = "unhealthy"
        spark_response_time = -1
        
    return {
        "status": "ok",
        "timestamp": time.time(),
        "system": {
            "cpu_usage": f"{cpu_usage}%",
            "memory_usage": f"{memory_info.percent}%",
            "available_memory": f"{round(memory_info.available / (1024 ** 3), 2)} GB"
        },
        "services": {
            "spark": {
                "status": spark_status,
                "response_time_ms": spark_response_time
            }
        }
    }
