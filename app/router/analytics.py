from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
from ..spark_processor import SparkProcessor
from ..models import AnalyticsResponse

router = APIRouter(tags=["Analytics"])

# 의존성 주입을 위한 함수
def get_spark_processor():
    processor = SparkProcessor()
    try:
        yield processor
    finally:
        processor.close()

@router.get("/analytics/time-patterns", response_model=AnalyticsResponse)
async def get_time_patterns(
    processor: SparkProcessor = Depends(get_spark_processor)
):
    try:
        # 시간별 패턴 분석
        time_patterns_df = processor.get_time_pattern_analysis()
        
        if time_patterns_df is None:
            return AnalyticsResponse(
                success=False,
                data=[],
                message="No timestamp data available for time pattern analysis"
            )
        
        # Pandas로 변환하여 반환
        time_patterns = processor.get_data_as_pandas(time_patterns_df).to_dict(orient="records")
        
        return AnalyticsResponse(
            success=True,
            data=time_patterns,
            message="Successfully retrieved time patterns"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/top-accounts", response_model=AnalyticsResponse)
async def get_top_accounts(
    limit: Optional[int] = Query(10, description="상위 계정 수"),
    processor: SparkProcessor = Depends(get_spark_processor)
):
    try:
        # 상위 계정 분석
        top_accounts_dict = processor.get_top_accounts(limit)
        
        # Pandas로 변환
        top_senders = processor.get_data_as_pandas(top_accounts_dict["top_senders"]).to_dict(orient="records")
        top_receivers = processor.get_data_as_pandas(top_accounts_dict["top_receivers"]).to_dict(orient="records")
        
        result = {
            "top_senders": top_senders,
            "top_receivers": top_receivers
        }
        
        return AnalyticsResponse(
            success=True,
            data=result,
            message=f"Successfully retrieved top {limit} accounts"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
