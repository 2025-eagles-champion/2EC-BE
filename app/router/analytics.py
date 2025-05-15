from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
from spark_processor import SparkProcessor
from models import AnalyticsResponse

router = APIRouter(tags=["Analytics"])

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
        time_patterns_df = processor.get_time_pattern_analysis()
        if time_patterns_df is None:
            return AnalyticsResponse(success=False, data=[], message="No timestamp data available")
        time_patterns = processor.get_data_as_pandas(time_patterns_df).to_dict(orient="records")
        return AnalyticsResponse(success=True, data=time_patterns, message="Retrieved time patterns")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/top-accounts", response_model=AnalyticsResponse)
async def get_top_accounts(
    limit: Optional[int] = Query(10, description="상위 계정 수"),
    processor: SparkProcessor = Depends(get_spark_processor)
):
    try:
        top_accounts_dict = processor.get_top_accounts(limit)
        top_senders = processor.get_data_as_pandas(top_accounts_dict["top_senders"]).to_dict(orient="records")
        top_receivers = processor.get_data_as_pandas(top_accounts_dict["top_receivers"]).to_dict(orient="records")
        result = {
            "top_senders": top_senders,
            "top_receivers": top_receivers
        }
        return AnalyticsResponse(success=True, data=result, message=f"Retrieved top {limit} accounts")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
