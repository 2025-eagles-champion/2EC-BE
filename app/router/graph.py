from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
from spark_processor import SparkProcessor
from models import AnalyticsResponse
from datetime import date

router = APIRouter(tags=["Graph"])

def get_spark_processor():
    processor = SparkProcessor()
    try:
        yield processor
    finally:
        processor.close()

@router.get("/graph/nodes", response_model=AnalyticsResponse)
async def get_all_nodes(
    limit: Optional[int] = Query(100, description="최대 노드 수 제한"),
    processor: SparkProcessor = Depends(get_spark_processor)
):
    try:
        address_stats_df = processor.get_transaction_stats().limit(limit)
        address_stats = processor.get_data_as_pandas(address_stats_df).to_dict(orient="records")
        return AnalyticsResponse(success=True, data=address_stats, message=f"Retrieved {len(address_stats)} nodes")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/graph/node/{address}", response_model=AnalyticsResponse)
async def get_node_details(
    address: str,
    processor: SparkProcessor = Depends(get_spark_processor)
):
    try:
        df = processor.get_transfers_df()
        node_transactions = df.filter(
            (df.fromAddress == address) | (df.toAddress == address)
        ).limit(100)
        if node_transactions.count() == 0:
            return AnalyticsResponse(success=False, data=[], message="No transactions found")
        transactions = processor.get_data_as_pandas(node_transactions).to_dict(orient="records")
        return AnalyticsResponse(success=True, data=transactions, message=f"Retrieved {len(transactions)} transactions")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/graph/transactions", response_model=AnalyticsResponse)
async def get_filtered_transactions(
    start_date: Optional[date] = Query(None, description="시작 날짜 (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="종료 날짜 (YYYY-MM-DD)"),
    processor: SparkProcessor = Depends(get_spark_processor)
):
    try:
        # 날짜 필터링 적용
        df = processor.get_transfers_df(start_date=start_date, end_date=end_date)
        
        # 결과 처리
        transactions = processor.get_data_as_pandas(df).to_dict(orient="records")
        
        return AnalyticsResponse(
            success=True,
            data=transactions,
            message=f"Found {len(transactions)} transactions between {start_date} and {end_date}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/graph/filter", response_model=AnalyticsResponse)
async def get_filtered_transactions(
    start_date: Optional[date] = Query(None, description="시작 날짜 (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="종료 날짜 (YYYY-MM-DD)"),
    chain: Optional[str] = Query(None, description="체인 필터 (예: cosmos)"),
    processor: SparkProcessor = Depends(get_spark_processor)
):
    try:
        filtered_df = processor.get_transfers_df(
            start_date=start_date, 
            end_date=end_date,
            chain_filter=chain
        )
        
        transactions = processor.get_data_as_pandas(filtered_df).to_dict(orient="records")
        
        return AnalyticsResponse(
            success=True,
            data=transactions,
            message=f"Found {len(transactions)} transactions matching criteria"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    