from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
from ..spark_processor import SparkProcessor
from ..models import AddressStats, AnalyticsResponse

router = APIRouter(tags=["Graph"])

# 의존성 주입을 위한 함수
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
        # 주소별 통계 가져오기
        address_stats_df = processor.get_transaction_stats().limit(limit)
        
        # Pandas로 변환하여 반환
        address_stats = processor.get_data_as_pandas(address_stats_df).to_dict(orient="records")
        
        return AnalyticsResponse(
            success=True,
            data=address_stats,
            message=f"Successfully retrieved {len(address_stats)} nodes"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/graph/node/{address}", response_model=AnalyticsResponse)
async def get_node_details(
    address: str,
    processor: SparkProcessor = Depends(get_spark_processor)
):
    try:
        # 특정 주소의 트랜잭션 데이터 가져오기
        df = processor.get_transfers_df()
        
        # 송신자 또는 수신자가 해당 주소인 경우 필터링
        node_transactions = df.filter(
            (df.from_address == address) | (df.to_address == address)
        ).limit(100)
        
        # 결과가 없는 경우 처리
        if node_transactions.count() == 0:
            return AnalyticsResponse(
                success=False,
                data=[],
                message=f"No transactions found for address {address}"
            )
        
        # Pandas로 변환하여 반환
        transactions = processor.get_data_as_pandas(node_transactions).to_dict(orient="records")
        
        return AnalyticsResponse(
            success=True,
            data=transactions,
            message=f"Successfully retrieved {len(transactions)} transactions for {address}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
