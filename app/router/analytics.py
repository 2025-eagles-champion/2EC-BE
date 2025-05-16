from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional
from datetime import date
from spark_processor import SparkProcessor
from models import AnalyticsResponse, NodeWeightRequest, TopNodesResponse

router = APIRouter(tags=["Analytics"])

def get_spark_processor():
    processor = SparkProcessor()
    try:
        yield processor
    finally:
        processor.close()

@router.post("/analytics/top_nodes", response_model=AnalyticsResponse)
@router.get("/analytics/top_nodes", response_model=AnalyticsResponse)
async def get_top_weighted_nodes(
    request: NodeWeightRequest,
    processor: SparkProcessor = Depends(get_spark_processor)
):
    try:
        # 가중치 기반 상위 노드 및 관련 거래 가져오기
        top_nodes_df, related_txs_df = processor.get_top_weighted_nodes(
            start_date=request.start_date,
            end_date=request.end_date,
            batch_quant_weight=request.batch_quant_weight,
            tx_count_weight=request.tx_count_weight,
            tx_amount_weight=request.tx_amount_weight
        )
        
        # 데이터프레임을 Python 객체로 변환
        top_nodes = processor.get_data_as_pandas(top_nodes_df).to_dict(orient="records")
        related_txs = processor.get_data_as_pandas(related_txs_df).to_dict(orient="records")
        
        # 응답 데이터 구성
        response_data = {
            "top_nodes": top_nodes,
            "related_transactions": related_txs
        }
        
        return AnalyticsResponse(
            success=True, 
            data=response_data, 
            message=f"Retrieved top {len(top_nodes)} nodes based on weighted criteria"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
