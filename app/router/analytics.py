# app/routers/analytics.py

from fastapi import APIRouter, HTTPException, Query, Depends, Body # Body 추가
from typing import Optional
from datetime import date
from spark_processor import SparkProcessor # 경로 수정 (app 폴더 기준)
from models import AnalyticsResponse, NodeWeightRequest # 경로 수정

router = APIRouter(tags=["Analytics"])

def get_spark_processor():
    processor = SparkProcessor()
    try:
        yield processor
    finally:
        processor.close()

# 이전 답변에서 GET/POST 분기 처리를 위해 수정한 라우트 핸들러 사용 권장
@router.api_route("/analytics/top_nodes", methods=["POST"], response_model=AnalyticsResponse)
async def get_top_weighted_nodes_route( # 함수 이름 변경 (예시)
    # GET 요청 파라미터
    start_date: Optional[date] = Query(None, description="시작 날짜 (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="종료 날짜 (YYYY-MM-DD)"),
    batch_quant_weight: float = Query(50.0, description="배치/퀀트 가중치"),
    tx_count_weight: float = Query(50.0, description="거래 횟수 가중치"),
    tx_amount_weight: float = Query(50.0, description="거래량 가중치"),
    # POST 요청 본문
    request_body: Optional[NodeWeightRequest] = Body(None),
    processor: SparkProcessor = Depends(get_spark_processor)
):
    params: NodeWeightRequest
    if request_body: # POST 요청인 경우
        params = request_body
    else: # GET 요청인 경우 Query 파라미터 사용
        params = NodeWeightRequest(
            start_date=start_date,
            end_date=end_date,
            batch_quant_weight=batch_quant_weight,
            tx_count_weight=tx_count_weight,
            tx_amount_weight=tx_amount_weight
        )

    try:
        # 가중치 기반 상위 노드 및 관련 거래 가져오기
        # spark_processor.get_top_weighted_nodes는 이미 가공된 Python dict 리스트를 반환
        processed_top_nodes_list, processed_related_transactions_list = processor.get_top_weighted_nodes(
            start_date=params.start_date,
            end_date=params.end_date,
            batch_quant_weight=params.batch_quant_weight,
            tx_count_weight=params.tx_count_weight,
            tx_amount_weight=params.tx_amount_weight
        )

        # 응답 데이터 구성: "top_nodes" 키를 "nodes"로 변경
        response_data = {
            "nodes": processed_top_nodes_list, # 키 변경!
            "related_transactions": processed_related_transactions_list
        }

        # FastAPI는 response_data를 AnalyticsResponse.data (Union[TopNodesResponse, ProcessedTopNodesResponse])에 맞춰 검증
        return AnalyticsResponse(
            success=True,
            data=response_data, # 이 data가 TopNodesResponse 또는 ProcessedTopNodesResponse로 유효해야 함
            message=f"Retrieved top {len(processed_top_nodes_list)} nodes based on weighted criteria"
        )

    except Exception as e:
        import traceback
        current_traceback = traceback.format_exc()
        print(f"Error in /analytics/top_nodes: {str(e)}\n{current_traceback}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}. Check logs for details.")
