from fastapi import APIRouter, HTTPException, Query, Depends, Body
from typing import Optional
from datetime import date
from spark_processor import SparkProcessor
from models import AnalyticsResponse, NodeWeightRequest, TopNodesResponse, TopNodesDerivedFeaturesResponse, RelatedTransactionsResponse, RelatedTransactionsDerivedFeaturesResponse, NodeScore, Transaction
from pydantic import ValidationError

router = APIRouter(tags=["Analytics"])

def get_spark_processor():
    processor = SparkProcessor()
    try:
        yield processor
    finally:
        processor.close()

@router.api_route("/analytics/top_nodes", methods=["POST"], response_model=AnalyticsResponse)
async def get_top_weighted_nodes_route(
    start_date: Optional[date] = Query(None, description="시작 날짜 (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="종료 날짜 (YYYY-MM-DD)"),
    batch_quant_weight: float = Query(50.0, description="배치/퀀트 가중치"),
    tx_count_weight: float = Query(50.0, description="거래 횟수 가중치"),
    tx_amount_weight: float = Query(50.0, description="거래량 가중치"),
    request_body: Optional[NodeWeightRequest] = Body(None),
    processor: SparkProcessor = Depends(get_spark_processor)
):
    params: NodeWeightRequest
    if request_body:
        params = request_body
    else:
        params = NodeWeightRequest(
            start_date=start_date,
            end_date=end_date,
            batch_quant_weight=batch_quant_weight,
            tx_count_weight=tx_count_weight,
            tx_amount_weight=tx_amount_weight
        )

    try:
        top_nodes, top_nodes_derived_features, related_transactions, related_txs_derived_features = processor.get_top_weighted_nodes(
            start_date=params.start_date,
            end_date=params.end_date,
            batch_quant_weight=params.batch_quant_weight,
            tx_count_weight=params.tx_count_weight,
            tx_amount_weight=params.tx_amount_weight
        )

        # 데이터 파싱 및 모델 검증
        try:
            top_nodes_parsed = [NodeScore(**node) for node in top_nodes]
            related_transactions_parsed = [Transaction(**tx) for tx in related_transactions]

            top_nodes_response = TopNodesResponse(
                nodes=top_nodes_parsed,
                related_transactions=related_transactions_parsed
            )
        except ValidationError as e:
            print(f"Validation Error for TopNodesResponse: {e}")
            raise HTTPException(status_code=500, detail=f"Validation error in top_nodes: {e}")

        try:
            top_nodes_derived_features_response = TopNodesDerivedFeaturesResponse(
                nodes_derived_features=top_nodes_derived_features
            )
        except ValidationError as e:
            print(f"Validation Error for TopNodesDerivedFeaturesResponse: {e}")
            raise HTTPException(status_code=500, detail=f"Validation error in top_nodes_derived_features: {e}")

        try:
            related_transactions_response = RelatedTransactionsResponse(
                related_transactions=[Transaction(**tx) for tx in related_transactions]
            )
        except ValidationError as e:
            print(f"Validation Error for RelatedTransactionsResponse: {e}")
            raise HTTPException(status_code=500, detail=f"Validation error in related_transactions: {e}")

        try:
            related_transactions_derived_features_response = RelatedTransactionsDerivedFeaturesResponse(
                related_transactions_derived_features=related_txs_derived_features
            )
        except ValidationError as e:
            print(f"Validation Error for RelatedTransactionsDerivedFeaturesResponse: {e}")
            raise HTTPException(status_code=500, detail=f"Validation error in related_txs_derived_features: {e}")

        # print(f"Top nodes: {top_nodes_response.nodes[0] if top_nodes_response.nodes else 'No nodes'}\n")
        # print(f"Top nodes derived features: {top_nodes_derived_features_response.nodes_derived_features}\n")
        # print(f"Related transactions: {related_transactions_response.related_transactions[0] if related_transactions_response.related_transactions else 'No transactions'}\n")
        # print(f"Related transactions derived features: {related_transactions_derived_features_response.related_transactions_derived_features}\n")
        
        return AnalyticsResponse(
            success=True,
            top_nodes=top_nodes_response,
            top_nodes_derived_features=top_nodes_derived_features_response,
            related_transactions=related_transactions_response,
            related_transactions_derived_features=related_transactions_derived_features_response,
            message="Retrieved top nodes based on weighted criteria"
        )

    except Exception as e:
        import traceback
        current_traceback = traceback.format_exc()
        print(f"Error in /analytics/top_nodes: {str(e)}\n{current_traceback}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}. Check logs for details.")
