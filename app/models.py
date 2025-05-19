from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Union
from datetime import date as dt

class AddressStats(BaseModel):
    address: str
    tx_count: int
    total_value: float
    avg_value: float

class Transaction(BaseModel): # TopNodesResponse 및 graph.py 엔드포인트에서 사용될 수 있음
    txhash: str                 # spark_processor에서 가공된 txhash (일부 문자열)를 받을 수 있도록 str
    fromAddress: str
    toAddress: str
    amount: float               # 'value'에서 'amount'로 변경
    timestamp: Optional[int] = None
    # CSV 및 Spark 처리 결과에 포함될 수 있는 추가 필드들
    type: Optional[str] = None
    denom: Optional[str] = None
    dpDenom: Optional[str] = None
    fromChain: Optional[str] = None
    fromChainId: Optional[str] = None # 예: 'cosmoshub-3'와 같은 문자열 ID
    toChain: Optional[str] = None
    toChainId: Optional[str] = None
    date: Optional[dt] = None     # spark_processor에서 추가하는 날짜 필드

class TimePattern(BaseModel):
    hour: int
    tx_count: int

class TopAccounts(BaseModel):
    top_senders: List[Dict[str, Any]]
    top_receivers: List[Dict[str, Any]]

# NodeWeightRequest는 변경 없음
class NodeWeightRequest(BaseModel):
    start_date: Optional[dt] = None
    end_date: Optional[dt] = None
    batch_quant_weight: float = 50.0
    tx_count_weight: float = 50.0
    tx_amount_weight: float = 50.0

class NodeScore(BaseModel): # TopNodesResponse에서 사용
    address: str                # spark_processor에서 가공된 주소 (일부 문자열)를 받을 수 있도록 str
    chain: str
    tier: str
    final_score: float          # 'trust_score'에서 'final_score'로 변경
    sent_tx_count: int
    recv_tx_count: int
    total_sent: float
    total_received: float
    # spark_processor.py의 apply_weights_and_get_top_nodes 결과에 포함될 수 있는 다른 필드들 (필요시 Optional로 추가)
    # 예: hour_entropy, active_days_count, counterparty_count_sent 등

class TopNodesResponse(BaseModel):
    nodes: List[NodeScore]
    related_transactions: List[Transaction]

class ProcessedTopNodesResponse(BaseModel): # Union의 두 번째 옵션
    nodes: List[Dict[str, Any]]
    related_transactions: List[Dict[str, Any]]

# AnalyticsResponse (Union을 사용하는 버전이 /analytics/top_nodes에 적용된다고 가정)
# models.py 파일 내에 AnalyticsResponse 클래스 정의는 하나만 존재해야 합니다.
# 만약 다른 라우터(예: graph.py)에서 data: Any를 사용하는 AnalyticsResponse가 필요하다면,
# 모델 이름을 다르게 하거나, 이 Union 버전을 모든 곳에서 사용하되 data 필드의 타입을 더 일반화해야 합니다.
# 여기서는 Union 버전이 유효하다고 가정하고 진행합니다.
class AnalyticsResponse(BaseModel):
    success: bool
    data: Union[TopNodesResponse, ProcessedTopNodesResponse]
    message: Optional[str] = None