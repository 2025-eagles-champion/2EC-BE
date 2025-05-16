from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import date

class AddressStats(BaseModel):
    address: str
    tx_count: int
    total_value: float
    avg_value: float

class Transaction(BaseModel):
    txhash: str
    fromAddress: str
    toAddress: str
    value: float
    timestamp: Optional[int] = None

class TimePattern(BaseModel):
    hour: int
    tx_count: int

class TopAccounts(BaseModel):
    top_senders: List[Dict[str, Any]]
    top_receivers: List[Dict[str, Any]]

class AnalyticsResponse(BaseModel):
    success: bool
    data: Any
    message: Optional[str] = None

# 파생변수 모델 정의
class NodeWeightRequest(BaseModel):
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    batch_quant_weight: float = 50.0
    tx_count_weight: float = 50.0
    tx_amount_weight: float = 50.0
    
class NodeScore(BaseModel):
    address: str
    chain: str
    tier: str
    trust_score: float
    sent_tx_count: int
    recv_tx_count: int
    total_sent: float
    total_received: float
    
class TopNodesResponse(BaseModel):
    nodes: List[NodeScore]
    related_transactions: List[Transaction]
