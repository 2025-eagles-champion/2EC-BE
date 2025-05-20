from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Union
from datetime import date as dt

class AddressStats(BaseModel):
    address: str
    tx_count: int
    total_value: float
    avg_value: float

class Transaction(BaseModel):
    txhash: str
    fromAddress: str
    toAddress: str
    amount: float
    timestamp: Optional[int] = None
    type: Optional[str] = None
    denom: Optional[str] = None
    dpDenom: Optional[str] = None
    fromChain: Optional[str] = None
    fromChainId: Optional[str] = None
    toChain: Optional[str] = None
    toChainId: Optional[str] = None
    date: Optional[dt] = None

class TimePattern(BaseModel):
    hour: int
    tx_count: int

class TopAccounts(BaseModel):
    top_senders: List[Dict[str, Any]]
    top_receivers: List[Dict[str, Any]]

class NodeWeightRequest(BaseModel):
    start_date: Optional[dt] = None
    end_date: Optional[dt] = None
    batch_quant_weight: float = 50.0
    tx_count_weight: float = 50.0
    tx_amount_weight: float = 50.0

class NodeScore(BaseModel):
    address: str
    chain: str
    tier: str
    final_score: float
    sent_tx_count: int
    recv_tx_count: int

class TopNodesResponse(BaseModel):
    nodes: List[NodeScore]
    related_transactions: List[Transaction]

class TopNodesDerivedFeaturesResponse(BaseModel):
    nodes_derived_features: List[Dict[str, Any]]

class RelatedTransactionsResponse(BaseModel):
    related_transactions: List[Transaction]

class RelatedTransactionsDerivedFeaturesResponse(BaseModel):
    related_transactions_derived_features: List[Dict[str, Any]]

class AnalyticsResponse(BaseModel):
    success: bool
    top_nodes: TopNodesResponse
    top_nodes_derived_features: TopNodesDerivedFeaturesResponse
    related_transactions: RelatedTransactionsResponse
    related_transactions_derived_features: RelatedTransactionsDerivedFeaturesResponse
    message: Optional[str] = None
