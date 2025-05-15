from pydantic import BaseModel
from typing import Optional, List, Dict, Any

class AddressStats(BaseModel):
    address: str
    tx_count: int
    total_value: float
    avg_value: float

class Transaction(BaseModel):
    tx_hash: str
    from_address: str
    to_address: str
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
