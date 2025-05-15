from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class TransactionBase(BaseModel):
    tx_hash: str
    from_address: str
    to_address: str
    value: float
    
class TransactionDetail(TransactionBase):
    timestamp: Optional[int] = None
    gas: Optional[float] = None
    gas_price: Optional[float] = None
    
class AddressStats(BaseModel):
    address: str = Field(..., alias="from_address")
    tx_count: int
    total_value: float
    avg_value: float
    
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
