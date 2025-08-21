from datetime import datetime
from pydantic import BaseModel
from typing import Optional,Union, List


class UpdateTransactionRequestModel(BaseModel):
    transaction_id: str
    description: Union[str, None] = None
    clubcard: Union[str, None] = None


class UploadTransactionDetailsRequestModel(BaseModel):
    store_ids: Optional[List[int]] = []
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    nudge_type: str = "all"
    

class UploadTransactionToDbRequestModel(BaseModel):
    transaction_id: str
    store_id: int
    description: str = None
    clubcard: str
    nudge_type: str = "all"


class UploadTransactionSkipRequestModel(BaseModel):
    transaction_id: str


class UploadTransactionStatusDetailsRequestModel(BaseModel):
    store_ids: Optional[List[int]] = []
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    nudge_type: str = "all"
    