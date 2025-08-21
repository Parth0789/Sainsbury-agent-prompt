from pydantic import BaseModel


class UpdateTransactionClubcard(BaseModel):
    store_id: int
    transaction_id: str
    clubcard_value: str
