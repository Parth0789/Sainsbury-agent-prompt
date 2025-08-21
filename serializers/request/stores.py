from pydantic import BaseModel

class UpdateScoAlertsData(BaseModel):
    store_id: int
    miss_scan: bool
    item_switching: bool
    item_stacking: bool
    on_scanner: bool
    in_hand: bool
    in_basket: bool
    incomplete_payment: bool
    logData: dict