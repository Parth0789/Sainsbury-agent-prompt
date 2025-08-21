from pydantic import BaseModel
from typing import Optional, List


class ApplicationStatusRequestModel(BaseModel):
    store_ids: Optional[List[int]] = None
    cam_nos: Optional[List[int]] = None
    search_query: Optional[str] = None
    search_date: Optional[str] = None
    app_status: Optional[str] = None
    tech_support: Optional[int] = 0
    tech_support_status: Optional[int] = 0
    page: int = 1
    per_page: int = 10


class StatusUpdateRequest(BaseModel):
    store_id: int
    script_name: str
    new_status: str
    date: Optional[str] = None  # Format: YYYY-MM-DD
    cam_no: Optional[int] = None  # Optional camera number filter


class AllStatusUpdateRequest(BaseModel):
    store_id: int


class TechSupportUpdateRequest(BaseModel):
    store_id: int
    script_name: str
    cam_no: Optional[int] = None  # Optional camera number filter
