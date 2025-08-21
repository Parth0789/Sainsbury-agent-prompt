from typing import Optional, List
from fastapi import UploadFile, File

from pydantic import BaseModel


class UpdateSupportTicketRequestModel(BaseModel):
    description: Optional[str]
    email_config_id: Optional[int]
    group_id: Optional[int]
    priority: Optional[int]
    requester_id: Optional[int]
    responder_id: Optional[int]
    source: Optional[int]
    status: Optional[int]
    subject: Optional[str]
    product_id: Optional[int]
    attachments: Optional[List[str]]
    tags: Optional[List[str]]
    due_by: Optional[str]
    fr_due_by: Optional[str]


class CreateSupportTicketRequestModel(BaseModel):
    email: str
    cc_emails: Optional[List[str]]
    fwd_emails: Optional[List[str]]
    reply_cc_emails: Optional[List[str]]
    email_config_id: Optional[int]
    group_id: Optional[int]
    priority: int = 1
    responder_id: Optional[int]
    source: Optional[int] = 2
    status: Optional[int] = 2
    subject: str
    company_id: Optional[int]
    id: Optional[int]
    type: Optional[str]
    to_emails: Optional[List[str]]
    product_id: Optional[int]
    fr_escalated: Optional[bool]
    spam: Optional[bool]
    urgent: Optional[bool]
    is_escalated: Optional[bool]
    created_at: Optional[str]
    updated_at: Optional[str]
    due_by: Optional[str]
    fr_due_by: Optional[str]
    description_text: Optional[str]
    description: str
    attachments: Optional[List[str]]
    tags: Optional[List[str]]


class CreateTicketReplyRequestModel(BaseModel):
    ticket_id: int
    body: str
    from_email: Optional[str] = None
    user_id: int
    cc_emails: Optional[List[str]] = None
    bcc_emails: Optional[List[str]] = None
    attachments: Optional[List[UploadFile]] = File(None)
