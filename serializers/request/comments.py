from pydantic import BaseModel


class UpdateCommentRequestModel(BaseModel):
    body: str
    id: int
    user_email: str
