from pydantic import BaseModel

class PresignedUrlRequest(BaseModel):
    object_name: str