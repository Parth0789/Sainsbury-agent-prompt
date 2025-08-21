from fastapi import APIRouter, HTTPException, Depends

from crud.login import AuthHandler
from serializers.request.media import PresignedUrlRequest
from secure_payload import return_encoded_data
from utils.s3_utils import create_presigned_url


router = APIRouter()
auth_handler = AuthHandler()

@router.post("/presigned-url")
async def get_presigned_url(body: PresignedUrlRequest, token_data=Depends(auth_handler.auth_wrapper)):
    body = body.dict()
    object_name = body.get("object_name")

    res = create_presigned_url(object_name)

    if not res:
        raise HTTPException(status_code=500, detail="Error generating presigned URL")
    
    return return_encoded_data({"presigned_url": res})
