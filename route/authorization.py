import base64

from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.orm import Session

from database import get_db
from crud.login import AuthHandler
from services.authorization import UsersService


router = APIRouter()
auth_handler = AuthHandler()


@router.get("/sso-login", status_code=status.HTTP_200_OK)
async def saml_login(url: str = None, db: Session = Depends(get_db)):
    if url is None:
        return HTTPException(status_code=404, detail="Url not found")

    email = base64.b64decode(url).decode()
    user_service = UsersService(db, email)
    user = user_service.get_user_details()

    result = None
    if not user:
        try:
            result = user_service.create_user(email, "admin")
        except Exception as e:
            raise HTTPException(status_code=404, detail=str(e))

    if result:
        user = user_service.get_user_details()

    if user:
        role = user.roles
        token = {
            "access_token": auth_handler.access_encode_token(email, role),
            "refresh_token": auth_handler.refresh_encode_token(email, role)
        }

        roles = UsersService.set_user_roles_permission(role)

        return {"tokens": token, "roles": roles, "email": email}

    return HTTPException(status_code=404, detail="User not found")
