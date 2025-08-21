from pydantic import BaseModel


class UserLoginRequestModel(BaseModel):
    email: str
    password: str
