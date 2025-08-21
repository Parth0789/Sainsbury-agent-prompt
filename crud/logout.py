import jwt

from fastapi import HTTPException, Security

from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from passlib.context import CryptContext

from datetime import datetime, timedelta

from sqlalchemy import func, and_, distinct, desc, or_, true, false, asc, extract, text

from model.model import Blacklist_JWT

from sqlalchemy.orm import Session, aliased

import uuid

from database import SessionLocal


def check_logout_user(jti):
    db = SessionLocal()

    existing_row = db.query(Blacklist_JWT).filter_by(jti=jti).first()

    return existing_row


def logout_user(db: Session, token_data):
    jti = token_data['jti']

    created_at = datetime.utcnow()

    expired_at = datetime.utcfromtimestamp(token_data['exp'])

    is_blacklisted = check_logout_user(jti)

    if not is_blacklisted:

        new_row = Blacklist_JWT(

            jti=jti,

            created_at=created_at,

            expired_at=expired_at

        )

        db.add(new_row)

        db.commit()

        return True


    else:

        return False