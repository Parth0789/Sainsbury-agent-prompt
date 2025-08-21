import requests
import json
from typing import List, Optional
from typing_extensions import Annotated
from pprint import pprint
from fastapi import APIRouter, status, HTTPException, UploadFile, File, Form
from fastapi.params import Depends
from fastapi.security import HTTPBearer

import config
from crud.login import (AuthHandler)
from serializers.request.support_ticket import (
    UpdateSupportTicketRequestModel
)

router = APIRouter()
security = HTTPBearer()
auth_handler = AuthHandler()

BASE_URL = config.FRESHDESK_API_BASE_URL
API_KEY = config.FRESHDESK_API_KEY


@router.get("/search/tickets", status_code=status.HTTP_200_OK)
def get_all_support_tickets(ticket_status: int = 2, page: int = 1, per_page: int = 30,
                            token_data=Depends(auth_handler.auth_wrapper)):
    header = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
    params = {
        "query": f'"status:{ticket_status} AND tag:sainsbury"'
    }

    try:
        response = requests.get(f'{BASE_URL}/api/v2/search/tickets', headers=header, params=params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if response.status_code != 200:
        print(response.json())
        return {"data": [], "count": 0}

    try:
        response_data = response.json()
        return {
            "data": response_data["results"][per_page * (page - 1):per_page * page], "count": response_data["total"]
        }
    except (KeyError, json.JSONDecodeError) as e:
        raise HTTPException(status_code=500, detail=f"Error parsing response: {str(e)}")


@router.get("/tickets/{ticket_id}", status_code=status.HTTP_200_OK)
def get_support_tickets(ticket_id: int, token_data=Depends(auth_handler.auth_wrapper)):
    header = {
        "Authorization": API_KEY,
        "Content-Type": "application/json",
    }

    params = {
        "include": "conversations"
    }

    try:
        response = requests.get(f'{BASE_URL}/api/v2/tickets/{ticket_id}', headers=header, params=params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if response.status_code != 200:
        return {}

    try:
        response_data = response.json()
        if "tags" in response_data and "sainsbury" not in response_data["tags"]:
            return {}
        return response_data
    except (KeyError, json.JSONDecodeError) as e:
        raise HTTPException(status_code=500, detail=f"Error parsing response: {str(e)}")


@router.put("/tickets/{ticket_id}", status_code=status.HTTP_200_OK)
def update_support_ticket(ticket_id: int, body: UpdateSupportTicketRequestModel,
                          token_data=Depends(auth_handler.auth_wrapper)):
    header = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }

    body = body.dict(exclude_unset=True)
    try:
        response = requests.put(f'{BASE_URL}/api/v2/tickets/{ticket_id}', headers=header, data=json.dumps(body))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)

    return response.json()


@router.post("/tickets")
def create_support_ticket(
        name: str = Form(...),
        email: str = Form(...),
        status: int = Form(...),
        priority: int = Form(...),
        description: str = Form(...),
        subject: Optional[str] = Form(None),
        cc_emails: Optional[List[str]] = Form(None),
        attachments: Optional[List[UploadFile]] = Form(None),
        token_data=Depends(auth_handler.auth_wrapper)
):
    print(name, email, status, priority, description, subject, cc_emails)
    header = {
        "Authorization": API_KEY
    }

    form_data = dict()
    form_data["name"] = name
    form_data["email"] = email
    form_data["status"] = int(status)
    form_data["priority"] = int(priority)
    form_data["description"] = description
    if subject:
        form_data["subject"] = subject
    if cc_emails:
        form_data["cc_emails"] = cc_emails

    files = []
    for attachment in attachments:
        files.append(('attachments[]', (attachment.filename, attachment.file, attachment.content_type)))

    if not files:
        header["Content-Type"] = "application/json"

    try:
        if files:
            response = requests.post(f'{BASE_URL}/api/v2/tickets', headers=header, data=form_data, files=files)
        else:
            response = requests.post(f'{BASE_URL}/api/v2/tickets', headers=header, data=form_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    response = response.json()
    if "id" in response:
        ticket_id = response["id"]
        body = {"tags": ["sainsbury"]}
        header = {"Authorization": API_KEY, "Content-Type": "application/json"}
        try:
            response = requests.put(f'{BASE_URL}/api/v2/tickets/{ticket_id}', headers=header, data=json.dumps(body))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    return response.json()


@router.post("/tickets/{ticket_id}/reply")
def create_ticket_reply(
        ticket_id: int,
        body: str = Form(...),
        user_id: int = Form(...),
        cc_emails: Optional[List[str]] = Form(None),
        bcc_emails: Optional[List[str]] = Form(None),
        attachments: Optional[List[UploadFile]] = File([]),
        token_data=Depends(auth_handler.auth_wrapper)
):
    header = {
        "Authorization": API_KEY
    }

    form_data = {
        "body": body,
        "user_id": user_id
    }

    if cc_emails:
        form_data["cc_emails"] = cc_emails
    if bcc_emails:
        form_data["bcc_emails"] = bcc_emails

    files = []
    for attachment in attachments:
        files.append(('attachments[]', (attachment.filename, attachment.file, attachment.content_type)))

    if not files:
        header["Content-Type"] = "application/json"
        form_data = json.dumps(form_data)

    try:
        if files:
            response = requests.post(f'{BASE_URL}/api/v2/tickets/{str(ticket_id)}/reply', headers=header,
                                    data=form_data, files=files)
        else:
            response = requests.post(f'{BASE_URL}/api/v2/tickets/{str(ticket_id)}/reply', headers=header,
                                     data=form_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return response.json()
