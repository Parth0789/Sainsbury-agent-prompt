from fastapi import APIRouter, HTTPException, status
from fastapi.params import Depends
from fastapi.security import HTTPBearer
from sqlalchemy.orm import Session

from database import get_db, get_internal_dev_xml_db
from crud.login import AuthHandler
from crud.store import (
    fetch_store_regions, fetch_searched_store, fetch_store_details, fetch_all_stores_sco_alerts_details, 
    update_stores_sco_alerts_details, insert_sco_change_logs, fetch_all_sco_config_update_log, fetch_store_zones,
    fetch_xml_stores_details
)
from serializers.request.stores import UpdateScoAlertsData

router = APIRouter()
security = HTTPBearer()
auth_handler = AuthHandler()


@router.get("/stores-region-dropdown", status_code=status.HTTP_200_OK)
def get_stores_regions_dropdown(
    store_id: int = None,
    zone: str = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    result =  fetch_store_regions(db, store_id, zone)
    result = [record.region_id for record in result if record.region_id is not None]
    return result


@router.get("/stores-zone-dropdown", status_code=status.HTTP_200_OK)
def get_stores_zones_dropdown(
    store_id: int = None,
    region_id: int = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    result = fetch_store_zones(db, store_id, region_id)
    result = [record.zone for record in result if record.zone is not None]
    return {"zones": result}


@router.get("/search-store", status_code=status.HTTP_200_OK)
def get_search_store(
    hyper_care_stores: bool = False,
    region_id: int = None,
    zone: str = None,
    query: str = None, 
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    result = fetch_searched_store(query, region_id, zone, hyper_care_stores, db)
    return result


@router.get("/stores-details-dropdown", status_code=status.HTTP_200_OK)
def get_stores_details_dropdown(
    store_id: int = None, 
    region_id: int = None, 
    db: Session = Depends(get_db)
):
    result = fetch_store_details(db, store_id, region_id)

    res = []
    for record in result:
        res.append({
            "id": record.id,
            "name": record.name,
            "region_id": record.company_region_id
        })

    res.sort(key=lambda x: x["name"])

    return {"data": res}


@router.get("/stores-sco-config", status_code=status.HTTP_200_OK)
def get_stores_sco_config_data(
    store_id: int = None,
    db: Session = Depends(get_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    result = fetch_all_stores_sco_alerts_details(db,store_id)

    res = []
    for record in result:
        res.append({
            "store_id": record.store_id,
            "store_name": record.name,
            "miss_scan": True if record.miss_scan == 1 else False,
            "item_switching": True if record.item_switching == 1 else False,
            "item_stacking": True if record.item_stacking == 1 else False,
            "on_scanner": True if record.on_scanner == 1 else False,
            "in_hand": True if record.in_hand == 1 else False,
            "in_basket": True if record.in_basket == 1 else False,
            "incomplete_payment": True if record.incomplete_payment == 1 else False
        })

    res.sort(key=lambda x: x["store_name"])

    return res


@router.get("/stores-sco-config-update-log", status_code=status.HTTP_200_OK)
def get_stores_sco_config_update_log( 
    page: int = 1,
    db: Session = Depends(get_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    return fetch_all_sco_config_update_log(db, page)

    
@router.post("/update-sco-config", status_code=status.HTTP_200_OK)
def update_sco_alert_config(
    body: UpdateScoAlertsData, 
    db: Session = Depends(get_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    body = dict(body)

    store_id = body.get("store_id")
    log_data = body.get("logData")

    update_stores_sco_alerts_details(store_id, body, db)
    insert_sco_change_logs(store_id,log_data,db)

    return {"message": "Updated"}


@router.get("/transaction-upload-stores-dropdown", status_code=status.HTTP_200_OK)
def get_transaction_upload_stores_dropdown(
    db: Session = Depends(get_internal_dev_xml_db),
):
    try:
        result = fetch_xml_stores_details(db)
    except Exception as e:
        print(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    
    return result
