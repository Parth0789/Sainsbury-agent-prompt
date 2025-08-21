from datetime import date, datetime, timedelta
from enum import Enum
from fastapi import APIRouter, status, HTTPException
from fastapi.params import Depends
from fastapi.security import HTTPBearer
from sqlalchemy.orm import Session
from typing import Optional

from database import get_db, get_xml_db, get_internal_dev_xml_db
from utils import get_default_time_range
from utils.general import get_week_start_end
from crud.login import AuthHandler
from crud.sco_main import get_transaction_data
from crud.store import fetch_store_details
from crud.transactions import (
    update_transaction_data_in_db, fetch_outcome_nudges, fetch_causes_nudges, fetch_muted_nudges, fetch_overall_nudges,
    fetch_nudges_per_store, fetch_nudges_details_per_store, fetch_total_transactions, fetch_transactions_with_nudges,
    fetch_nudges_days_wise, fetch_missed_items_per_day, fetch_missed_items_per_week, fetch_missed_items_per_hour,
    fetch_nudge_types, fetch_triggers_week, fetch_triggered_percentage_data, fetch_total_transactions_store_wise,
    fetch_upload_transaction_details, insert_transaction_to_dashboard_db, update_transaction_entry_status,
    upload_transaction_skip_op, fetch_transaction_status_details, fetch_sco_main_bank_nudges_count
)
from crud.for_xml_table_operations import upload_transaction, update_transaction_and_items
from crud.search import search_by_transaction, delete_item
from serializers.request.transactions import (
    UpdateTransactionRequestModel, UploadTransactionDetailsRequestModel, UploadTransactionToDbRequestModel,
    UploadTransactionSkipRequestModel, UploadTransactionStatusDetailsRequestModel
)
from secure_payload import return_encoded_data


router = APIRouter()
security = HTTPBearer()
auth_handler = AuthHandler()


class TimeRange(str, Enum):
    """Valid time ranges for the triggered percentage endpoint."""
    ONE_DAY = "1D"
    ONE_WEEK = "1W"
    ONE_MONTH = "1M"
    ONE_YEAR = "1Y"


@router.post("/transactions-data", status_code=status.HTTP_201_CREATED)
def update_transaction_data(
    body: UpdateTransactionRequestModel, 
    token_data=Depends(auth_handler.auth_wrapper),
    db: Session = Depends(get_db)
):
    body = body.dict()
    transaction_id = body.get("transaction_id")
    description = body.get("description", None)
    clubcard = body.get("clubcard", None)

    data_to_update = {}
    if description is not None:
        data_to_update["description"] = description
    if clubcard is not None:
        data_to_update["clubcard"] = clubcard
    data_to_update["transaction_updated"] = 1

    # try:
    update_transaction_data_in_db(transaction_id, data_to_update, db)
    # except Exception as err:
    #     print(err)
    #     raise HTTPException(status_code=404, detail="transaction id not found")

    # try:
    res = get_transaction_data(db, transaction_id)
    # except Exception as err:
    #     print(err)
    #     raise HTTPException(status_code=404, detail="transaction not found")

    return res


@router.get("/upload-transaction", status_code=status.HTTP_200_OK)
def upload_transaction_(
    db2: Session = Depends(get_xml_db), 
    db: Session = Depends(get_db), 
    transaction_id: str = None,
    counter_type: int = 2
):
    val = upload_transaction(db2, db, transaction_id=transaction_id, counter_type=counter_type)
    return val


@router.get("/search-transaction", status_code=status.HTTP_200_OK)
def search_transaction_(
    db: Session = Depends(get_db), 
    db1: Session = Depends(get_xml_db), 
    transaction_id: str = None
):
    return search_by_transaction(db, db1, transaction_id)


@router.post("/update_transactions_and_items", status_code=status.HTTP_200_OK)
def update_transaction_and_items_(
    details: dict, 
    transaction_id: str, 
    db: Session = Depends(get_db)
):
    return update_transaction_and_items(details, transaction_id, db)


@router.get("/delete_item", status_code=status.HTTP_200_OK)
def delete_item_(
    db: Session = Depends(get_db), 
    db_id: int = None
):
    return delete_item(db, db_id)


@router.get("/outcome-data", status_code=status.HTTP_200_OK)
def get_outcome_data(
    store_id: int = None, 
    region_id: int = None,
    zone: str = None,
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    result = fetch_outcome_nudges(store_id, region_id, zone, start_date, end_date, db)
    rectified_count, non_rectified_count, monitored_count = 0, 0, 0
    for record in result:
        if record.clubcard == "Rectified":
            rectified_count += 1
        elif record.clubcard == "Non-Rectified":
            non_rectified_count += 1
        elif record.clubcard == "Not-Present":
            monitored_count += 1

    total = len(result)
    rectified_percentage = round((rectified_count / total) * 100) if total else 0
    non_rectified_percentage = round((non_rectified_count / total) * 100) if total else 0
    monitored_percentage = round((monitored_count / total) * 100) if total else 0

    return {
        # "protected_count": rectified_percentage, "loss_count": non_rectified_percentage,
        "data": [
            {"name": "Corrected", "count": rectified_count, "percentage": rectified_percentage, "color": "#f06c00"},
            {"name": "Failed", "count": non_rectified_count, "percentage": non_rectified_percentage, "color": "#d7d6d6"},
            {"name": "Monitored", "count": monitored_count, "percentage": monitored_percentage, "color": "#8f8f8f"}
        ],
        "total": total
    }


@router.get("/causes-data", status_code=status.HTTP_200_OK)
def get_causes_data(
    store_id: int = None, 
    region_id: int = None, 
    zone: str = None,
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db),
    db_xml: Session = Depends(get_xml_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    trigger_color_mapping = {
        "Missed Scan": "#EEEEEE",
        "On Scanner": "#8F8F8F",
        "In Hand": "#DFDDDD",
        "Incomplete Payment": "#B3AFAF",
        "In Basket": "#C9C9C9",
        "Item Switching": "#F06C00",
        "Item Stacking": "#DFD9D8",
        "Floor": "#eeeeee",
        "Trolley": "#838383"
    }
    result = fetch_causes_nudges(store_id, region_id, zone, start_date, end_date, db)
    total = 0
    for record in result:
        total += record.trigger_count

    data = []
    for record in result:
        data.append(
            {
                "name": record.description,
                "count": record.trigger_count,
                "percentage": round((record.trigger_count / total) * 100, 2) if total else 0,
                "color": (
                    trigger_color_mapping[record.description] 
                    if record.description in trigger_color_mapping else "#c9c9c9"
                )
            }
        )

    result = fetch_overall_nudges(store_id, region_id, zone, start_date, end_date, db)
    loss_count, no_loss_count = 0, 0
    for record in result:
        if record.clubcard == "Non-Rectified":
            loss_count = record.count
        elif record.clubcard == "Rectified":
            no_loss_count = record.count

    total_transactions = fetch_total_transactions(store_id, start_date, end_date, db_xml)
            
    return {
        "data": data,
        "no_loss": {"count": no_loss_count},
        "loss": {"count": loss_count},
        "total": total,
        "total_percentage": round((total / total_transactions) * 100, 2) if total_transactions else 0
    }


@router.get("/causes-data-staging", status_code=status.HTTP_200_OK)
def get_causes_data_staging(
    store_id: int = None, 
    region_id: int = None, 
    zone: str = None,
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db),
    db_xml: Session = Depends(get_xml_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    trigger_color_mapping = {
        "Missed Scan": "#EEEEEE",
        "On Scanner": "#8F8F8F",
        "In Hand": "#DFDDDD",
        "Incomplete Payment": "#B3AFAF",
        "In Basket": "#C9C9C9",
        "Item Switching": "#F06C00",
        "Item Stacking": "#DFD9D8",
        "Floor": "#eeeeee",
        "Trolley": "#838383"
    }
    result = fetch_causes_nudges(store_id, region_id, zone, start_date, end_date, db)
    total = 0
    for record in result:
        if record.description in ("Item Missed Scan","Missed Scan", "On Scanner", "In Hand", "Incomplete Payment"):
            total += record.trigger_count

    data = []
    for record in result:
        if record.description in ("Item Missed Scan","Missed Scan", "On Scanner", "In Hand", "Incomplete Payment"):
            data.append(
                {
                    "name": record.description,
                    "count": record.trigger_count,
                    "percentage": round((record.trigger_count / total) * 100, 2) if total else 0,
                    "color": (
                        trigger_color_mapping[record.description] 
                        if record.description in trigger_color_mapping else "#c9c9c9"
                    )
                }
            )

    result = fetch_overall_nudges(store_id, region_id, zone, start_date, end_date, db)
    loss_count, no_loss_count = 0, 0
    for record in result:
        if record.clubcard == "Non-Rectified":
            loss_count = record.count
        elif record.clubcard == "Rectified":
            no_loss_count = record.count
    
    total_transactions = fetch_total_transactions(store_id, start_date, end_date, db_xml)

    if not data:
        data = [
            {
                "name": "Item Missed Scan",
                "count": 0,
                "percentage": 0,
                "color": "#EEEEEE"
            },
            {
                "name": "On Scanner",
                "count": 0,
                "percentage": 0,
                "color": "#8F8F8F"
            },
            {
                "name": "In Hand",
                "count": 0,
                "percentage": 0,
                "color": "#DFDDDD"
            },
            {
                "name": "Incomplete Payment",
                "count": 0,
                "percentage": 0,
                "color": "#B3AFAF"
            }
        ]
            
    return {
        "data": data,
        "no_loss": {"count": no_loss_count},
        "loss": {"count": loss_count},
        "total": total,
        "total_percentage": round((total / total_transactions) * 100, 2) if total_transactions else 0
    }


@router.get("/muted-nudges-data", status_code=status.HTTP_200_OK)
def get_muted_nudges_data(
    store_id: int = None, 
    region_id: int = None, 
    zone: str = None,
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    trigger_color_mapping = {
        "Missed Scan": "#EEEEEE",
        "On Scanner": "#8F8F8F",
        "In Hand": "#DFDDDD",
        "Incomplete Payment": "#B3AFAF",
        "In Basket": "#C9C9C9",
        "Item Switching": "#F06C00",
        "Item Stacking": "#DFD9D8",
        "Floor": "#eeeeee",
        "Trolley": "#838383"
    }

    result = fetch_muted_nudges(store_id, region_id, zone, start_date, end_date, db)

    total = 0
    for record in result:
        total += record.trigger_count

    data = []
    for record in result:
        data.append(
            {
                "name": record.description,
                "count": record.trigger_count,
                "percentage": round((record.trigger_count / total) * 100, 2) if total else 0,
                "color": trigger_color_mapping[
                    record.description] if record.description in trigger_color_mapping else "#c9c9c9"
            }
        )

    return {
        "data": data,
        "total": total
    }


@router.get("/muted-nudges-data-staging", status_code=status.HTTP_200_OK)
def get_muted_nudges_data_staging(
    store_id: int = None, 
    region_id: int = None, 
    zone: str = None,
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    trigger_color_mapping = {
        "Missed Scan": "#EEEEEE",
        "On Scanner": "#8F8F8F",
        "In Hand": "#DFDDDD",
        "Incomplete Payment": "#B3AFAF",
        "In Basket": "#C9C9C9",
        "Item Switching": "#F06C00",
        "Item Stacking": "#DFD9D8",
        "Floor": "#eeeeee",
        "Trolley": "#838383"
    }

    result = fetch_muted_nudges(store_id, region_id, zone, start_date, end_date, db)

    total = 0
    for record in result:
        if record.description in ("In Basket", "Item Switching", "Item Stacking", "Floor", "Trolley"):
            total += record.trigger_count

    # basket_count, item_switch_count, item_stacking_count, floor_count, trolley_count = 0, 0, 0, 0, 0
    # for record in result:
    #     if record.description.lower() == "basket":
    #         basket_count += 1
    #     elif record.description.lower() == "item switching":
    #         item_switch_count += 1
    #     elif record.description.lower() == "item stacking":
    #         item_stacking_count += 1
    #     elif record.description.lower() == "floor":
    #         floor_count += 1
    #     elif record.description.lower() == "trolley":
    #         trolley_count += 1
    #
    # total = len(result)
    # basket_percentage = round((basket_count / total) * 100) if total else 0
    # item_switch_percentage = round((item_switch_count / total) * 100) if total else 0
    # item_stacking_percentage = round((item_stacking_count / total) * 100) if total else 0
    # floor_percentage = round((floor_count / total) * 100) if total else 0
    # trolley_percentage = round((trolley_count / total) * 100) if total else 0

    data = []
    for record in result:
        if record.description in ("In Basket", "Item Switching", "Item Stacking", "Floor", "Trolley"):
            data.append(
                {
                    "name": record.description,
                    "count": record.trigger_count,
                    "percentage": round((record.trigger_count / total) * 100, 2) if total else 0,
                    "color": trigger_color_mapping[
                        record.description] if record.description in trigger_color_mapping else "#c9c9c9"
                }
            )

    return {
        "data": data,
        "total": total
    }

    # return {
    #     "basket_count": basket_percentage,
    #     "item_switch_count": item_switch_percentage,
    #     "item_stacking_count": item_stacking_percentage,
    #     "floor_count": floor_percentage,
    #     "trolley_count": trolley_percentage,
    #     "data": [
    #         {"name": "Basket", "count": basket_count, "percentage": basket_percentage, "color": "#c9c9c9"},
    #         {"name": "Item Switching", "count": item_switch_count, "percentage": item_switch_percentage, "color": "#f06c00"},
    #         {"name": "Item Stacking", "count": item_stacking_count, "percentage": item_stacking_percentage, "color": "#ffa995"},
    #         {"name": "Floor", "count": floor_count, "percentage": floor_percentage, "color": "#eeeeee"},
    #         {"name": "Trolley", "count": trolley_count, "percentage": trolley_percentage, "color": "#8f8f8f"}
    #     ],
    #     "total": total
    # }


@router.get("/overall-nudges-data", status_code=status.HTTP_200_OK)
def get_overall_nudges_data(
    store_id: int = None, 
    region_id: int = None, 
    zone: str = None,
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    result = fetch_overall_nudges(store_id, region_id, zone, start_date, end_date, db)

    trigger_color_mapping = {
        "Loss": "#8F8F8F",
        "No Loss": "#F06C00"
    }

    total = 0
    for record in result:
        total += record.count

    data = []
    for record in result:
        label = "Loss" if record.clubcard == "Non-Rectified" else "No Loss"
        data.append({
            "name": label,
            "count": record.count,
            "percentage": round((record.count / total) * 100, 2) if total else 0,
            "color": trigger_color_mapping[label] if label in trigger_color_mapping else "#c9c9c9"
        })

    result = fetch_sco_main_bank_nudges_count(store_id, region_id, zone, start_date, end_date, db)
    main_bank_count, sco_count = result.main_bank_count, result.sco_count

    return {
        "data": data,
        "main_bank": {"count": main_bank_count if main_bank_count else 0},
        "sco": {"count": sco_count if sco_count else 0},
        "total": total
    }


@router.get("/overall-nudges-data-staging", status_code=status.HTTP_200_OK)
def get_overall_nudges_data_staging(
    store_id: int = None, 
    region_id: int = None, 
    zone: str = None,
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    result = fetch_overall_nudges(store_id, region_id, zone, start_date, end_date, db)

    trigger_color_mapping = {
        "Loss": "#8F8F8F",
        "No Loss": "#F06C00"
    }

    total = 0
    for record in result:
        total += record.count

    data = []
    for record in result:
        label = "Loss" if record.clubcard == "Non-Rectified" else "No Loss"
        data.append({
            "name": label,
            "count": record.count,
            "percentage": round((record.count / total) * 100, 2) if total else 0,
            "color": trigger_color_mapping[label] if label in trigger_color_mapping else "#c9c9c9"
        })

    if not data:
        data.append({
            "name": "Loss",
            "count": 0,
            "percentage": 0,
            "color": "#8F8F8F"
        })
    if len(data) == 1:
        data.append({
            "name": "No Loss",
            "count": 0,
            "percentage": 0,
            "color": "#F06C00"
        })

    result = fetch_sco_main_bank_nudges_count(store_id, region_id, zone, start_date, end_date, db)
    main_bank_count, sco_count = result.main_bank_count, result.sco_count

    return {
        "data": data,
        "main_bank": {"count": main_bank_count},
        "sco": {"count": sco_count},
        "total": total
    }


@router.get("/nudges-per-store", status_code=status.HTTP_200_OK)
def get_nudges_per_store(
    store_id: int = None, 
    region_id: int = None, 
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    if not start_date and not end_date:
        start_date, end_date = get_default_time_range()

    result = fetch_nudges_per_store(store_id, region_id, start_date, end_date, db)
    res = []
    for record in result:
        res.append({
            "name": record.name,
            "count": record.trigger_count if record.total_count else 0
        })

    return res


@router.get("/triggers-percentage-week-wise", status_code=status.HTTP_200_OK)
def get_triggers_percentage_week(
    store_id: int = None, 
    region_id: int = None, 
    db: Session = Depends(get_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    result = fetch_triggers_week(store_id, region_id, db)
    result_dict = {record.day_of_week: record for record in result}
    week_days_list = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    res = []
    for day_of_week in week_days_list:
        percentage = 0
        if day_of_week in result_dict:
            record = result_dict[day_of_week]
            percentage = round((record.trigger_count / record.total_count) * 100) if record.total_count else 0

        res.append({
            "day_of_week": day_of_week,
            "percentage": percentage
        })

    return res


@router.get("/top-stores", status_code=status.HTTP_200_OK)
def get_nudges_per_store(
    store_id: int = None, 
    region_id: int = None,
    zone: str = None,
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db),
    db_xml: Session = Depends(get_xml_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    # if not start_date and not end_date:
    #     start_date, end_date = get_default_time_range()

    result_stores = fetch_store_details(db, store_id, region_id, zone)
    store_ids = [int(record.id) for record in result_stores]
    total_transactions = fetch_total_transactions_store_wise(store_ids, start_date, end_date, db_xml)
    result = fetch_nudges_details_per_store(store_ids, region_id, zone, start_date, end_date, db)

    total_transactions_dict = {record.store_id: record.total_transactions for record in total_transactions}
    res = []
    for record in result:
        record = dict(record)
        store_id = int(record["store_id"])
        if store_id in total_transactions_dict:
            total_transactions = total_transactions_dict[store_id]
            record["percentage"] = (
                float("{:.2f}".format((record["nudges"]/total_transactions)*100))
                if total_transactions else 0
            )
            record["protected_percentage"] = (
                float("{:.2f}".format((record["protected"]/record["nudges"])*100))
                if record["nudges"] else 0
            )
            res.append(record)

    res = sorted(res, key=lambda k: k["nudges"], reverse=True)

    return res[:10]


@router.get("/bottom-stores", status_code=status.HTTP_200_OK)
def get_nudges_per_store(
    store_id: int = None, 
    region_id: int = None, 
    zone: str = None,
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db), 
    db_xml: Session = Depends(get_xml_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    # if not start_date and not end_date:
    #     start_date, end_date = get_default_time_range()

    result_stores = fetch_store_details(db, store_id, region_id, zone)
    store_ids = [int(record.id) for record in result_stores]
    total_transactions = fetch_total_transactions_store_wise(store_ids, start_date, end_date, db_xml)
    result = fetch_nudges_details_per_store(store_ids, region_id, zone, start_date, end_date, db)

    total_transactions_dict = {record.store_id: record.total_transactions for record in total_transactions}
    res = []
    for record in result:
        record = dict(record)
        store_id = int(record["store_id"])
        if store_id in total_transactions_dict:
            total_transactions = total_transactions_dict[store_id]
            record["percentage"] = (
                float("{:.2f}".format((record["nudges"] / total_transactions) * 100))
                if total_transactions else 0
            )
            record["protected_percentage"] = (
                float("{:.2f}".format((record["protected"] / record["nudges"]) * 100))
                if record["nudges"] else 0
            )
            res.append(record)

    res = sorted(res, key=lambda k: k["nudges"], reverse=False)

    return res[:10]


@router.get("/transactions-with-nudges", status_code=status.HTTP_200_OK)
def get_nudges_per_store(
    store_id: int = None, 
    region_id: int = None,
    zone: str = None,
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db), 
    db_xml: Session = Depends(get_xml_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    # if not start_date and not end_date:
    #     start_date, end_date = get_default_time_range()

    result_stores = fetch_store_details(db, store_id, region_id, zone)
    store_ids = [int(record.id) for record in result_stores]
    total_transactions = fetch_total_transactions(store_ids, start_date, end_date, db_xml)
    result = fetch_transactions_with_nudges(store_id, region_id, zone, start_date, end_date, db)

    muted = result.muted if result.muted else 0
    triggered = result.triggered if result.triggered else 0
    loss = result.loss if result.loss else 0
    total = result.total if result.total else 0
    res = {
        "transactions": total_transactions,
        "monitored": muted,
        "triggered": triggered,
        "loss": loss,
        "monitored_percentage": float("{:.2f}".format((muted/result.total)*100)) if total else 0,
        "triggered_percentage": float("{:.2f}".format((triggered/result.total)*100)) if total else 0,
        "loss_percentage": float("{:.2f}".format((loss/result.total)*100)) if total else 0
    }

    return res


@router.get("/last-n-days-nudges", status_code=status.HTTP_200_OK)
def get_last_n_days_nudges(
    nudge_type: str, 
    days: int = 6, 
    store_id: int = None, 
    region_id: int = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    current_date = datetime.now().date()
    date_list = [current_date-timedelta(days=i) for i in range(1, days+1)]
    
    result = fetch_nudges_days_wise(store_id, region_id, nudge_type, date_list, db)
    result_dict = {record.date: record.nudges_count for record in result}
    res = []
    total = 0
    for date in date_list:
        res.append({
            "date": date,
            "nudges": result_dict.get(date, 0)
        })
        total += result_dict.get(date, 0)

    return {"data": res, "total": total}


@router.get("/item-missed-by-day", status_code=status.HTTP_200_OK)
def get_item_missed_by_day(
    store_id: int = None, 
    region_id: int = None, 
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    if not start_date and not end_date:
        start_date, end_date = get_default_time_range()

    result = fetch_missed_items_per_day(store_id, region_id, start_date, end_date, db)
    
    # Define the order of days starting with Sunday
    day_order = {
        'Sunday': 0,
        'Monday': 1, 
        'Tuesday': 2,
        'Wednesday': 3,
        'Thursday': 4,
        'Friday': 5,
        'Saturday': 6
    }
    
    # Sort the result based on the day order
    res = sorted(result, key=lambda x: day_order[x.date])

    if not res:
        res = [
            {"date": day, "missed_items_count": 0}
            for day in ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        ]

    return res


@router.get("/item-missed-by-week", status_code=status.HTTP_200_OK)
def get_item_missed_by_week(
    store_id: int = None, 
    region_id: int = None, 
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    if not start_date and not end_date:
        start_date, end_date = get_default_time_range()

    result = fetch_missed_items_per_week(store_id, region_id, start_date, end_date, db)
    res = []
    for record in result:
        record = dict(record)
        week_no = int(record["week"])
        year = int(record["year"])
        week_start_date, week_end_date = get_week_start_end(year, week_no)
        record["week_start_date"] = week_start_date
        record["week_end_date"] = week_end_date
        res.append(record)

    res = res[::-1]

    if not res:
        start_date -= timedelta(days=start_date.weekday())
        while start_date <= end_date:
            year = start_date.year
            week_no = start_date.isocalendar()[1]
            week_start_date, week_end_date = get_week_start_end(year, week_no)
            res.append({
                "week": week_no,
                "year": year,
                "missed_items_count": 0,
                "week_start_date": week_start_date,
                "week_end_date": week_end_date
            })

            start_date += timedelta(days=7)

    return res


@router.get("/item-missed-by-hour", status_code=status.HTTP_200_OK)
def get_item_missed_by_hour(
    store_id: int = None, 
    region_id: int = None, 
    start_date: date = None, 
    end_date: date = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    if not start_date and not end_date:
        start_date, end_date = get_default_time_range()

    result = fetch_missed_items_per_hour(store_id, region_id, start_date, end_date, db)
    # result = result[:24]
    res = result
    # for record in result:
    #     record = dict(record)
    #     record["date"] = record["date"].strftime("%d-%m-%Y")
    #     res.append(record)

    if not res:
        for i in range(24):
            res.append({
                "date": (start_date + timedelta(hours=i)).strftime("%d-%m-%Y"),
                "hour": i,
                "missed_items_count": 0
            })

    return res


@router.get("/nudge-types", status_code=status.HTTP_200_OK)
def get_nudge_types(
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    result = fetch_nudge_types(db)
    return [record.nudge_type for record in result]


@router.get("/trigger-types-dropdown", status_code=status.HTTP_200_OK)
def get_nudge_types(
    nudge_type: str = "corrected",
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    if nudge_type == "corrected":
        return ["Item Missed Scan", "On Scanner", "In Hand", "Incomplete Payment"]
    elif nudge_type == "failed":
        return ["Item Missed Scan", "On Scanner", "In Hand", "Incomplete Payment"]
    elif nudge_type == "monitored":
        return ["Floor", "Item Switching", "Item Stacking", "In Basket", "Trolley"]
    
    return []


@router.get("/triggered-percentage", status_code=status.HTTP_200_OK)
def get_triggered_percentage(
    store_id: Optional[int] = None,
    region_id: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    """
    Get triggered percentage data for graphs.
    
    Args:
        store_id: ID of the store (optional)
        region_id: ID of the region (optional)
        client_region_id: ID of the client region (optional)
        start_date: Start date for filtering (optional)
        end_date: End date for filtering (optional)
        
    Returns:
        dict: Triggered percentage data for graphs
    """
    try:
        # Track if this is the default case (no dates provided)
        is_default = False
        
        # Calculate date range if not provided
        if not start_date or not end_date:
            # Get current date
            current_date = date.today()
            
            # Set to show all data in yearly view
            # Start from beginning of previous year for more historical data
            start_date = date(current_date.year - 1, 1, 1)
            
            # End date is current date
            end_date = current_date
            
            # Use ONE_YEAR time range for default view
            time_range = TimeRange.ONE_YEAR
            is_default = True
            # Set date_diff for the default case
            date_diff = (end_date - start_date).days
        else:
            # Determine time range based on date difference
            date_diff = (end_date - start_date).days

            # Updated time range logic based on requirements
            if date_diff < 6:  # Less than 6 days should show daily data
                time_range = TimeRange.ONE_WEEK  # Use per day view for < 6 days
            elif date_diff <= 30:  # 6+ days show weekly data
                time_range = TimeRange.ONE_MONTH  # Use per week view for 6-31 days (inclusive)
            elif date_diff <= 365:
                time_range = TimeRange.ONE_YEAR  # Use per month view for <= 365 days
            else:
                time_range = TimeRange.ONE_YEAR  # Use year view for > 365 days
            
        # Convert dates to datetime for beginning and end of day
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        # Fetch data from database
        result = fetch_triggered_percentage_data(db, store_id, region_id, start_datetime, end_datetime, time_range)
                
        # Format data based on time_range
        formatted_data = []
        
        # Set title based on time range
        title = ""
        if time_range == TimeRange.ONE_DAY:
            title = "Triggered % per day"
        elif time_range == TimeRange.ONE_WEEK:
            title = "Triggered % per day"  # Daily view for less than 6 days
        elif time_range == TimeRange.ONE_MONTH:
            title = "Triggered % per week"  # Weekly view for 6-31 days
        elif time_range == TimeRange.ONE_YEAR:
            if date_diff >= 365:
                title = "Triggered % per year"
            else:
                title = "Triggered % per month"
            
        # ONE_DAY case is removed as per requirements
            
        if time_range == TimeRange.ONE_WEEK:
            # For time ranges less than a week, show daily data
            day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
            
            # Check if we have date objects in the result (custom date range)
            if result and isinstance(result[0][0], date):
                for actual_date, percentage in result:
                    # Get the day of week for the date
                    day_name = day_names[(actual_date.weekday() + 1) % 7]  # Convert to Sunday=0
                    
                    # Format as "Weekday (MM-DD)"
                    formatted_data.append({
                        "label": f"{day_name} ({actual_date.strftime('%m-%d')})",
                        "value": percentage
                    })
            else:
                # Fallback to standard day of week formatting
                for day_idx, percentage in result:
                    formatted_data.append({
                        "label": day_names[day_idx],
                        "value": percentage
                    })
            
        elif time_range == TimeRange.ONE_MONTH:
            # For time ranges less than a month, show weekly data with proper date boundaries
            from utils.datetime_utils import format_date_range_label
            
            for week_range, percentage in result:
                # Format the date range as a string using the formatter
                label = format_date_range_label(week_range)
                formatted_data.append({
                    "label": label,
                    "value": percentage
                })
                
        elif time_range == TimeRange.ONE_YEAR:
            # Check if this is a date range >= 1 year, or the default case with year-only data
            if date_diff >= 365 or (is_default and result and isinstance(result[0][0], str) and result[0][0].isdigit()):
                # Format for year view: Years only
                for year, percentage in result:
                    formatted_data.append({
                        "label": year,
                        "value": percentage
                    })
            else:
                # Format for time ranges less than a year: show monthly data
                month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                
                # Check if we have tuples with year and month (from a multi-year range)
                if result and isinstance(result[0][0], tuple):
                    for (year, month), percentage in result:
                        # Include year in label when multiple years are in the range
                        if start_datetime.year != end_datetime.year:
                            formatted_data.append({
                                "label": f"{month_names[month - 1]} {year}",
                                "value": percentage
                            })
                        else:
                            # Single year range, no need to show year
                            formatted_data.append({
                                "label": month_names[month - 1],
                                "value": percentage
                            })
                else:
                    # Legacy format (month index only)
                    for month_idx, percentage in result:
                        formatted_data.append({
                            "label": month_names[month_idx - 1],  # Month index is 1-based
                            "value": percentage
                        })

        return return_encoded_data({
            "title": title,
            "data": formatted_data
        })

    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving triggered percentage data: {str(e)}")
    

@router.post("/upload-transaction-list", status_code=status.HTTP_200_OK)
def upload_transaction_details(
    body: UploadTransactionDetailsRequestModel,
    db: Session = Depends(get_internal_dev_xml_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    body = body.dict()
    stores_id_list = body.get("store_ids", [])
    start_time = body.get("start_time", None)
    end_time = body.get("end_time", None)
    nudge_type = body.get("nudge_type", "all")
    transaction_id, item_details, missed_items_details, store_name, store_id = fetch_upload_transaction_details(
        stores_id_list=stores_id_list, start_time=start_time, end_time=end_time, db_xml_dev=db,nudge_type=nudge_type
    )

    transaction_date = missed_items_details[0].beginDate if missed_items_details else None
    sequence_number = item_details[0].sequence_no if item_details else None
    till_number = item_details[0].counterno if item_details else None
    nudge_count = len(missed_items_details) if missed_items_details else 0
    
    if not missed_items_details:
        return {}
    
    if nudge_type == "not_attended":
        video_url = f"https://sainsbury-zip-hdfjs.s3.eu-west-2.amazonaws.com/Videos/{transaction_date[:10]}/{str(store_id)}/unzip/not_attended/{transaction_id}.mp4"
    else:
        video_url = f"https://sainsbury-zip-hdfjs.s3.eu-west-2.amazonaws.com/Videos/{transaction_date[:10]}/{str(store_id)}/unzip/alerts/{transaction_id}.mp4"

    items = []
    for record in item_details:
        items.append({
            "item_name": record.Name,
            "item_quantity": int(record.Quantity) if record.Quantity else 0,
            "item_date": record.BeginDateTime.replace("T", " "),
            "missed_items": False
        })
    
    for record in missed_items_details:
        items.append({
            "item_name": "Item",
            "item_quantity": 1,
            "item_date": record.beginDate.replace("T", " "),
            "missed_items": True
        })

    items = sorted(items, key=lambda x: x["item_date"])
    transaction_date = items[0]["item_date"] if items else None

    return {
        "transaction_id": transaction_id,
        "store_name": store_name,
        "store_id": store_id,
        "sequence_number": sequence_number,
        "till_number": till_number,
        "nudge_count": nudge_count,
        "transaction_date": transaction_date,
        "video_url": video_url,
        "items": items
    }


@router.put("/upload-transaction-to-db", status_code=status.HTTP_200_OK)
def upload_transaction_to_db(
    body: UploadTransactionToDbRequestModel,
    db: Session = Depends(get_db),
    db_xml_dev_db: Session = Depends(get_internal_dev_xml_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    body = body.dict()
    transaction_id = body.get("transaction_id", None)
    store_id = body.get("store_id", None)
    description = body.get("description", None)
    clubcard = body.get("clubcard", None)
    nudge_type = body.get("nudge_type", "all")

    try:
        insert_transaction_to_dashboard_db(transaction_id, description, clubcard, store_id, db, db_xml_dev_db,nudge_type=nudge_type)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Error uploading transaction to dashboard: {str(e)}"
        )

    update_transaction_entry_status(transaction_id, db_xml_dev_db)

    return {
        "message": "Transaction uploaded to dashboard successfully"
    }


@router.post("/upload-transaction-skip", status_code=status.HTTP_200_OK)
def upload_transaction_skip(
    body: UploadTransactionSkipRequestModel,
    db_xml_dev_db: Session = Depends(get_internal_dev_xml_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    body = body.dict()
    transaction_id = body.get("transaction_id", None)

    try:
        upload_transaction_skip_op(transaction_id, db_xml_dev_db)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error uploading transaction skip: {str(e)}")

    return {"message": "Transaction skipped successfully"}


@router.post("/upload-transaction-status-details", status_code=status.HTTP_200_OK)
def upload_transaction_status_details(
    body: UploadTransactionStatusDetailsRequestModel,
    db: Session = Depends(get_internal_dev_xml_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    body = body.dict()
    store_ids = body.get("store_ids", [])
    start_time = body.get("start_time", None)
    end_time = body.get("end_time", None)
    nudge_type = body.get("nudge_type", "all")

    entry_status_result, skip_result = fetch_transaction_status_details(store_ids, start_time, end_time, db, nudge_type=nudge_type)

    entry_status_dict = {record.id: record for record in entry_status_result}
    skip_dict = {record.id: record for record in skip_result}

    store_id_result = set(list(entry_status_dict.keys()) + list(skip_dict.keys()))

    res = []
    total_entry_status_count = 0
    total_skip_count = 0
    for store_id in store_id_result:
        store_name = None
        if store_id in entry_status_dict:
            store_name = entry_status_dict.get(store_id).name
        elif store_id in skip_dict:
            store_name = skip_dict.get(store_id).name

        entry_status_count = 0
        if store_id in entry_status_dict:
            entry_status_count = entry_status_dict.get(store_id).entry_status_count
        
        skip_count = 0
        if store_id in skip_dict:
            skip_count = skip_dict.get(store_id).skip_count

        total_entry_status_count += entry_status_count
        total_skip_count += skip_count

        if store_name:
            res.append({
                "store_id": store_id,
                "store_name": store_name,
                "entry_status_count": entry_status_count,
                "skip_count": skip_count
            })

    print("total_entry_status_count: ", total_entry_status_count)
    print("total_skip_count: ", total_skip_count)

    return {"data": res}
