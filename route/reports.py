import json
import pandas as pd
from io import BytesIO
from pprint import pprint
from fastapi import APIRouter, status, HTTPException, UploadFile, File
from fastapi.params import Depends
from fastapi.security import HTTPBearer
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from datetime import date, datetime, timedelta

from database import get_db, get_xml_db
from crud.login import AuthHandler
from crud.reports import get_overall_details_report_v2, get_overall_details_report_v3, fetch_operator_losses_data
from crud.store import fetch_store_details
from crud.status import fetch_store_status_for_report, fetch_live_stores_sco_count
from secure_payload import return_encoded_data
from utils import send_mail


router = APIRouter()
security = HTTPBearer()
auth_handler = AuthHandler()


HYPERCARE_STORES = [35, 69, 9, 51, 54, 170, 195, 190, 125, 134, 150, 25, 7, 158, 136, 188, 154, 43, 183, 48, 204, 206,
                    210]


@router.get("/overall-details-report", status_code=status.HTTP_200_OK)
def get_overall_details_report_(
    store_id: int = None,
    region_id: int = None,
    zone: str = None,
    start_time: date = None, 
    end_time: date = None,
    sort_by: str = "Store Name", 
    sort_order: str = "ASC", 
    hyper_care_stores: bool = False,
    new_dashboard: bool = False, 
    token_data = Depends(auth_handler.auth_wrapper),
    db: Session = Depends(get_db), 
    db_xml: Session = Depends(get_xml_db)
):
    result_stores = fetch_store_details(db, store_id, region_id, zone)
    result_transaction, result_overall_report_sco, result_transaction_comment = (
        get_overall_details_report_v2(db, db_xml, start_time, end_time, store_id, region_id, zone))
    sco_count_result = fetch_live_stores_sco_count(store_id, db)

    transaction_dict = dict()
    for record in result_transaction:
        if record.store_id not in transaction_dict:
            transaction_dict[record.store_id] = {}
        transaction_dict[record.store_id][record.clubcard] = record.nudge_count

    transaction_comment_dict = dict()
    for record in result_transaction_comment:
        if record.store_id not in transaction_comment_dict:
            transaction_comment_dict[record.store_id] = {}
        transaction_comment_dict[record.store_id][record.clubcard] = record.comment_count

    sco_count_dict = {str(record.id): record.sco_count for record in sco_count_result}

    overall_report_sco_dict = {str(record.store_id): record for record in result_overall_report_sco}

    stores_dict = {
        str(record.id): {"name": record.name, "zone": record.zone, "region": record.company_region_id} 
        for record in result_stores
    }

    rectified_total, non_rectified_total, persistent_memory_total, not_presented_total, shopping_habit_total, \
        missed_total, days_total, total_transactions, total_sco = 0, 0, 0, 0, 0, 0, 0, 0, 0
    res = []
    for store_id, store_details in stores_dict.items():
        if hyper_care_stores and int(store_id) not in HYPERCARE_STORES:
            continue

        data = dict()
        data["Store Name"] = store_details["name"]
        data["Zone"] = store_details["zone"]
        data["Region"] = store_details["region"]
        data["Total Transactions"] = int(overall_report_sco_dict[store_id].total_transactions) \
            if store_id in overall_report_sco_dict else 0
        if store_id in transaction_dict and "Rectified" in transaction_dict[store_id]:
            data["Rectified"] = transaction_dict[store_id]["Rectified"]
        else:
            data["Rectified"] = 0
        if (store_id in transaction_comment_dict and "Rectified" in transaction_comment_dict[store_id]
                and data["Rectified"]):
            data["Rectified"] -= transaction_comment_dict[store_id]["Rectified"]

        if store_id in transaction_dict and "Non-Rectified" in transaction_dict[store_id]:
            data["Non-Rectified"] = transaction_dict[store_id]["Non-Rectified"]
        else:
            data["Non-Rectified"] = 0
        if (store_id in transaction_comment_dict and "Non-Rectified" in transaction_comment_dict[store_id]
                and data["Non-Rectified"]):
            data["Non-Rectified"] -= transaction_comment_dict[store_id]["Non-Rectified"]

        if store_id in transaction_dict and "Persistent Memory" in transaction_dict[store_id]:
            data["Persistent Memory"] = transaction_dict[store_id]["Persistent Memory"]
        else:
            data["Persistent Memory"] = 0
        if (store_id in transaction_comment_dict and "Persistent Memory" in transaction_comment_dict[store_id] and
                data["Persistent Memory"]):
            data["Persistent Memory"] -= transaction_comment_dict[store_id]["Persistent Memory"]

        if store_id in transaction_dict and "Not-Present" in transaction_dict[store_id]:
            data["Not Presented"] = transaction_dict[store_id]["Not-Present"]
        else:
            data["Not Presented"] = 0
        if (store_id in transaction_comment_dict and "Not-Present" in transaction_comment_dict[store_id]
                and data["Not Presented"]):
            data["Not Presented"] -= transaction_comment_dict[store_id]["Not-Present"]

        data["Shopping Habit"] = sum(transaction_comment_dict[store_id].values()) \
            if store_id in transaction_comment_dict else 0

        if new_dashboard:
            data["Missed"] = (data["Rectified"] + data["Non-Rectified"] + data["Not Presented"])
        else:
            data["Missed"] = (data["Rectified"] + data["Non-Rectified"] + data["Persistent Memory"] + data["Not Presented"]
                              + data["Shopping Habit"])

        data["Days"] = overall_report_sco_dict[store_id].days_count \
            if store_id in overall_report_sco_dict else 0
        data["Nudges Per Day"] = data["Missed"] // data["Days"] if data["Days"] else 0
        data["Transactions With Nudges"] = round(
            (data["Missed"] / data["Total Transactions"]) * 100, 2
        ) if data["Total Transactions"] else 0
        data["Transactions With No Loss"] = round(
            (data["Rectified"] / (data["Rectified"] + data["Non-Rectified"])) * 100, 2
        ) if (data["Rectified"] + data["Non-Rectified"]) else 0
        data["Number of SCO"] = sco_count_dict[store_id] if store_id in sco_count_dict else 0

        rectified_total += data["Rectified"]
        non_rectified_total += data["Non-Rectified"]
        persistent_memory_total += data["Persistent Memory"]
        not_presented_total += data["Not Presented"]
        shopping_habit_total += data["Shopping Habit"]
        missed_total += data["Missed"]
        days_total += data["Days"]
        total_transactions += data["Total Transactions"]
        total_sco += data["Number of SCO"]

        res.append(data)

    if sort_order == "DESC":
        sort_order = True
    else:
        sort_order = False
    if not sort_by:
        sort_by = "Store Name"
    res.sort(key=lambda x: x[sort_by], reverse=sort_order)

    res.append({
        "Store Name": "",
        "Zone": "",
        "Region": "",
        "Total Transactions": total_transactions,
        "Rectified": rectified_total,
        "Non-Rectified": non_rectified_total,
        "Persistent Memory": persistent_memory_total,
        "Not Presented": not_presented_total,
        "Shopping Habit": shopping_habit_total,
        "Missed": missed_total,
        "Days": days_total,
        "Nudges Per Day": missed_total//days_total if days_total else 0,
        "Transactions With Nudges": round((missed_total/total_transactions)*100, 2) if total_transactions else 0,
        "Transactions With No Loss": (
                round((rectified_total / (rectified_total + non_rectified_total)) * 100, 2)
                if rectified_total + non_rectified_total else 0
            ),
        "Number of SCO": total_sco
    }) 

    return res

    return return_encoded_data(res)


@router.get("/overall-details-report-download", status_code=status.HTTP_200_OK)
def get_overall_details_report_(
    store_id: int = None, 
    region_id: int = None,
    zone: str = None,
    start_time: date = None, 
    end_time: date = None,
    sort_by: str = "Store Name", 
    sort_order: str = "asc", 
    hyper_care_stores: bool = False,
    new_dashboard: bool = False, 
    token_data = Depends(auth_handler.auth_wrapper),
    db: Session = Depends(get_db), 
    db_xml: Session = Depends(get_xml_db)
):
    result_stores = fetch_store_details(db, store_id, region_id, zone)
    result_transaction, result_overall_report_sco, result_transaction_comment = (
        get_overall_details_report_v2(db, db_xml, start_time, end_time, store_id, region_id, zone))
    sco_count_result = fetch_live_stores_sco_count(store_id, db)

    transaction_dict = dict()
    for record in result_transaction:
        if record.store_id not in transaction_dict:
            transaction_dict[record.store_id] = {}
        transaction_dict[record.store_id][record.clubcard] = record.nudge_count

    transaction_comment_dict = dict()
    for record in result_transaction_comment:
        if record.store_id not in transaction_comment_dict:
            transaction_comment_dict[record.store_id] = {}
        transaction_comment_dict[record.store_id][record.clubcard] = record.comment_count

    sco_count_dict = {str(record.id): record.sco_count for record in sco_count_result}

    overall_report_sco_dict = {str(record.store_id): record for record in result_overall_report_sco}

    stores_dict = {
        str(record.id): {"name": record.name, "zone": record.zone, "region": record.company_region_id} 
        for record in result_stores
    }

    rectified_total, non_rectified_total, persistent_memory_total, not_presented_total, shopping_habit_total, \
        transaction_with_nudges, days_total, total_transaction, total_sco = 0, 0, 0, 0, 0, 0, 0, 0, 0
    res = []
    for store_id, store_details in stores_dict.items():
        if hyper_care_stores and int(store_id) not in HYPERCARE_STORES:
            continue

        data = dict()
        new_data = dict()
        data["Store Name"] = store_details["name"]
        new_data["Store Name"] = store_details["name"]
        data["Zone"] = store_details["zone"]
        new_data["Zone"] = store_details["zone"]
        data["Region"] = store_details["region"]
        new_data["Region"] = store_details["region"]
        data["Total Transactions"] = int(overall_report_sco_dict[store_id].total_transactions) \
            if store_id in overall_report_sco_dict else 0
        new_data["Total Transactions"] = data["Total Transactions"]

        if store_id in transaction_dict and "Rectified" in transaction_dict[store_id]:
            data["Rectified"] = int(transaction_dict[store_id]["Rectified"])
        else:
            data["Rectified"] = 0
        if (store_id in transaction_comment_dict and "Rectified" in transaction_comment_dict[store_id]
                and data["Rectified"]):
            data["Rectified"] -= int(transaction_comment_dict[store_id]["Rectified"])
        new_data["Corrected"] = data["Rectified"]

        if store_id in transaction_dict and "Non-Rectified" in transaction_dict[store_id]:
            data["Non-Rectified"] = int(transaction_dict[store_id]["Non-Rectified"])
        else:
            data["Non-Rectified"] = 0
        if (store_id in transaction_comment_dict and "Non-Rectified" in transaction_comment_dict[store_id]
                and data["Non-Rectified"]):
            data["Non-Rectified"] -= int(transaction_comment_dict[store_id]["Non-Rectified"])
        new_data["Failed"] = data["Non-Rectified"]

        if not new_dashboard:
            if store_id in transaction_dict and "Persistent Memory" in transaction_dict[store_id]:
                data["Persistent Memory"] = int(transaction_dict[store_id]["Persistent Memory"])
            else:
                data["Persistent Memory"] = 0
            if (store_id in transaction_comment_dict and "Persistent Memory" in transaction_comment_dict[store_id] and
                    data["Persistent Memory"]):
                data["Persistent Memory"] -= int(transaction_comment_dict[store_id]["Persistent Memory"])

        if store_id in transaction_dict and "Not-Present" in transaction_dict[store_id]:
            data["Not Presented"] = int(transaction_dict[store_id]["Not-Present"])
        else:
            data["Not Presented"] = 0
        if (store_id in transaction_comment_dict and "Not-Present" in transaction_comment_dict[store_id]
                and data["Not Presented"]):
            data["Not Presented"] -= int(transaction_comment_dict[store_id]["Not-Present"])
        new_data["Monitored"] = data["Not Presented"]

        if not new_dashboard:
            data["Shopping Habit"] = sum(transaction_comment_dict[store_id].values()) \
                if store_id in transaction_comment_dict else 0

        if new_dashboard:
            data["Transactions with nudges"] = (data["Rectified"] + data["Non-Rectified"] + data["Not Presented"])
        else:
            data["Transactions with nudges"] = (data["Rectified"] + data["Non-Rectified"] + data["Persistent Memory"] + data["Not Presented"]
                              + data["Shopping Habit"])
        new_data["Transactions with Nudges"] = data["Transactions with nudges"]

        data["Days"] = int(overall_report_sco_dict[store_id].days_count) if store_id in overall_report_sco_dict else 0
        new_data["Days"] = data["Days"]

        data["Nudges Per Day"] = data["Transactions with nudges"] // data["Days"] if data["Days"] else 0
        new_data["Nudges Per Day"] = data["Nudges Per Day"]

        data["% Transactions With Nudges"] = round(
            (data["Transactions with nudges"] / data["Total Transactions"]) * 100, 2
        ) if data["Total Transactions"] else 0
        new_data["% Transactions With Nudges"] = data["% Transactions With Nudges"]

        data["Number of SCO"] = sco_count_dict[store_id] if store_id in sco_count_dict else 0
        new_data["Number of SCO"] = data["Number of SCO"]

        data["% Transactions With No Loss"] = round(
            (data["Rectified"] / (data["Rectified"] + data["Non-Rectified"])) * 100, 2
        ) if (data["Rectified"] + data["Non-Rectified"]) else 0
        new_data["% Transactions With No Loss"] = data["% Transactions With No Loss"]

        rectified_total += data["Rectified"]
        non_rectified_total += data["Non-Rectified"]
        not_presented_total += data["Not Presented"]
        transaction_with_nudges += data["Transactions with nudges"]
        days_total += data["Days"]
        total_transaction += data["Total Transactions"]
        total_sco += data["Number of SCO"]
        if not new_dashboard:
            persistent_memory_total += data["Persistent Memory"]
            shopping_habit_total += data["Shopping Habit"]

        if not new_dashboard:
            res.append(data)
        else:
            res.append(new_data)

    if sort_order == "DESC":
        sort_order = True
    else:
        sort_order = False
    if not sort_by:
        sort_by = "Store Name"
    res.sort(key=lambda x: x[sort_by], reverse=sort_order)

    if not new_dashboard:
        res.append({
            "Store Name": None,
            "Zone": None,
            "Region": None,
            "Total Transactions": total_transaction,
            "Rectified": rectified_total,
            "Non-Rectified": non_rectified_total,
            "Persistent Memory": persistent_memory_total,
            "Not Presented": not_presented_total,
            "Shopping Habit": shopping_habit_total,
            "Transactions with Nudges": transaction_with_nudges,
            "Days": days_total,
            "Nudges Per Day": transaction_with_nudges // days_total if days_total else 0,
            "% Transactions With Nudges": (
                round((transaction_with_nudges / total_transaction) * 100, 2) 
                if total_transaction else 0
            ),
            "Number of SCO": total_sco
        })
    else:
        res.append({
            "Store Name": None,
            "Region": None,
            "Zone": None,
            "Total Transactions": total_transaction,
            "Transactions with Nudges": transaction_with_nudges,
            "% Transactions With Nudges": (
                round((transaction_with_nudges / total_transaction) * 100, 2) 
                if total_transaction else 0
            ),
            "Nudges Per Day": transaction_with_nudges // days_total if days_total else 0,
            "Corrected": rectified_total,
            "Failed": non_rectified_total,
            "% Transactions With No Loss": (
                round((rectified_total / (rectified_total + non_rectified_total)) * 100, 2)
                if (rectified_total + non_rectified_total) else 0
            )
        })

    old_columns = [
        "Store Name", "Zone", "Region", "Total Transactions", "Rectified", "Non-Rectified", "Persistent Memory", 
        "Not Presented", "Shopping Habit", "Transactions with nudges", "Days", "Nudges Per Day", 
        "% Transactions With Nudges", "Number of SCO"
    ]
    new_columns = [
        "Store Name", "Region", "Zone", "Total Transactions", "Transactions with Nudges", "% Transactions With Nudges", 
        "Corrected", "Failed", "% Transactions With No Loss", "Nudges Per Day"
    ]

    if not new_dashboard:
        df = pd.DataFrame(res, columns=old_columns)
    else:
        df = pd.DataFrame(res, columns=new_columns)

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)

    buffer.seek(0)
    return StreamingResponse(
        buffer, 
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=overall_details.xlsx"
        }
    )


@router.get("/send-camera-status-report", status_code=status.HTTP_200_OK)
def send_camera_status_report(db: Session = Depends(get_db)):
    data_camera, data_store = fetch_store_status_for_report(db)

    check_hours = 360
    # check_hours = 4

    data_camera_filter, data_store_filter = [], []
    for record in data_camera:
        record = dict(record)
        if (
            record["Last Seen"] == "Never" or 
            datetime.strptime(record["Last Seen"], "%Y-%m-%d %H:%M:%S") <= (datetime.now() - timedelta(hours=check_hours))
        ):
            data_camera_filter.append(record)
    
    for record in data_store:
        record = dict(record)
        if (
            record["Last Seen"] == "Never" or 
            datetime.strptime(record["Last Seen"], "%Y-%m-%d %H:%M:%S") <= (datetime.now() - timedelta(hours=check_hours))
        ):
            data_store_filter.append(record)

    camera_report_columns = ["Store ID", "Store Name", "Camera IP", "POS ID", "Status", "Checked On", "Last Seen"]
    store_report_columns = ["Store ID", "Store Name", "Status", "Checked On", "Last Seen"]

    # df_camera = pd.DataFrame(data_camera, columns=camera_report_columns)
    # df_store = pd.DataFrame(data_store, columns=store_report_columns)
    df_camera = pd.DataFrame(data_camera_filter, columns=camera_report_columns)
    df_store = pd.DataFrame(data_store_filter, columns=store_report_columns)

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df_camera.to_excel(writer, sheet_name="Cameras", index=False)
        df_store.to_excel(writer, sheet_name="Stores", index=False)

    buffer.seek(0)

    # send_mail(buffer.read())

    return StreamingResponse(buffer, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={
                                 "Content-Disposition": "attachment; filename=report.xlsx"
                             })


#new api
@router.get("/overall-details-report-description", status_code=status.HTTP_200_OK)
def get_overall_details_report_description(
    store_id: int = None, 
    start_time: date = None, 
    end_time: date = None,
    db: Session = Depends(get_db),
    db_xml: Session = Depends(get_xml_db),
    token_data = Depends(auth_handler.auth_wrapper)
):
    result_stores = fetch_store_details(db)
    result_transaction, result_overall_report_sco = (
        get_overall_details_report_v3(db, db_xml, start_time, end_time, store_id))

    transaction_dict = dict()
    for record in result_transaction:
        if record.store_id not in transaction_dict:
            transaction_dict[record.store_id] = {}
        transaction_dict[record.store_id][record.description] = record.nudge_count
        transaction_dict[record.store_id][record.description + " missed_item_count"] = record.missed_item_count

    overall_report_sco_dict = {str(record.store_id): record for record in result_overall_report_sco}

    stores_dict = {str(record.id): record.name for record in result_stores}

    store_ids = set(list(transaction_dict.keys()) + list(overall_report_sco_dict.keys()))

    # Initialize counts for totals
    incomplete_payment_count, incomplete_payment_item_count, total_transactions = 0, 0, 0
    in_hand_count, in_hand_item_count = 0, 0
    missed_scan_count, missed_scan_item_count = 0, 0
    stacking_count, stacking_item_count = 0, 0
    switching_count, switching_item_count = 0, 0
    on_scanner_count, on_scanner_item_count = 0, 0
    shopping_habit_count, shopping_habit_item_count = 0, 0
    basket_count, basket_item_count = 0, 0
    trolley_count, trolley_item_count = 0, 0
    total_count, total_item_count = 0, 0  # Add new totals

    res = []
    for store_id in store_ids:
        if store_id not in stores_dict:
            continue

        data = dict()
        data["Store Name"] = stores_dict[store_id]
        data["Total Transactions"] = overall_report_sco_dict[store_id].total_transactions \
            if store_id in overall_report_sco_dict else 0

        # Initialize local counts for the current store
        store_total_count, store_total_item_count = 0, 0

        descriptions = ["Incomplete Payment", "In Hand", "Item Missed Scan", "Item Stacking", "Item Switching", 
                        "On Scanner" , "Basket", "Trolley"]

        for description in descriptions:
            count_key = description + " Count"
            item_count_key = description + " Item Count"

            # Fetch counts from transaction_dict if available, otherwise default to 0
            count = transaction_dict.get(store_id, {}).get(description, 0)
            item_count = transaction_dict.get(store_id, {}).get(description + " missed_item_count", 0)

            data[count_key] = count
            data[item_count_key] = item_count

            # Accumulate totals
            store_total_count += count
            store_total_item_count += item_count
            total_count += count
            total_item_count += item_count

            # Add to the overall counts
            if description == "Incomplete Payment":
                incomplete_payment_count += count
                incomplete_payment_item_count += item_count
            elif description == "In Hand":
                in_hand_count += count
                in_hand_item_count += item_count
            elif description == "Item Missed Scan":
                missed_scan_count += count
                missed_scan_item_count += item_count
            elif description == "Item Stacking":
                stacking_count += count
                stacking_item_count += item_count
            elif description == "Item Switching":
                switching_count += count
                switching_item_count += item_count
            elif description == "On Scanner":
                on_scanner_count += count
                on_scanner_item_count += item_count
            elif description == "Basket":
                basket_count += count
                basket_item_count += item_count
            elif description == "Trolley":
                trolley_count += count
                trolley_item_count += item_count
            # elif description == "Shopping Habit":
            #     shopping_habit_count += count
            #     shopping_habit_item_count += item_count

        # Add the store-level totals
        data["Total Count"] = store_total_count
        data["Total Item Count"] = store_total_item_count

        res.append(data)

    res.sort(key=lambda x: x["Store Name"])

    # Add summary row with totals
    res.append({
        "Store Name": "Total",
        "Total Transactions": sum([x["Total Transactions"] for x in res]),
        "Total Count": total_count,
        "Total Item Count": total_item_count,
        "Incomplete Payment Count": incomplete_payment_count,
        "Incomplete Payment Item Count": incomplete_payment_item_count,
        "In Hand Count": in_hand_count,
        "In Hand Item Count": in_hand_item_count,
        "Item Missed Scan Count": missed_scan_count,
        "Item Missed Scan Item Count": missed_scan_item_count,
        "Item Stacking Count": stacking_count,
        "Item Stacking Item Count": stacking_item_count,
        "Item Switching Count": switching_count,
        "Item Switching Item Count": switching_item_count,
        "On Scanner Count": on_scanner_count,
        "On Scanner Item Count": on_scanner_item_count,
        "Basket Count" : basket_count,
        "Basket Item Count" : basket_item_count,
        "Trolley Count": trolley_count,
        "Trolley Item Count": trolley_item_count
        # "Shopping Habit Count": shopping_habit_count,
        # "Shopping Habit Item Count": shopping_habit_item_count
    })

    return res


@router.get("/operator-loss-details", status_code=status.HTTP_200_OK)
def get_operator_loss_details(
    store_id: int = None, 
    start_time: date = None, 
    end_time: date = None,
    sort_by: str = "store_name", 
    sort_order: str = "ASC",
    page: int = 1, 
    per_page: int = 10,
    db: Session = Depends(get_db),
    token_data = Depends(auth_handler.auth_wrapper)
):
    result = fetch_operator_losses_data(db, store_id, start_time, end_time)

    res = [dict(record) for record in result]

    if sort_order == "DESC":
        sort_order = True
    elif sort_order == "ASC":
        sort_order = False

    res.sort(key=lambda x: x[sort_by], reverse=sort_order)
    count = len(result)

    return {"data": res[(page-1)*per_page: page*per_page], "total": count}


@router.get("/operator-loss-details-download", status_code=status.HTTP_200_OK)
def get_operator_loss_details(
    store_id: int = None, 
    start_time: date = None, 
    end_time: date = None,
    sort_by: str = "store_name", 
    sort_order: str = "ASC",
    db: Session = Depends(get_db),
    token_data = Depends(auth_handler.auth_wrapper)
):
    result = fetch_operator_losses_data(db, store_id, start_time, end_time)
    res = []
    for record in result:
        res.append({
            "Store ID": record["store_id"],
            "Store Name": record["store_name"],
            "Operator ID": record["operator_id"],
            "Number Of Transactions With No Loss": record["transactions_with_no_loss"],
            "Number Of Transactions With Loss": record["transactions_with_loss"]
        })

    sort_key_mapping = {
        "store_id": "Store ID",
        "store_name": "Store Name",
        "operator_id": "Operator ID",
        "transactions_with_no_loss": "Number Of Transactions With No Loss",
        "transactions_with_loss": "Number Of Transactions With Loss"
    }

    if sort_order == "DESC":
        sort_order = True
    elif sort_order == "ASC":
        sort_order = False
    
    report_columns = ["Store ID", "Store Name", "Operator ID", "Number Of Transactions With No Loss", 
                      "Number Of Transactions With Loss"]

    res.sort(key=lambda x: x[sort_key_mapping.get(sort_by, "Number Of Transactions With Loss")], reverse=sort_order)

    df = pd.DataFrame(res, columns=report_columns)

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)

    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={
                                 "Content-Disposition": "attachment; filename=operator_loss_details.xlsx"
                             })
