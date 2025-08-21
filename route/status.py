import json
from collections import defaultdict
from datetime import date, datetime
from enum import Enum
from io import BytesIO
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, status, HTTPException, UploadFile, File, Query
from fastapi.params import Depends
from fastapi.security import HTTPBearer
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

import config
from database import get_db
from model.model import AisleImages
from crud.login import AuthHandler
from crud.aisle import get_camera_number
from crud.store import fetch_store_details
from crud.status import (
    get_all_status_message,
    get_all_status,
    get_status_summary_data,
    get_store_map_issue_data,
    fetch_camera_status_images,
    fetch_all_latest_status,
    fetch_vtc_data,
    fetch_jitter_data,
    fetch_camera_last_update_time,
    fetch_store_status_for_report,
    fetch_active_status_stores,
    fetch_camera_status_for_report,
    fetch_system_status_for_report,
    fetch_stream_data_for_report,
    fetch_stores_status_summary,
    fetch_live_store_status,
    fetch_data_application_store_status_table,
    fetch_data_application_store_status_table_v2,
    update_application_store_status,
    update_application_store_status_v2,
    update_all_application_store_status,
    update_application_store_tech_support,
    update_application_store_tech_support_status
)
from utils.status_utils import determine_status_message, get_issue_message
from utils.general import get_serialized_object, send_application_monitoring_mail, get_table_email_body
from secure_payload import return_encoded_data
from serializers.request.status import (
    StatusUpdateRequest, TechSupportUpdateRequest, ApplicationStatusRequestModel, AllStatusUpdateRequest
)

# Initialize router and authentication
router = APIRouter()
security = HTTPBearer()
auth_handler = AuthHandler()

# Constants
# CAMERA_REPORT_COLUMNS = ["Store ID", "Store Name", "Camera IP", "POS ID", "Status", "Checked On", "Last Seen"]
CAMERA_REPORT_COLUMNS = ["Store ID", "Store Name", "Camera IP", "POS ID", "Status", "Checked On", "Last Seen"]
STORE_REPORT_COLUMNS = ["Store ID", "Store Name", "Status", "Checked On", "Last Seen"]
REPORT_FILENAME = "StoreCameraStatusReport.xlsx"


class StatusType(str, Enum):
    """Valid status types for the status endpoint."""
    CAMERA = "camera"
    SYSTEM = "system"


class CameraStatusFilter(str, Enum):
    """Valid status filters for the status endpoint."""
    MIS_CONFIGURED = "Misconfigured"
    STREAM_BREAKAGE = "Stream Breakage"
    JITTER = "Jittery"
    PINGING = "Pinging"
    NOT_PINGING = "Not Pinging"


@router.get("/camera-status-filter", status_code=status.HTTP_200_OK)
def get_camera_status_filter(db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    """
    Get camera status filter.
    """
    return return_encoded_data({"data": [i.value for i in CameraStatusFilter]})


# Status Message Endpoints
@router.get("/status-message", status_code=status.HTTP_200_OK)
def get_status_message(store_id: int, db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    """
    Get status messages for cameras and system for a specific store.
    
    Args:
        store_id: ID of the store
        
    Returns:
        dict: Status messages for cameras and system, and the last checked timestamp
    """
    try:
        result_camera, result_system = get_all_status_message(db, store_id)
        aisle_images = db.query(
            AisleImages.id, AisleImages.images, AisleImages.label
        ).filter(
            AisleImages.store_id == store_id
        ).all()

        aisle_images_set = set()
        for item in aisle_images:
            img = item["images"].split(".")
            if len(img) > 2:
                camera_no = int(img[-2])
            else:
                camera_no = int(img[0])
            aisle_images_set.add(camera_no)

        if result_camera:
            last_checked = result_camera[0]["updated_at"]
        else:
            last_checked = ""

        temp = []
        for record in result_camera:
            if int(record["camera_no"]) in aisle_images_set:
                temp.append(record['current_status'])

        result_camera = temp
        result_system = [record["current_status"] for record in result_system]
        
        camera_message, system_message, _ = determine_status_message(result_camera, result_system)

        return {"camera": camera_message, "system": system_message, "last_checked": last_checked}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving status message: {str(e)}")


@router.get("/status", status_code=status.HTTP_200_OK)
def get_status(
    type: StatusType, 
    store_id: int, 
    camera_no: int, 
    from_date: date = None, 
    to_date: date = None,
    total_seconds: int = 1800, 
    page: int = 1, 
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    """
    Get status data for a specific store and camera.
    
    Args:
        type: Type of status to retrieve (camera or system)
        store_id: ID of the store
        camera_no: Camera number
        from_date: Start date for filtering (optional)
        to_date: End date for filtering (optional)
        total_seconds: Total seconds for time window (default: 1800)
        page: Page number for pagination (default: 1)
        
    Returns:
        dict: Status data
    """
    try:
        result = get_all_status(db, type, store_id, camera_no, from_date, to_date, total_seconds, page)
        return result
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving status data: {str(e)}")


@router.get("/status-summary", status_code=status.HTTP_200_OK)
def get_status_summary(store_id: int, db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    """
    Get a summary of status data for a specific store.
    
    Args:
        store_id: ID of the store
        
    Returns:
        dict: Status summary data for the store
    """
    try:
        result = get_status_summary_data(store_id, db)
        return result
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving status summary: {str(e)}")


# Store Map Data Endpoints
@router.get("/get_store_map_data_v2", status_code=status.HTTP_200_OK)
def get_store_map_data_v2(
    store_id: int = None, 
    region_id: int = None, 
    area_id: int = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    """
    Get store map data with status information.
    
    Args:
        store_id: ID of the store (optional)
        region_id: ID of the region (optional)
        area_id: ID of the area (optional)
        
    Returns:
        dict: Store map data with status information
    """
    try:
        if not store_id:
            store_ids = []
        else:
            store_ids = [store_id]

        # Get store data
        result = get_store_map_issue_data(store_ids, region_id, area_id, db)
        
        # Get all camera and system status data in one query
        all_camera_result, all_system_result = get_all_status_message(db, None)
        
        # Organize camera data by store_id
        all_camera = {}
        for record in all_camera_result:
            key = str(record["store_id"])
            if key not in all_camera:
                all_camera[key] = []
            all_camera[key].append(record)

        # Organize system data by store_id
        all_system = {}
        for record in all_system_result:
            key = str(record["store_id"])
            if key not in all_system:
                all_system[key] = []
            all_system[key].append(record)
            
        # Get all aisle images for all stores in one query
        store_ids_list = [record["id"] for record in result]
        all_aisle_images = db.query(AisleImages.store_id, AisleImages.images) \
            .filter(AisleImages.store_id.in_(store_ids_list)) \
            .all()
            
        # Organize aisle images by store_id
        aisle_images_by_store = {}
        for item in all_aisle_images:
            store_id = item["store_id"]
            if store_id not in aisle_images_by_store:
                aisle_images_by_store[store_id] = []
            aisle_images_by_store[store_id].append(item["images"])

        res = {"data": [], "count": len(result)}
        for record in result:
            store_id = record["id"]

            # Get camera and system status for this store
            result_camera = all_camera.get(str(store_id), [])
            result_system = all_system.get(str(store_id), [])

            # Get aisle images for this store
            aisle_images = aisle_images_by_store.get(store_id, [])
            aisle_images_set = set([int(get_camera_number(image)) for image in aisle_images])

            # Filter camera status by aisle images
            filtered_camera = []
            for r in result_camera:
                if int(r["camera_no"]) in aisle_images_set:
                    filtered_camera.append(r['current_status'])

            filtered_system = [r["current_status"] for r in result_system]

            # Determine status message and category color
            camera_message, system_message, category_color = determine_status_message(filtered_camera, filtered_system)
            
            # Determine issue message
            issue = get_issue_message(camera_message, system_message)

            res["data"].append({
                "id": record["id"],
                "name": record["name"],
                "region_id": record["region_id"],
                "area_id": record["area_id"],
                "latitude": record["latitude"],
                "longitude": record["longitude"],
                "issue": issue,
                "category_color": category_color
            })

        return res
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving store map data: {str(e)}")


# Camera Status Endpoints
@router.get("/camera-status-images", status_code=status.HTTP_200_OK)
def get_camera_status_images(
    store_id: str, 
    db: Session = Depends(get_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    """
    Get camera status images for a specific store.
    
    Args:
        store_id: ID of the store
        
    Returns:
        dict: Camera status images
    """
    try:
        return fetch_camera_status_images(store_id, db)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving camera status images: {str(e)}")


@router.get("/camera-last-update", status_code=status.HTTP_200_OK)
def get_camera_last_update_time(db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    """
    Get the last update time for all cameras.
    
    Returns:
        dict: Last update time for all cameras
    """
    try:
        return return_encoded_data(fetch_camera_last_update_time(db))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving camera last update time: {str(e)}")
    

@router.get("/camera-status-report", status_code=status.HTTP_200_OK)
def get_camera_status_report(
    store_id: int = None, 
    region_id: int = None,
    client_region_id: int = None,
    area_id: int = None, 
    page: int = 1,
    per_page: int = 10, 
    db: Session = Depends(get_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    """
    Get camera status report.
    
    Args:
        store_id: ID of the store (optional)
        region_id: ID of the region (optional)
        area_id: ID of the area (optional)
        page: Page number for pagination (default: 1)
        per_page: Number of items per page (default: 10)
        
    Returns:
        dict: Camera status report data and count
    """
    try:
        camera_result = fetch_camera_status_for_report(db, store_id, region_id, area_id, client_region_id)

        return return_encoded_data(
            {
                "data": camera_result[(page - 1) * per_page: page * per_page], 
                "count": len(camera_result)
            }
        )
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving camera status report: {str(e)}")


@router.get("/camera-status-report-v2", status_code=status.HTTP_200_OK)
def get_camera_status_report_v2(
    store_id: int = None, 
    region_id: int = None,
    client_region_id: int = None,
    area_id: int = None,
    status_filter: str = None,
    page: int = 1,
    per_page: int = 10, 
    db: Session = Depends(get_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    """
    Get camera status report.
    
    Args:
        store_id: ID of the store (optional)
        region_id: ID of the region (optional)
        area_id: ID of the area (optional)
        page: Page number for pagination (default: 1)
        per_page: Number of items per page (default: 10)
        
    Returns:
        dict: Camera status report data and count
    """
    try:
        camera_result = fetch_camera_status_for_report(db, store_id, region_id, area_id, client_region_id)
        stream_result = fetch_stream_data_for_report(db, store_id, region_id, area_id, client_region_id)
        latest_status_result = fetch_all_latest_status(db, store_id, region_id, area_id, client_region_id)
        vtc_result = fetch_vtc_data(db, store_id, region_id, area_id, client_region_id)
        jitter_result = fetch_jitter_data(db, store_id, region_id, area_id, client_region_id)

        camera_result_dict = {f"{record['store_id_dashboard']}_{record['camera_no']}": record for record in camera_result}
        latest_status_dict = {f"{record.store_id_dashboard}_{record.camera_no}": record for record in latest_status_result}
        vtc_dict = {f"{record.store_id_dashboard}_{record.pos_id}": record for record in vtc_result}
        jitter_dict = {f"{record.store_id_dashboard}_{record.pos_id}": record for record in jitter_result}

        stream_res = []
        for record in stream_result:
            record = dict(record)
            key = f"{record['store_id_dashboard']}_{record['camera_no']}"
            key_pos_id = f"{record['store_id_dashboard']}_{record['pos_id']}"

            if key in camera_result_dict:
                continue
            
            current_time = datetime.now().replace(minute=0, second=0, microsecond=0)
            record["checked_on"] = datetime.strftime(record["checked_on"], "%Y-%m-%d %H:%M:%S")
            if record["fps"] < 25 or record["bitrate"] < 400 or record["frame_width"] != 640 or record["frame_height"] != 480:
                record["status"] = "Misconfigured"
            if key in latest_status_dict:
                record["last_seen"] = latest_status_dict[key].last_seen
            else:
                record["last_seen"] = "Never"
            if key_pos_id in vtc_dict:
                record["status"] = "Stream Breakage"
            if key_pos_id in jitter_dict and jitter_dict[key_pos_id].created_at >= current_time:
                record["status"] = "Jittery"
            
            del record["fps"]
            del record["bitrate"]
            del record["frame_width"]
            del record["frame_height"]
            
            if record["status"] != "pinging":
                stream_res.append(record)

        res = [dict(record) for record in camera_result]
        res += [dict(record) for record in stream_res]

        if status_filter:
            res = [record for record in res if record["status"] == status_filter]

        res.sort(key=lambda x: (x["store_name"], x["camera_no"]))

        return return_encoded_data({"data": res[(page - 1) * per_page: page * per_page], "count": len(res)})
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving camera status report: {str(e)}")


# System Status Endpoints
@router.get("/system-status-report", status_code=status.HTTP_200_OK)
def get_system_status_report(
    store_id: int = None, 
    region_id: int = None,
    client_region_id: int = None,
    area_id: int = None, 
    page: int = 1,
    per_page: int = 10, 
    db: Session = Depends(get_db),
    token_data=Depends(auth_handler.auth_wrapper)
):
    """
    Get system status report.
    
    Args:
        store_id: ID of the store (optional)
        region_id: ID of the region (optional)
        area_id: ID of the area (optional)
        page: Page number for pagination (default: 1)
        per_page: Number of items per page (default: 10)
        
    Returns:
        dict: System status report data and count
    """
    try:
        result, count = fetch_system_status_for_report(db, store_id, region_id, area_id, page, per_page,
                                                       client_region_id)
        print(result)
        return return_encoded_data({"data": result, "count": count})
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving system status report: {str(e)}")


@router.get("/store-status-report", status_code=status.HTTP_200_OK)
def get_store_status_report(store_id: int = None, region_id: int = None, client_region_id: int = None,
                            db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    """
    Generate an Excel report with store and camera status information.
    
    This endpoint returns a StreamingResponse with an Excel file containing two sheets:
    1. Cameras: Information about camera status for each store
    2. Stores: Information about store status
    
    Returns:
        StreamingResponse: Excel file as a downloadable attachment
    """
    try:
        data_camera, data_store = fetch_store_status_for_report(db, store_id, region_id, client_region_id)

        df_camera = pd.DataFrame(data_camera, columns=CAMERA_REPORT_COLUMNS)
        df_store = pd.DataFrame(data_store, columns=STORE_REPORT_COLUMNS)

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df_camera.to_excel(writer, sheet_name="Cameras", index=False)
            df_store.to_excel(writer, sheet_name="Stores", index=False)

        buffer.seek(0)

        return StreamingResponse(
            buffer, 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={REPORT_FILENAME}"}
        )
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error generating store status report: {str(e)}")


@router.get("/store-status-report-v2", status_code=status.HTTP_200_OK)
def get_store_status_report_v2(store_id: int = None, 
    region_id: int = None,
    client_region_id: int = None,
    area_id: int = None,
    status_filter: str = None,
    db: Session = Depends(get_db),
    token_data=Depends(auth_handler.auth_wrapper)):
    """
    Generate an Excel report with store and camera status information.
    
    This endpoint returns a StreamingResponse with an Excel file containing two sheets:
    1. Cameras: Information about camera status for each store
    2. Stores: Information about store status
    
    Returns:
        StreamingResponse: Excel file as a downloadable attachment
    """
    try:
        data_camera, data_store = fetch_store_status_for_report(db, store_id, region_id, client_region_id)
        camera_result = fetch_camera_status_for_report(db, store_id, region_id, area_id, client_region_id)
        stream_result = fetch_stream_data_for_report(db, store_id, region_id, area_id, client_region_id)
        latest_status_result = fetch_all_latest_status(db, store_id, region_id, area_id, client_region_id)
        vtc_result = fetch_vtc_data(db, store_id, region_id, area_id, client_region_id)
        jitter_result = fetch_jitter_data(db, store_id, region_id, area_id, client_region_id)

        camera_result_dict = {f"{record['store_id_dashboard']}_{record['camera_no']}": record for record in camera_result}
        latest_status_dict = {f"{record.store_id_dashboard}_{record.camera_no}": record for record in latest_status_result}
        vtc_dict = {f"{record.store_id_dashboard}_{record.pos_id}": record for record in vtc_result}
        jitter_dict = {f"{record.store_id_dashboard}_{record.pos_id}": record for record in jitter_result}

        stream_res = []
        for record in stream_result:
            record = dict(record)
            key = f"{record['store_id_dashboard']}_{record['camera_no']}"
            key_pos_id = f"{record['store_id_dashboard']}_{record['pos_id']}"

            if key in camera_result_dict:
                continue
            
            current_time = datetime.now().replace(minute=0, second=0, microsecond=0)
            record["checked_on"] = datetime.strftime(record["checked_on"], "%Y-%m-%d %H:%M:%S")
            if record["fps"] < 25 or record["bitrate"] < 400 or record["frame_width"] != 640 or record["frame_height"] != 480:
                record["status"] = "Misconfigured"
            if key in latest_status_dict:
                record["last_seen"] = latest_status_dict[key].last_seen
            else:
                record["last_seen"] = "Never"
            if key_pos_id in vtc_dict:
                record["status"] = "Stream Breakage"
            if key_pos_id in jitter_dict and jitter_dict[key_pos_id].created_at >= current_time:
                record["status"] = "Jittery"
            
            if record["status"] != "pinging":
                stream_res.append(record)

        res = [dict(record) for record in camera_result]
        res += [dict(record) for record in stream_res]

        if status_filter:
            res = [record for record in res if record["status"] == status_filter]

        res.sort(key=lambda x: (x["store_name"], x["camera_no"]))

        data_camera = []
        for record in res:
            data_camera.append({
                "Store ID": record["store_id"],
                "Store Name": record["store_name"],
                "Camera IP": record["camera_ip"],
                "POS ID": record["pos_id"],
                "Status": record["status"],
                "Checked On": record["checked_on"],
                "Last Seen": record["last_seen"]
            })

        df_camera = pd.DataFrame(data_camera, columns=CAMERA_REPORT_COLUMNS)
        df_store = pd.DataFrame(data_store, columns=STORE_REPORT_COLUMNS)

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df_camera.to_excel(writer, sheet_name="Cameras", index=False)
            df_store.to_excel(writer, sheet_name="Stores", index=False)

        buffer.seek(0)

        return StreamingResponse(
            buffer, 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={REPORT_FILENAME}"}
        )
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error generating store status report: {str(e)}")
    

# Dropdown and Summary Endpoints
@router.get("/dropdown-status-stores", status_code=status.HTTP_200_OK)
def get_dropdown_status_stores(db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    """
    Get a list of active stores for dropdown menus.
    
    Returns:
        list: Active stores for dropdown menus
    """
    try:
        data = fetch_active_status_stores(db)
        data = get_serialized_object(data)
        return return_encoded_data(data)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving dropdown status stores: {str(e)}")


@router.get("/status-result", status_code=status.HTTP_200_OK)
def get_status_statistics(
    store_id: int = None, 
    region_id: int = None,
    db: Session = Depends(get_db), 
    token_data=Depends(auth_handler.auth_wrapper)
):
    """
    Get summary statistics of store status.
    
    Args:
        store_id: ID of the store (optional)
        region_id: ID of the region (optional)
        
    Returns:
        dict: Count of online, offline, and warning stores
    """
    try:
        result_camera, result_store = fetch_stores_status_summary(store_id, region_id, db)

        camera_online, camera_warning, camera_offline = 0, 0, 0
        store_online, store_offline = 0, 0
        for record in result_camera:
            if record.online == record.total:
                camera_online += 1
            elif record.offline == record.total:
                camera_offline += 1
            else:
                camera_warning += 1
        
        for record in result_store:
            if record.online == 1:
                store_online += 1
            elif record.offline == 1:
                store_offline += 1

        return {
            "online": camera_online, "offline": camera_offline, "warning": camera_warning,
            "camera": {
                "online": camera_online, "offline": camera_offline, "warning": camera_warning,
            },
            "store": {
                "online": store_online, "offline": store_offline,
            }
        }
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving status statistics: {str(e)}")
    

@router.get("/live-store-status", status_code=status.HTTP_200_OK)
def get_live_store_status(
    store_id: int = None, 
    region_id: int = None,
    client_region_id: int = None,
    page: int = 1,
    per_page: int = 10,
    db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    """
    Get live store status data.
    
    Returns:
        dict: Live store status data
    """
    try:
        result = fetch_live_store_status(store_id, region_id, client_region_id, db)
        stream_result = fetch_stream_data_for_report(
            db=db, store_id=store_id, region_id=region_id, client_region_id=client_region_id
        )
        stream_dict = {f"{record['store_id_dashboard']}_{record['pos_id']}": record for record in stream_result}

        data = []
        for record in result:
            record = dict(record)
            key = f"{record['store_id_dashboard']}_{record['pos_id']}"

            record["fps"] = ""
            record["bitrate"] = ""
            record["frame_width"] = ""
            record["frame_height"] = ""

            current_time = datetime.now().replace(minute=0, second=0, microsecond=0)
            if key in stream_dict and stream_dict[key]["checked_on"] >= current_time:
                record["fps"] = f'{stream_dict[key]["fps"]} FPS' if stream_dict[key]["fps"] else ""
                record["bitrate"] = f'{stream_dict[key]["bitrate"]} kb/sec' if stream_dict[key]["bitrate"] else ""
                record["frame_width"] = stream_dict[key]["frame_width"]
                record["frame_height"] = stream_dict[key]["frame_height"]

            data.append(record)
        
        res = {"data": data[(page - 1) * per_page:page * per_page], "count": len(result)}
        res["data"] = get_serialized_object(res["data"])

        return return_encoded_data(res)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                           detail=f"Error retrieving live store status: {str(e)}")
    

@router.get("/live-store-status-download", status_code=status.HTTP_200_OK)
def get_live_store_status_download(
    store_id: int = None, 
    region_id: int = None,
    client_region_id: int = None,
    db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    """
    Get live store status data.
    
    Returns:
        dict: Live store status data
    """
    try:
        COLUMNS_NAME = ["Store ID", "Store Name", "Camera IP", "POS ID", "Setup Date", "Bit Rate", "FPS", "Resolution"]
        result = fetch_live_store_status(store_id, region_id, client_region_id, db)

        stream_result = fetch_stream_data_for_report(
            db=db, store_id=store_id, region_id=region_id, client_region_id=client_region_id
        )
        stream_dict = {f"{record['store_id_dashboard']}_{record['pos_id']}": record for record in stream_result}

        data = []
        for record in result:
            record = dict(record)
            key = f"{record['store_id_dashboard']}_{record['pos_id']}"

            record["fps"] = ""
            record["bitrate"] = ""
            record["frame_width"] = ""
            record["frame_height"] = ""
            
            current_time = datetime.now().replace(minute=0, second=0, microsecond=0)
            if key in stream_dict and stream_dict[key]["checked_on"] >= current_time:
                record["fps"] = stream_dict[key]["fps"] or None
                record["bitrate"] = stream_dict[key]["bitrate"] or None
                record["frame_width"] = stream_dict[key]["frame_width"] or None
                record["frame_height"] = stream_dict[key]["frame_height"] or None

            data.append(record)

        res = []
        for record in data:
            res.append({
                "Store ID": record["store_id"],
                "Store Name": record["name"],
                "Camera IP": record["camera_ip"],
                "POS ID": record["pos_id"],
                "FPS": record["fps"] if record["fps"] else None,
                "Bit Rate": record["bitrate"] if record["bitrate"] else None,
                "Resolution": f'{record["frame_width"]} x {record["frame_height"]}' if record["frame_width"] and record["frame_height"] else None,
                "Setup Date": record["setup_date"] if record["setup_date"] else None
            })
        
        df = pd.DataFrame(res, columns=COLUMNS_NAME)

        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name="Cameras", index=False)

        buffer.seek(0)

        return StreamingResponse(
            buffer, 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename=report.xlsx"}
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Error retrieving live store status report: {str(e)}"
        )


@router.get("/application-status", status_code=status.HTTP_200_OK)
def get_application_status(
        store_id: Optional[int] = Query(None, description="Filter by store ID"),
        cam_no: Optional[int] = Query(None, description="Filter by camera number"),
        search_date: Optional[str] = Query(None, description="Search date in YYYY-MM-DD format, defaults to today"),
        app_status: Optional[str] = Query(None, description="Filter by application status"),
        tech_support: Optional[int] = Query(0, description="Filter by technical support"),
        tech_support_status: Optional[int] = Query(0, description="Filter by technical support status"),
        page: int = 1,
        per_page: int = 10,
        db: Session = Depends(get_db)
):
    result = fetch_data_application_store_status_table(db, store_id, cam_no, search_date, app_status, tech_support,
                                                       tech_support_status)

    return {"data": result[(page - 1) * per_page: page * per_page], "count": len(result)}


@router.post("/application-status-v2", status_code=status.HTTP_200_OK)
def get_application_status_v2(body: ApplicationStatusRequestModel, db: Session = Depends(get_db)):
    body = body.dict()
    store_id = body.get("store_ids", [])
    search_query = body.get("search_query")
    search_date = body.get("search_date")
    cam_nos = body.get("cam_nos", None)
    app_status = body.get("app_status")
    tech_support = body.get("tech_support")
    tech_support_status = body.get("tech_support_status")
    page = body.get("page", 1)
    per_page = body.get("per_page", 10)

    result = fetch_data_application_store_status_table_v2(
        db, store_id, cam_nos, search_query, search_date, app_status, tech_support, tech_support_status
    )

    store_wise_data = defaultdict(list)
    for record in result:
        store_wise_data[record.store_actual_id].append(record)

    res = []
    for store_id, issues in store_wise_data.items():
        data = {
            "store_actual_id": store_id,
            "name": issues[0].name if issues else "",
            "client_name": issues[0].client_name if issues else "",
            "store_id": issues[0].store_id if issues else "",
            "created_at": max(map(lambda x: x.created_at, issues)) if issues else "",
            "issues": []
        }
        for record in issues:
            record = dict(record)
            del record["store_actual_id"]
            del record["store_id"]
            del record["name"]
            del record["client_name"]
            data["issues"].append(record)

        if data["issues"]:
            res.append(data)

    res.sort(key=lambda x: x["created_at"], reverse=True)

    return {"data": res[(page - 1) * per_page: page * per_page], "count": len(res)}


@router.get("/application-status-download", status_code=status.HTTP_200_OK)
def get_application_status_download(
        store_id: Optional[int] = Query(None, description="Filter by store ID"),
        cam_no: Optional[int] = Query(None, description="Filter by camera number"),
        search_date: Optional[str] = Query(None, description="Search date in YYYY-MM-DD format, defaults to today"),
        app_status: Optional[str] = Query(None, description="Filter by application status"),
        tech_support: Optional[int] = Query(0, description="Filter by technical support"),
        tech_support_status: Optional[int] = Query(0, description="Filter by technical support status"),
        db: Session = Depends(get_db)
):
    result = fetch_data_application_store_status_table(db, store_id, cam_no, search_date, app_status, tech_support,
                                                             tech_support_status)
    res = []
    for record in result:
        res.append({
            "Store Id": record.store_actual_id,
            "Store Name": record.name,
            "Camera No": record.cam_no,
            "Script Name": record.script_name,
            "Status": record.status,
            "Created At": record.created_at,
            "Retailer Name": record.client_name
        })

    res.sort(key=lambda x: x["Store Name"])

    df = pd.DataFrame(res, columns=["Store Id", "Store Name", "Camera No", "Script Name", "Status", "Created At",
                                    "Retailer Name"])

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)

    buffer.seek(0)
    return StreamingResponse(buffer, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={
                                 "Content-Disposition": "attachment; filename=sainsbury_application_monitoring_report.xlsx"
                             })


@router.post("/application-status-download-v2", status_code=status.HTTP_200_OK)
def get_application_status_download_v2(
    body: ApplicationStatusRequestModel, 
    db: Session = Depends(get_db)
):
    body = body.dict()
    store_id = body.get("store_ids", [])
    search_query = body.get("search_query")
    search_date = body.get("search_date")
    cam_nos = body.get("cam_nos", None)
    app_status = body.get("app_status")
    tech_support = body.get("tech_support")
    tech_support_status = body.get("tech_support_status")

    result = fetch_data_application_store_status_table_v2(
        db=db,
        store_id=store_id,
        cam_no=cam_nos,
        search_query=search_query,
        search_date=search_date,
        app_status=app_status,
        tech_support=tech_support,
        tech_support_status=tech_support_status
    )

    res = []
    for record in result:
        res.append({
            "Store Id": record.store_actual_id,
            "Store Name": record.name,
            "Camera No": record.cam_no,
            "Script Name": record.script_name,
            "Status": record.status,
            "Created At": record.created_at,
            "Retailer Name": record.client_name
        })

    res.sort(key=lambda x: x["Store Name"])

    columns = [
        "Store Id",
        "Store Name",
        "Camera No",
        "Script Name",
        "Status",
        "Created At",
        "Retailer Name"
    ]

    # Create Excel file
    df = pd.DataFrame(res, columns=columns)
    buffer = BytesIO()
    
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)

    buffer.seek(0)
    
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=sainsbury_application_monitoring_report.xlsx"
        }
    )


@router.put("/update-application-status", status_code=status.HTTP_200_OK)
def update_application_status(request: StatusUpdateRequest, db: Session = Depends(get_db)):
    try:
        result = update_application_store_status(
            db=db,
            store_id=request.store_id,
            script_name=request.script_name,
            new_status=request.new_status,
            search_date=request.date,
            cam_no=request.cam_no
        )
        return {"message": f"Updated {result} record(s)"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update status: {str(e)}")


@router.put("/update-application-status-v2", status_code=status.HTTP_200_OK)
def update_application_status_v2(data: List[StatusUpdateRequest], db: Session = Depends(get_db)):
    try:
        result = update_application_store_status_v2(
            db=db,
            data=data
        )

        return {"message": f"Updated {result} record(s)"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update status: {str(e)}")


@router.put("/update-all-application-status", status_code=status.HTTP_200_OK)
def update_all_application_status(data: AllStatusUpdateRequest, db: Session = Depends(get_db)):
    data = data.dict()
    store_id = data.get("store_id")

    try:
        result = update_all_application_store_status(
            db=db,
            store_id=store_id
        )

        return {"message": f"Updated {result} record(s)"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update status: {str(e)}")


@router.put("/send-to-tech-support", status_code=status.HTTP_200_OK)
def send_to_tech_support(request: TechSupportUpdateRequest, db: Session = Depends(get_db)):
    try:
        result = update_application_store_tech_support(
            db=db,
            store_id=request.store_id,
            script_name=request.script_name,
            cam_no=request.cam_no
        )
        store = fetch_store_details(db, store_id=request.store_id)
        store_name = ""
        if store:
            store_name = store[0].name

        email_data = {
            "Sai Store Id": request.store_id,
            "Store Name": store_name,
            "Script Name": request.script_name,
            "Camera Number": request.cam_no if request.cam_no else ""
        }
        email_body = get_table_email_body(email_data)
        send_application_monitoring_mail(email_body)
        return {"message": f"Updated {result} record(s)"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update status: {str(e)}")


@router.put("/update-tech-support-status", status_code=status.HTTP_200_OK)
def update_tech_support_status(request: TechSupportUpdateRequest, db: Session = Depends(get_db)):
    try:
        result = update_application_store_tech_support_status(
            db=db,
            store_id=request.store_id,
            script_name=request.script_name,
            cam_no=request.cam_no
        )
        return {"message": f"Updated {result} record(s)"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update status: {str(e)}")
