import pytz
from datetime import date, timedelta
from collections import defaultdict

from sqlalchemy import desc, distinct, or_, and_, true, false, func, case
from sqlalchemy.sql import expression
from sqlalchemy.orm import aliased

from model.model import (
    Stores, Status, AisleImages, StoreCameraMapping, CameraInfo, LatestStatus, ApplicationStoreStatus, StreamData,
    VTCData, JitterData
)
from utils import get_last_hour_timestamp, get_current_year_timestamp, convert_seconds_to_hhmmss


def get_all_camera_status(db, store_id):
    start_time, end_time = get_last_hour_timestamp()
    result_last_one_hour = db.query(
        distinct(Status.camera_no).label("camera_no"), Status.current_status
    ).filter(
        Status.store_id == store_id,
        Status.current_status == 1,
        Status.camera_no.is_not(None),
        Status.created_at > start_time,
        Status.created_at <= end_time
    ).order_by(
        desc(Status.created_at)
    ).all()

    return result_last_one_hour


def get_store_camera_mapping(store_id, db):
    result = db.query(
        StoreCameraMapping.camera_no
    ).filter(
        StoreCameraMapping.store_id == store_id
    ).all()

    return result


def fetch_all_latest_status(db, store_id, region_id, area_id, client_region_id):
    result = db.query(
        LatestStatus.store_id.label("store_id_dashboard"),
        LatestStatus.camera_no.label("camera_no"),
        LatestStatus.updated_at.label("checked_on"),
        case(
            ((LatestStatus.last_active.is_not(None), LatestStatus.last_active)),
              else_="Never"
        ).label("last_seen")
    ).join(
        Stores, Stores.id == LatestStatus.store_id
    ).filter(
        or_(LatestStatus.store_id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.area_id == area_id, true() if not area_id else false()),
        or_(Stores.company_region_id == client_region_id, true() if not client_region_id else false())
    ).all()

    return result


def fetch_vtc_data(db, store_id, region_id, area_id, client_region_id):
    result = db.query(
        VTCData.store_id.label("store_id_dashboard"),
        VTCData.camera_no.label("pos_id"),
    ).join(
        Stores, Stores.id == VTCData.store_id
    ).filter(
        or_(VTCData.store_id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.area_id == area_id, true() if not area_id else false()),
        or_(Stores.company_region_id == client_region_id, true() if not client_region_id else false()),
        VTCData.breakage_duration == 0
    ).group_by(VTCData.store_id, VTCData.camera_no).all()

    return result


def fetch_jitter_data(db, store_id, region_id, area_id, client_region_id):
    result = db.query(
        JitterData.store_id.label("store_id_dashboard"),
        JitterData.camera_no.label("pos_id"),
        func.max(JitterData.created_at).label("created_at")
    ).join(
        Stores, Stores.id == JitterData.store_id
    ).filter(
        or_(JitterData.store_id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.area_id == area_id, true() if not area_id else false()),
        or_(Stores.company_region_id == client_region_id, true() if not client_region_id else false())
    ).group_by(JitterData.store_id, JitterData.camera_no).all()
    
    return result

def get_all_status_message(db, store_id):
    start_time, end_time = get_last_hour_timestamp()
    result = db.query(
        distinct(Status.camera_no).label("camera_no"), Status.store_id, Status.system_name, Status.current_status,
        Status.updated_at
    ).filter(
        or_(Status.store_id == store_id, true() if store_id is None else false()),
        Status.created_at > start_time, Status.created_at <= end_time
    ).order_by(
        desc(Status.created_at)
    )

    result_camera = result.filter(Status.camera_no.is_not(None)).all()
    result_system = result.filter(Status.camera_no.is_(None)).all()

    return result_camera, result_system


def get_all_status(db, type, store_id, camera_no, from_date, to_date, total_seconds, page):
    if not from_date and not to_date:
        from_date, to_date = get_current_year_timestamp()
    else:
        to_date += timedelta(days=1)

    if type == "camera":
        aisle_images = db.query(
            AisleImages.camera_no
        ).filter(
            AisleImages.store_id == store_id
        ).all()
        aisle_images_set = set([int(item.camera_no) for item in aisle_images])
        print(aisle_images_set)

        result = db.query(
            Status.camera_no, Status.current_status, Status.updated_at
        ).filter(
            Status.store_id == store_id,
            Status.camera_no == camera_no,
            Status.camera_no.in_(aisle_images_set),
            Status.created_at >= from_date,
            Status.created_at <= to_date
        ).order_by(
            Status.created_at
        ).all()

        data = []
        start_time = None
        for record in result:
            if not start_time and not record["current_status"]:
                start_time = record["updated_at"]

            if start_time and record["current_status"]:
                if (record["updated_at"] - start_time).total_seconds() >= total_seconds:
                    start_time = pytz.utc.localize(start_time).astimezone(pytz.timezone("Europe/London"))
                    end_time = pytz.utc.localize(record["updated_at"]).astimezone(pytz.timezone("Europe/London"))
                    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
                    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
                    data.append(
                        {
                            "camera_no": camera_no,
                            "start_time": start_time,
                            "end_time": end_time,
                            "working": False
                        }
                    )

                start_time = None

        if start_time and not result[-1]["current_status"]:
            if (result[-1]["updated_at"] - start_time).total_seconds() >= total_seconds:
                start_time = pytz.utc.localize(start_time).astimezone(pytz.timezone("Europe/London"))
                end_time = pytz.utc.localize(result[-1]["updated_at"]).astimezone(pytz.timezone("Europe/London"))
                start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
                end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
                data.append(
                    {
                        "camera_no": camera_no,
                        "start_time": start_time,
                        "end_time": end_time,
                        "working": False
                    }
                )

        data = data[::-1]
        print(data)
        return {"data": data[(page - 1) * 10: ((page - 1) * 10) + 10], "total_count": len(data)}
    elif type == "system":
        result = db.query(
            Status.system_name, Status.current_status, Status.updated_at
        ).filter(
            Status.store_id == store_id,
            Status.camera_no.is_(None),
            Status.created_at >= from_date,
            Status.created_at <= to_date
        ).order_by(
            Status.created_at
        ).all()

        data = []
        start_time = None
        for record in result:
            if not start_time and not record["current_status"]:
                start_time = record["updated_at"]

            if start_time and record["current_status"]:
                if (record["updated_at"] - start_time).total_seconds() >= total_seconds:
                    start_time = pytz.utc.localize(start_time).astimezone(pytz.timezone("Europe/London"))
                    end_time = pytz.utc.localize(record["updated_at"]).astimezone(pytz.timezone("Europe/London"))
                    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
                    end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
                    data.append(
                        {
                            "system_name": record["system_name"],
                            "start_time": start_time,
                            "end_time": end_time,
                            "working": False
                        }
                    )

                start_time = None

        if start_time and not result[-1]["current_status"]:
            if (result[-1]["updated_at"] - start_time).total_seconds() >= total_seconds:
                start_time = pytz.utc.localize(start_time).astimezone(pytz.timezone("Europe/London"))
                end_time = pytz.utc.localize(result[-1]["updated_at"]).astimezone(pytz.timezone("Europe/London"))
                start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
                end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
                data.append(
                    {
                        "system_name": result[-1]["system_name"],
                        "start_time": start_time,
                        "end_time": end_time,
                        "working": False
                    }
                )

        data = data[::-1]
        return {"data": data[(page - 1) * 10: ((page - 1) * 10) + 10], "total_count": len(data)}


def get_status_summary_data(store_id, db):
    aisle_images = db.query(
        AisleImages.camera_no
    ).filter(
        AisleImages.store_id == store_id
    ).all()

    # available_cam = set()
    # for item in aisle_images:
    #     img = item["images"].split(".")
    #     if len(img) > 2:
    #         camera_no = int(img[-2])
    #     else:
    #         camera_no = int(img[0])
    #     available_cam.add(camera_no)
    available_cam = set([int(item.camera_no) for item in aisle_images])

    camera_result = db.query(
        Status.camera_no, Status.current_status, Status.updated_at, CameraInfo.counter_no.label("pos_id")
    ).join(
        CameraInfo, and_(CameraInfo.store_id == store_id, CameraInfo.camera_no == Status.camera_no)
    ).filter(
        Status.store_id == store_id,
        Status.camera_no.in_(available_cam)
    ).order_by(Status.updated_at).all()

    system_result = db.query(
        Status.current_status, Status.updated_at, Status.system_name
    ).filter(
        Status.store_id == store_id,
        Status.camera_no.is_(None)
    ).order_by(Status.updated_at).all()

    data = {"camera_data": [], "system_data": []}
    raw_data = {}
    for record in camera_result:
        camera_no = record["camera_no"]

        if str(camera_no) not in raw_data:
            raw_data[str(camera_no)] = {
                "working_duration": 0,
                "non_working_duration": 0,
                "prev_timestamp": record["updated_at"]
            }
            continue

        seconds = int((record["updated_at"] - raw_data[str(camera_no)]["prev_timestamp"]).total_seconds())
        raw_data[str(camera_no)]["pos_id"] = record["pos_id"]
        raw_data[str(camera_no)]["prev_timestamp"] = record["updated_at"]
        if record["current_status"]:
            raw_data[str(camera_no)]["working_duration"] += seconds
        else:
            raw_data[str(camera_no)]["non_working_duration"] += seconds

    for key, value in raw_data.items():
        data["camera_data"].append(
            {
                "camera_no": key,
                "pos_id": value["pos_id"],
                "working_duration": convert_seconds_to_hhmmss(value["working_duration"]),
                "non_working_duration": convert_seconds_to_hhmmss(value["non_working_duration"])
            }
        )

    raw_data = {
        "system_name": system_result[0]["system_name"],
        "working_duration": 0,
        "non_working_duration": 0,
        "prev_timestamp": system_result[0]["updated_at"]
    }
    for record in system_result[1:]:
        seconds = int((record["updated_at"] - raw_data["prev_timestamp"]).total_seconds())
        raw_data["prev_timestamp"] = record["updated_at"]
        if record["current_status"]:
            raw_data["working_duration"] += seconds
        else:
            raw_data["non_working_duration"] += seconds

    raw_data["working_duration"] = convert_seconds_to_hhmmss(raw_data["working_duration"])
    raw_data["non_working_duration"] = convert_seconds_to_hhmmss(raw_data["non_working_duration"])
    raw_data.pop("prev_timestamp")
    data["system_data"].append(raw_data)

    return data


def get_store_map_issue_data(store_ids, region_id, area_id, db):
    result = db.query(
        Stores.id, Stores.name, Stores.region_id, Stores.area_id, Stores.latitude, Stores.longitude
    ).join(
        Status, Status.store_id == Stores.id
    ).filter(
        or_(Stores.id.in_(store_ids), true() if not store_ids else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false())
    ).group_by(
        Status.store_id
    ).all()

    return result


def fetch_camera_status_images(store_id, db):
    if store_id is not None:
        aisle_images = db.query(
            AisleImages.id, AisleImages.images, AisleImages.label, CameraInfo.camera_ip
        ).join(
            CameraInfo, and_(AisleImages.camera_no == CameraInfo.camera_no, AisleImages.store_id == CameraInfo.store_id)
        ).filter(
            AisleImages.store_id == store_id
        ).all()
        result_one_hour = get_all_camera_status(db, store_id)
        store_camera_map = get_store_camera_mapping(store_id, db)
        result_one_hour = set(map(lambda x: x["camera_no"], result_one_hour))
        available_cam = set(map(lambda x: x["camera_no"], store_camera_map))
    else:
        aisle_images = []
        result_one_hour = set([])
        available_cam = set([])

    formatted_aisle_images = []
    print(len(aisle_images))
    for item in aisle_images:
        img = item["images"].split(".")
        if len(img) > 2:
            camera_no = int(img[-2])
        else:
            camera_no = int(img[0])

        if int(camera_no) not in available_cam:
            continue

        formatted_aisle_images.append(
            {
                "id": item['id'],
                "images": "https://sainsbury-zip.s3.eu-west-2.amazonaws.com/aisle_images/{}/{}".format(
                    store_id, item["images"]),
                "label": item['label'],
                "camera_no": camera_no,
                "camera_ip": item["camera_ip"],
                "one_hour_status": 1 if int(camera_no) in result_one_hour else 0
            }
        )

    return formatted_aisle_images


def fetch_camera_last_update_time(db):
    result = db.query(func.max(Status.created_at).label("last_update_at")).all()

    if not result:
        return {"message": "No Data Found!"}, 400

    return result[0].last_update_at.strftime("%Y-%m-%d %H:%M:%S")


def fetch_store_status_for_report(db, store_id = None, region_id = None, client_region_id = None):
    result_camera_record = db.query(
        LatestStatus.store_id.label("store_id_dashboard"),
        Stores.store_num.label("Store ID"),
        Stores.name.label("Store Name"),
        expression.literal("not pinging").label("Status"),
        LatestStatus.updated_at.label("Checked On"),
        # LatestStatus.camera_no.label("camera_no"),
        case(
            ((LatestStatus.last_active.is_not(None), LatestStatus.last_active)),
            else_="Never"
        ).label("Last Seen"),
        CameraInfo.camera_ip.label("Camera IP"),
        CameraInfo.counter_no.label("POS ID")
    ).join(
        Stores, Stores.id == LatestStatus.store_id
    ).join(
        CameraInfo, and_(CameraInfo.store_id == LatestStatus.store_id,
                         CameraInfo.camera_no == LatestStatus.camera_no)
    ).filter(
        Stores.store_running == 1,
        LatestStatus.current_status == 0,
        LatestStatus.camera_no.is_not(None),
        or_(Stores.id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.company_region_id == client_region_id, true() if not client_region_id else false()),
    ).order_by(Stores.name).all()

    result_store_record = db.query(
        LatestStatus.store_id.label("store_id_dashboard"),
        Stores.store_num.label("Store ID"),
        Stores.name.label("Store Name"),
        expression.literal("not pinging").label("Status"),
        LatestStatus.updated_at.label("Checked On"),
        case(
            ((LatestStatus.last_active.is_not(None), LatestStatus.last_active)),
            else_="Never"
        ).label("Last Seen")
    ).join(
        Stores, Stores.id == LatestStatus.store_id
    ).filter(
        Stores.store_running == 1,
        LatestStatus.current_status == 0,
        LatestStatus.camera_no.is_(None),
        or_(Stores.id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.company_region_id == client_region_id, true() if not client_region_id else false()),
    ).order_by(Stores.name).all()

    offline_stores = set([record.store_id_dashboard for record in result_store_record])

    result_camera_record = [record for record in result_camera_record if record.store_id_dashboard not in offline_stores]
    result_camera = []
    for record in result_camera_record:
        record = dict(record)
        del record["store_id_dashboard"]
        result_camera.append(record)

    result_store = []
    for record in result_store_record:
        record = dict(record)
        del record["store_id_dashboard"]
        result_store.append(record)

    return result_camera, result_store


def fetch_camera_status_for_report(
    db, store_id: int = None, region_id: int = None, area_id: int = None, client_region_id = None
):
    result_camera_record = db.query(
        LatestStatus.store_id.label("store_id_dashboard"),
        Stores.store_num.label("store_id"),
        Stores.name.label("store_name"),
        expression.literal("not pinging").label("status"),
        expression.literal("").label("frame_width"),
        expression.literal("").label("frame_height"),
        expression.literal("").label("fps"),
        expression.literal("").label("bitrate"),
        LatestStatus.updated_at.label("checked_on"),
        LatestStatus.camera_no.label("camera_no"),
        case(
            ((LatestStatus.last_active.is_not(None), LatestStatus.last_active)),
              else_="Never"
        ).label("last_seen"),
        CameraInfo.camera_ip.label("camera_ip"),
        CameraInfo.counter_no.label("pos_id"),
    ).join(
        Stores, Stores.id == LatestStatus.store_id
    ).join(
        CameraInfo, and_(CameraInfo.store_id == LatestStatus.store_id, CameraInfo.camera_no == LatestStatus.camera_no)
    ).filter(
        LatestStatus.current_status == 0,
        Stores.store_running == 1,
        LatestStatus.camera_no.is_not(None),
        or_(LatestStatus.store_id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.company_region_id == client_region_id, true() if not client_region_id else false()),
        or_(Stores.area_id == area_id, true() if not area_id else false()),
    ).order_by(Stores.name).all()

    result_store_record = db.query(
        LatestStatus.store_id,
        Stores.name.label("store_name"),
        expression.literal("not pinging").label("status"),
        LatestStatus.updated_at.label("checked_on"),
    ).join(
        Stores, Stores.id == LatestStatus.store_id
    ).filter(
        LatestStatus.current_status == 0,
        LatestStatus.camera_no.is_(None),
        Stores.store_running == 1,
        or_(LatestStatus.store_id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.company_region_id == client_region_id, true() if not client_region_id else false()),
        or_(Stores.area_id == area_id, true() if not area_id else false())
    ).order_by(Stores.name).all()

    offline_stores = set([record.store_id for record in result_store_record])

    result_camera_record = [record for record in result_camera_record if record.store_id_dashboard not in offline_stores]

    # result_camera_record = result_camera_record[(page - 1) * per_page: page * per_page]
    res = []
    for record in result_camera_record:
        record = dict(record)
        record["checked_on"] = record["checked_on"].strftime("%Y-%m-%d %H:%M:%S")
        res.append(record)

    return res


def fetch_system_status_for_report(db, store_id: int = None, region_id: int = None, area_id: int = None, page: int = 1,
                                   per_page: int = 10, client_region_id: int = None):
    start_time, end_time = get_last_hour_timestamp()

    result_store_record = db.query(
        Stores.store_num.label("store_id"),
        Stores.name.label("store_name"),
        expression.literal("not pinging").label("status"),
        LatestStatus.updated_at.label("checked_on"),
        case(
            ((LatestStatus.last_active.is_not(None), LatestStatus.last_active)),
            else_="Never"
        ).label("last_seen")
    ).join(
        Stores, Stores.id == LatestStatus.store_id
    ).filter(
        LatestStatus.current_status == 0,
        LatestStatus.camera_no.is_(None),
        Stores.store_running == 1,
        or_(LatestStatus.store_id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.company_region_id == client_region_id, true() if not client_region_id else false()),
        or_(Stores.area_id == area_id, true() if not area_id else false()),
        # Status.created_at > start_time,
        # Status.created_at <= end_time
    ).order_by(Stores.name).all()

    # result_store = sorted(result_store, key=lambda record: record["store_name"])
    count = len(result_store_record)
    result_store_record = result_store_record[(page - 1) * per_page: page * per_page]
    res = []
    for record in result_store_record:
        record = dict(record)
        record["checked_on"] = record["checked_on"].strftime("%Y-%m-%d %H:%M:%S")
        res.append(record)

    return res, count


def fetch_stream_data_for_report(db, store_id: int = None, region_id: int = None, area_id: int = None, 
                                 client_region_id: int = None):
    from datetime import datetime
    result = db.query(
        Stores.id.label("store_id_dashboard"),
        Stores.store_num.label("store_id"),
        Stores.name.label("store_name"),
        expression.literal("pinging").label("status"),
        CameraInfo.camera_ip.label("camera_ip"),
        CameraInfo.camera_no.label("camera_no"),
        StreamData.camera_no.label("pos_id"),
        StreamData.frame_width.label("frame_width"),
        StreamData.frame_height.label("frame_height"),
        StreamData.fps.label("fps"),
        StreamData.bitrate.label("bitrate"),
        func.max(StreamData.created_at).label("checked_on")
    ).join(
        Stores, StreamData.store_id == Stores.id
    ).join(
        CameraInfo, and_(CameraInfo.store_id == StreamData.store_id, CameraInfo.counter_no == StreamData.camera_no)
    ).filter(
        or_(StreamData.store_id == store_id, true() if not store_id else false()),
        or_(Stores.region_id == region_id, true() if not region_id else false()),
        or_(Stores.area_id == area_id, true() if not area_id else false()),
        or_(Stores.company_region_id == client_region_id, true() if not client_region_id else false()),
        StreamData.created_at >= datetime.now().replace(minute=0, second=0, microsecond=0)
    ).group_by(
        StreamData.store_id, StreamData.camera_no
    ).all()

    return result


def fetch_active_status_stores(db):
    result = db.query(
        Stores.id, Stores.name
    ).where(
        Stores.store_running == 1
    ).group_by(Stores.id).order_by(Stores.name).all()

    return result


def fetch_stores_status_summary(store_id, region_id, db):
    result_camera_status = db.query(
        LatestStatus.store_id,
        func.sum(
            case(
                ((LatestStatus.current_status == 1, 1)),
                else_=0
            )
        ).label("online"),
        func.sum(
            case(
                ((LatestStatus.current_status == 0, 1)),
                else_=0
            )
        ).label("offline"),
        func.count(LatestStatus.store_id).label("total")
    ).join(
        Stores, LatestStatus.store_id == Stores.id
    ).filter(
        LatestStatus.camera_no.is_not(None),
        Stores.store_running == 1,
        or_(LatestStatus.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
    ).group_by(LatestStatus.store_id).all()

    result_store_status = db.query(
        LatestStatus.store_id,
        func.sum(
            case(
                ((LatestStatus.current_status == 1, 1)),
                else_=0
            )
        ).label("online"),
        func.sum(
            case(
                ((LatestStatus.current_status == 0, 1)),
                else_=0
            )
        ).label("offline"),
        func.count(LatestStatus.store_id).label("total")
    ).join(
        Stores, LatestStatus.store_id == Stores.id
    ).filter(
        LatestStatus.camera_no.is_(None),
        Stores.store_running == 1,
        or_(LatestStatus.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
    ).group_by(LatestStatus.store_id).all()

    return result_camera_status, result_store_status


def fetch_live_store_status(store_id, region_id, client_region_id, db):
    result = db.query(
        Stores.id.label("store_id_dashboard"),
        Stores.store_num.label("store_id"),
        Stores.name.label("name"),
        CameraInfo.camera_ip.label("camera_ip"),
        CameraInfo.counter_no.label("pos_id"),
        CameraInfo.setup_date.label("setup_date")
    ).join(
        Stores, Stores.id == CameraInfo.store_id
    ).where(
        Stores.store_running == 1,
        or_(Stores.id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.company_region_id == client_region_id, true() if client_region_id is None else false())
    ).order_by(Stores.name).all()

    return result


def fetch_live_stores_sco_count(store_id, db):
    result = db.query(
        Stores.id, func.count(CameraInfo.counter_no).label("sco_count")
    ).join(
        Stores, Stores.id == CameraInfo.store_id
    ).where(
        Stores.store_running == 1,
        or_(Stores.id == store_id, true() if store_id is None else false())
    ).group_by(Stores.id).all()

    return result


def fetch_data_application_store_status_table(db, store_id=None, cam_no=None, search_date=None, app_status=None,
                                              tech_support = 0, tech_support_status=1):
    # Parse date if provided, otherwise use today's date
    filter_date = date.today()
    if search_date:
        try:
            filter_date = date.fromisoformat(search_date)
        except ValueError:
            # Handle invalid date format
            raise ValueError("Invalid date format. Please use YYYY-MM-DD.")

    if not app_status:
        app_status = "Not Running"

    # Build the base query
    query = db.query(
        func.any_value(ApplicationStoreStatus.id).label('id'),
        ApplicationStoreStatus.store_id,
        Stores.store_actual_id.label("store_actual_id"),
        Stores.name,
        ApplicationStoreStatus.cam_no,
        ApplicationStoreStatus.script_name,
        ApplicationStoreStatus.status,
        func.max(ApplicationStoreStatus.created_at).label('created_at'),
        ApplicationStoreStatus.company.label("client_name")
    ).join(
        Stores, Stores.id == ApplicationStoreStatus.store_id
    ).where(
        func.date(ApplicationStoreStatus.created_at) == filter_date,
        ApplicationStoreStatus.status == app_status
        # ApplicationStoreStatus.tech_support_status == tech_support_status
    )

    # Add filters for store_id and cam_no if provided
    if store_id is not None:
        query = query.where(ApplicationStoreStatus.store_id == store_id)

    if cam_no is not None:
        query = query.where(ApplicationStoreStatus.cam_no == cam_no)

    if tech_support:
        query = query.where(ApplicationStoreStatus.tech_support == tech_support)

    # Add group by clause
    query = query.group_by(
        ApplicationStoreStatus.store_id,
        ApplicationStoreStatus.cam_no,
        ApplicationStoreStatus.script_name
    )
    result = query.order_by(
        desc(func.max(ApplicationStoreStatus.created_at))
    ).all()

    return result


def fetch_data_application_store_status_table_v2(
        db, 
        store_id = None, 
        cam_no = None, 
        search_query = None, 
        search_date = None, 
        app_status = None,
        tech_support = 0, 
        tech_support_status=1
):
    filter_date = date.today()
    if search_date:
        try:
            filter_date = date.fromisoformat(search_date)
        except ValueError:
            raise ValueError("Invalid date format. Please use YYYY-MM-DD.")

    if not app_status:
        app_status = "Not Running"

    # Build the base query
    query = db.query(
        func.any_value(ApplicationStoreStatus.id).label('id'),
        Stores.name.label("name"),
        ApplicationStoreStatus.store_id.label("store_id"),
        Stores.store_actual_id.label("store_actual_id"),
        ApplicationStoreStatus.cam_no.label("cam_no"),
        ApplicationStoreStatus.status.label("status"),
        ApplicationStoreStatus.company.label("client_name"),
        ApplicationStoreStatus.script_name.label("script_name"),
        func.max(ApplicationStoreStatus.created_at).label('created_at')
    ).join(
        Stores, Stores.id == ApplicationStoreStatus.store_id
    ).filter(
        ApplicationStoreStatus.status == app_status,
        func.date(ApplicationStoreStatus.created_at) == filter_date,
    )

    # Add filters for store_id and cam_no if provided
    if store_id is not None:
        query = query.filter(ApplicationStoreStatus.store_id.in_(store_id))

    if cam_no is not None:
        query = query.filter(ApplicationStoreStatus.cam_no.in_(cam_no))

    if tech_support:
        query = query.filter(
            ApplicationStoreStatus.tech_support == tech_support
        )

    if search_query:
        query = query.filter(
            or_(
                ApplicationStoreStatus.script_name.ilike(f'%{search_query}%'),
                ApplicationStoreStatus.script_name.ilike(f'{search_query}%'),
                ApplicationStoreStatus.script_name.ilike(f'%{search_query}'),
            )
        )

    query = query.group_by(
        ApplicationStoreStatus.store_id,
        ApplicationStoreStatus.cam_no,
        ApplicationStoreStatus.script_name
    )
    
    result = query.order_by(
        desc(func.max(ApplicationStoreStatus.created_at))
    ).all()

    return result


def update_application_store_status(db, store_id, script_name, new_status, search_date=None, cam_no=None):
    # Parse date if provided, otherwise use today's date
    filter_date = date.today()
    if search_date:
        try:
            filter_date = date.fromisoformat(search_date)
        except ValueError:
            raise ValueError("Invalid date format. Please use YYYY-MM-DD.")

    # Build query conditions
    conditions = [
        ApplicationStoreStatus.store_id == store_id,
        ApplicationStoreStatus.script_name == script_name
    ]

    if cam_no is not None:
        conditions.append(ApplicationStoreStatus.cam_no == cam_no)

    db.query(
        ApplicationStoreStatus
    ).filter(
        and_(*conditions)
    ).update(
        {"status": new_status}, synchronize_session=False
    )

    db.commit()

    return True


def update_all_application_store_status(db, store_id):
    conditions = [
        ApplicationStoreStatus.store_id == store_id,
        ApplicationStoreStatus.status == "Not Running"
    ]

    db.query(
        ApplicationStoreStatus
    ).filter(
        and_(*conditions)
    ).update(
        {"status": "Running"}, synchronize_session=False
    )

    db.commit()

    return True


def update_application_store_status_v2(db, data):
    db.begin()

    try:
        for record in data:
            record = dict(record)
            search_date = record.get("search_date")
            store_id = record.get("store_id")
            script_name = record.get("script_name")
            new_status = record.get("new_status")
            cam_no = record.get("cam_no", None)

            conditions = [
                ApplicationStoreStatus.store_id == store_id,
                ApplicationStoreStatus.script_name == script_name,
                ApplicationStoreStatus.status == "Not Running"
            ]

            if cam_no:
                conditions.append(ApplicationStoreStatus.cam_no == cam_no)

            db.query(
                ApplicationStoreStatus
            ).filter(
                and_(*conditions)
            ).update(
                {"status": new_status}, synchronize_session=False
            )

        db.commit()
    except Exception as e:
        db.rollback()
        raise e

    return True


def update_application_store_tech_support(db, store_id, script_name, cam_no=None):
    conditions = [
        ApplicationStoreStatus.store_id == store_id,
        ApplicationStoreStatus.script_name == script_name,
        ApplicationStoreStatus.tech_support == 0,
        ApplicationStoreStatus.status == "Not Running"
    ]

    # Add camera filter if provided
    if cam_no is not None:
        conditions.append(ApplicationStoreStatus.cam_no == cam_no)

    db.query(
        ApplicationStoreStatus
    ).filter(
        and_(*conditions)
    ).update(
        {"tech_support": 1}, synchronize_session=False
    )

    db.commit()

    return True


def update_application_store_tech_support_status(db, store_id, script_name, cam_no=None):
    conditions = [
        ApplicationStoreStatus.store_id == store_id,
        ApplicationStoreStatus.script_name == script_name,
        ApplicationStoreStatus.tech_support == 1,
        ApplicationStoreStatus.tech_support_status == 0,
        ApplicationStoreStatus.status == "Not Running"
    ]

    # Add camera filter if provided
    if cam_no is not None:
        conditions.append(ApplicationStoreStatus.cam_no == cam_no)

    db.query(
        ApplicationStoreStatus
    ).filter(
        and_(*conditions)
    ).update(
        {"tech_support_status": 1}, synchronize_session=False
    )

    db.commit()

    return True