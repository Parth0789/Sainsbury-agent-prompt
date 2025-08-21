from sqlalchemy import func, distinct,or_, and_ ,true,false,desc,asc,extract, join, select
from model.model import Transactions, Stores
from sqlalchemy.orm import Session
from utils import (current_date_time,
                   get_last_3_months_from_current_date,
                   sort_by_year_month,
                   sort_by_year_month_week,
                   sort_by_year_month_day,
                   merge_list_of_aisle_theft
                   )
import json
import requests

BASE_URL = "https://api.saivalentine.com/"

def get_count_aisle(db: Session,store_id,region_id,area_id,start_time,end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()
    print(start_time, end_time)
    aisle=db.query(
        func.count(distinct(Transactions.transaction_id)).label(
            'aisle_count'),
        func.ifnull(func.sum(Transactions.bag_price),0).label(
            'aisle_total')).join(Stores,Transactions.store_id == Stores.id)\
        .filter(or_(Transactions.store_id == store_id, true() if store_id is None else false()),
             or_(Stores.region_id == region_id, true() if region_id is None else false()),
             or_(Stores.area_id == area_id, true() if area_id is None else false())
             ).filter(Transactions.source_id == 3,Transactions.hidden == 0,Transactions.triggers == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .first()

    aisle_intervention=db.query(
        func.count(distinct(Transactions.transaction_id)).label(
            'intervention_count'),
        func.ifnull(func.sum(Transactions.bag_price),0).label(
            'intervention_total')).join(Stores,Transactions.store_id == Stores.id)\
        .filter(or_(Transactions.store_id == store_id, true() if store_id is None else false()),
             or_(Stores.region_id == region_id, true() if region_id is None else false()),
             or_(Stores.area_id == area_id, true() if area_id is None else false())
             )\
        .filter(Transactions.source_id == 3,Transactions.hidden == 0,Transactions.triggers == 1,Transactions.intervention == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .first()

    return {"aisle_count":aisle.aisle_count,
            "aisle_total":aisle.aisle_total,
            "intervention_count":aisle_intervention.intervention_count,
            "intervention_total":aisle_intervention.intervention_total}

def get_top5_aisle_store_region(db: Session,region_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    in_region_theft=db.query(Stores.name,
        func.count(distinct(Transactions.transaction_id)).label(
            'aisle_count'),
        func.sum(Transactions.bag_price).label(
            'aisle_total')).join(Stores,or_(Stores.region_id == region_id, true() if region_id is None else false())).filter(Transactions.store_id == Stores.id)\
        .filter(Transactions.source_id == 3,Transactions.hidden == 0,Transactions.triggers == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.store_id).order_by(
        desc('aisle_count')).limit(5).all()

    in_region_non_theft = db.query(Stores.name,
                               func.count(distinct(Transactions.transaction_id)).label(
                                   'aisle_count'),
                               func.sum(Transactions.bag_price).label(
                                   'aisle_total')).join(Stores, or_(Stores.region_id == region_id, true() if region_id is None else false())).filter(
        Transactions.store_id == Stores.id) \
        .filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(
        Transactions.store_id).order_by(
        asc('aisle_count')).limit(5).all()
    return {"top5_aisle_store_in_region_theft": in_region_theft, "top5_aisle_store_in_region_non_theft": in_region_non_theft}
def get_top5_aisle_store_area(db: Session,area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    in_area_theft=db.query(Stores.name,
        func.count(distinct(Transactions.transaction_id)).label(
            'aisle_count'),
        func.sum(Transactions.bag_price).label(
            'aisle_total')).join(Stores,or_(Stores.area_id == area_id, true() if area_id is None else false())).filter(Transactions.store_id == Stores.id)\
        .filter(Transactions.source_id == 3,Transactions.hidden == 0,Transactions.triggers == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.store_id).order_by(
        desc('aisle_count')).limit(5).all()

    in_area_non_theft = db.query(Stores.name,
                               func.count(distinct(Transactions.transaction_id)).label(
                                   'aisle_count'),
                               func.sum(Transactions.bag_price).label(
                                   'aisle_total')).join(Stores, or_(Stores.area_id == area_id, true() if area_id is None else false())).filter(
        Transactions.store_id == Stores.id) \
        .filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(
        Transactions.store_id).order_by(
        asc('aisle_count')).limit(5).all()

    return {"top5_aisle_store_in_area_theft": in_area_theft, "top5_aisle_store_in_area_non_theft": in_area_non_theft}


def get_count_aisle_stats(db, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = get_last_3_months_from_current_date()
        start_time_current, end_time_current = current_date_time()
    else:
        start_time_current, end_time_current = start_time, end_time

    keys_for_ailse_month = ["Year", "Month", "aisle_count", "aisle_total"]
    keys_for_ailse_week = ["Year", "Month", "Week", "aisle_count", "aisle_total"]
    keys_for_ailse_day = ["Year", "Month", "Day", "aisle_count", "aisle_total"]

    keys_for_intervention_month = ["Year", "Month", "intervention_count", "intervention_total"]
    keys_for_intervention_week = ["Year", "Month", "Week", "intervention_count", "intervention_total"]
    keys_for_intervention_day = ["Year", "Month", "Day", "intervention_count", "intervention_total"]

    aisle_month = db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        func.count(distinct(Transactions.transaction_id)).label(
            'aisle_count'),
        func.ifnull(func.sum(Transactions.bag_price), 0).label(
            'aisle_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
        ).filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time)\
        .group_by(func.month(Transactions.begin_date)).order_by(
        desc(extract('month', Transactions.begin_date))).all()

    aisle_intervention_month = db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        func.count(distinct(Transactions.transaction_id)).label(
            'intervention_count'),
        func.ifnull(func.sum(Transactions.bag_price), 0).label(
            'intervention_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
        ).filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1,
                 Transactions.intervention == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(func.month(Transactions.begin_date)).order_by(
        desc(extract('month', Transactions.begin_date))).limit(3).all()

    aisle_week = db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        extract('week', Transactions.begin_date).label("Week"),
        func.count(distinct(Transactions.transaction_id)).label(
            'aisle_count'),
        func.ifnull(func.sum(Transactions.bag_price), 0).label(
            'aisle_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time_current) \
        .filter(func.date(Transactions.begin_date) <= end_time_current) \
        .group_by(func.week(Transactions.begin_date))\
        .order_by(desc(extract('week', Transactions.begin_date))).limit(4).all()

    aisle_intervention_week = db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        extract('week', Transactions.begin_date).label("Week"),
        func.count(distinct(Transactions.transaction_id)).label(
            'intervention_count'),
        func.ifnull(func.sum(Transactions.bag_price), 0).label(
            'intervention_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1,
             Transactions.intervention == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time_current) \
        .filter(func.date(Transactions.begin_date) <= end_time_current) \
        .group_by(
        func.week(Transactions.begin_date)).order_by(desc(extract('week', Transactions.begin_date))).limit(4).all()

    aisle_days = db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        extract('day', Transactions.begin_date).label("Day"),
        func.count(distinct(Transactions.transaction_id)).label(
            'aisle_count'),
        func.ifnull(func.sum(Transactions.bag_price), 0).label(
            'aisle_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time_current) \
        .filter(func.date(Transactions.begin_date) <= end_time_current) \
        .group_by(
        func.day(Transactions.begin_date)).order_by(desc(extract('month', Transactions.begin_date)),
                                                    desc(extract('day', Transactions.begin_date))).limit(7).all()

    aisle_intervention_days = db.query(
        extract('year', Transactions.begin_date).label("Year"),
        extract('month', Transactions.begin_date).label("Month"),
        extract('day', Transactions.begin_date).label("Day"),
        func.count(distinct(Transactions.transaction_id)).label(
            'intervention_count'),
        func.ifnull(func.sum(Transactions.bag_price), 0).label(
            'intervention_total')).join(Stores, Transactions.store_id == Stores.id).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1,
             Transactions.intervention == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time_current) \
        .filter(func.date(Transactions.begin_date) <= end_time_current) \
        .group_by(func.day(Transactions.begin_date))\
        .order_by(desc(extract('month', Transactions.begin_date)),desc(extract('day', Transactions.begin_date)))\
        .limit(7).all()

    aisle_month = sorted([dict(zip(keys_for_ailse_month, tup)) for tup in aisle_month], key=sort_by_year_month)
    aisle_intervention_month = sorted([dict(zip(keys_for_intervention_month, tup)) for tup in aisle_intervention_month], key=sort_by_year_month)

    aisle_week = sorted([dict(zip(keys_for_ailse_week, tup)) for tup in aisle_week], key=sort_by_year_month_week)
    aisle_intervention_week = sorted([dict(zip(keys_for_intervention_week, tup)) for tup in aisle_intervention_week], key=sort_by_year_month_week)

    aisle_days = sorted([dict(zip(keys_for_ailse_day, tup)) for tup in aisle_days], key=sort_by_year_month_day)
    aisle_intervention_days = sorted([dict(zip(keys_for_intervention_day, tup)) for tup in aisle_intervention_days], key=sort_by_year_month_day)

    return {
        "aisle_month": aisle_month, "aisle_intervention_month":aisle_intervention_month,
        "aisle_week": aisle_week, "aisle_intervention_week":aisle_intervention_week,
        "aisle_days": aisle_days, "aisle_intervention_daye": aisle_intervention_days
            }

def get_top3_theft_current_month_data(db: Session):
    query = (db.query(Stores.name, Transactions.store_id, func.count(Transactions.transaction_id),
                              func.sum(Transactions.bag_price))
             .select_from(Transactions).join(Stores, Transactions.store_id == Stores.id)
             .filter(Transactions.triggers == 1)
             .filter(func.month(Transactions.begin_date) == func.month(func.now()))
             .filter(func.year(Transactions.begin_date) == func.year(func.now()))
             .filter(Transactions.hidden == 0)
             .group_by(Transactions.store_id)
             .order_by(func.sum(Transactions.bag_price).desc())
             .limit(3))

    result = query.all()
    res = []
    for i in result:
        res.append(dict(
            store_name = i[0],
            store_id = i[1],
            count = i[2],
            total = i[3]
        ))
    return res

def get_top3_intervention_current_month_data(db: Session):
    query = (db.query(Stores.name, Transactions.store_id, func.count(Transactions.transaction_id),
                              func.sum(Transactions.bag_price)) # NOQA:E127,E261
             .select_from(Transactions).join(Stores, Transactions.store_id == Stores.id)
             .filter(Transactions.triggers == 1)
             .filter(Transactions.intervention == 1)
             .filter(func.month(Transactions.begin_date) == func.month(func.now()))
             .filter(func.year(Transactions.begin_date) == func.year(func.now()))
             .filter(Transactions.hidden == 0)
             .group_by(Transactions.store_id)
             .order_by(func.sum(Transactions.bag_price).desc())
             .limit(3)) # NOQA

    result = query.all()
    print(result)
    res = []
    for i in result:
        res.append(dict(
            store_name=i[0],
            store_id=i[1],
            count=i[2],
            total=i[3]
                )
        )
    return res

def get_aisle_count_data(db: Session, store_id, region_id, area_id, is_intervention, start_time, end_time, page, seen_status):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()
    per_page = 10
    count = {"total_count": 0}
    if is_intervention == 1:
        aisle_intervention_data = db.query(Stores.name.label("store"), Transactions.transaction_id,
                                           Transactions.sequence_no, Transactions.counter_no, Transactions.operator_id,
                                           Transactions.operator_id,
                                           Transactions.description, Transactions.missed_scan, Transactions.video_link,
                                           Transactions.begin_date) \
            .filter(
            or_(Transactions.store_id == store_id, true() if store_id is None else false()),
            or_(Stores.region_id == region_id, true() if region_id is None else false()),
            or_(Stores.area_id == area_id, true() if area_id is None else false())
        ) \
            .join(Stores, Transactions.store_id == Stores.id) \
            .filter(Transactions.source_id == 3, Transactions.hidden == 0,
                    Transactions.triggers == 1, Transactions.intervention == is_intervention, Transactions.seen_status == seen_status) \
            .filter(func.date(Transactions.begin_date) >= start_time) \
            .filter(func.date(Transactions.begin_date) <= end_time) \
            .order_by(desc(func.date(Transactions.begin_date)))
        count["total_count"]= aisle_intervention_data.count()
        return aisle_intervention_data.limit(10).offset((page - 1) * per_page).all(), count

    aisle_intervention_data = db.query(Stores.name.label("store"), Transactions.transaction_id,Transactions.sequence_no,Transactions.counter_no,Transactions.operator_id, Transactions.operator_id,
             Transactions.description,Transactions.missed_scan,Transactions.video_link, Transactions.begin_date)\
        .filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
        )\
        .join(Stores, Transactions.store_id == Stores.id) \
        .filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1, Transactions.seen_status == seen_status)\
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .order_by(desc(func.date(Transactions.begin_date)))
    count["total_count"]= aisle_intervention_data.count()
    return aisle_intervention_data.limit(10).offset((page - 1) * per_page).all(), count

def get_ailse_transaction_data_by_id(details, db):
    none_val = [None, ""]
    transaction_id = details["transaction_id"]
    if transaction_id in none_val:
        return {"message": "ID is not valid"}
    result = db.query(Transactions.aisle_theft_type, Transactions.aisle_theft_id).filter(Transactions.transaction_id == transaction_id).all()
    result = result[0]._asdict()
    if result["aisle_theft_type"] not in none_val and result["aisle_theft_id"] not in none_val:
        url_ext = "get_detail_notoficaion_detail"
        payload = json.dumps({
            "id": result["aisle_theft_id"],
            "type": result["aisle_theft_type"],
            "user_company": "Sainsbury"
        })
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", BASE_URL + url_ext, headers=headers, data=payload, verify=False)
        return response.json()

    return {"message": "No Data Found"}

def report_top10_aisle(db, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    result = db.query(Stores.name,
        func.count(distinct(Transactions.transaction_id)).label(
            'aisle_count'),
        func.sum(Transactions.bag_price).label(
            'aisle_total')).join(Stores, Transactions.store_id == Stores.id) \
        .filter(or_(Stores.region_id == region_id, true() if region_id is None else false()),
                or_(Stores.area_id == area_id, true() if area_id is None else false())) \
        .filter(Transactions.source_id == 3,Transactions.hidden == 0,Transactions.triggers == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.store_id).order_by(
        desc('aisle_count')).limit(10).all()

    return result


def report_top10_intervention(db, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    result = db.query(Stores.name,
                      func.count(distinct(Transactions.transaction_id)).label(
                          'intervention_count'),
                      func.sum(Transactions.bag_price).label(
                          'intervention_total')).join(Stores, Transactions.store_id == Stores.id) \
        .filter(or_(Stores.region_id == region_id, true() if region_id is None else false()),
                or_(Stores.area_id == area_id, true() if area_id is None else false())) \
        .filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1, Transactions.intervention == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.store_id).order_by(
        desc('intervention_count')).limit(10).all()

    return result

def list_of_aisle_theft(db, store_id, region_id, area_id, start_time, end_time):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()

    result_aisle = db.query(Stores.id, Stores.name,
                      func.count(distinct(Transactions.transaction_id)).label(
                          'aisle_no_of_theft'),
                      func.sum(Transactions.bag_price).label(
                          'aisle_total')).join(Stores, Transactions.store_id == Stores.id) \
        .filter(or_(Transactions.store_id == store_id, true() if store_id is None else false()),
                or_(Stores.region_id == region_id, true() if region_id is None else false()),
                or_(Stores.area_id == area_id, true() if area_id is None else false())) \
        .filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.store_id).order_by(
        desc('aisle_no_of_theft')).group_by(Transactions.store_id).all()

    result_intervention = db.query(Stores.id, Stores.name,
                      func.count(distinct(Transactions.transaction_id)).label(
                          'intervention_no_of_theft'),
                      func.sum(Transactions.bag_price).label(
                          'intervention_total')).join(Stores, Transactions.store_id == Stores.id) \
        .filter(or_(Transactions.store_id == store_id, true() if store_id is None else false()),
                or_(Stores.region_id == region_id, true() if region_id is None else false()),
                or_(Stores.area_id == area_id, true() if area_id is None else false())) \
        .filter(Transactions.source_id == 3, Transactions.hidden == 0, Transactions.triggers == 1, Transactions.intervention == 1) \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.store_id).order_by(
        desc('intervention_no_of_theft')).group_by(Transactions.store_id).all()

    aisle_list = [dict(zip(["id", "name", "aisle_no_of_theft", "aisle_total"], tup)) for tup in result_aisle]
    intervention_list = [dict(zip(["id", "name", "intervention_no_of_theft", "intervention_total"], tup)) for tup in result_intervention]


    merged = merge_list_of_aisle_theft(aisle_list, intervention_list)

    store_id = [data["id"] for data in merged]
    url = "get_theft_active_count"
    payload = json.dumps({
        "store_id": store_id,
        "start_date": str(start_time),
        "end_date": str(end_time),
        "company_name": "Sainsbury"
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.post(BASE_URL+url, headers=headers, data=payload, verify=False)
    res = response.json()
    res = res["data"]
    if response.status_code == 200:
        result = []
        for merged_item in merged:
            for item in res:
                if merged_item["id"] == item["store_id"]:
                    result.append({**merged_item, "no_of_alert": item["count"]})
        for merged_item in merged:
            flag = False  # Assume not in the other list
            for result_item in result:
                if merged_item["name"] == result_item["name"]:
                    flag = True  # Found in the list
                    break
            if flag == False:
                result.append({**merged_item, "no_of_alert": item["count"]})
    else:
        result = [{**item, "no_of_alert": 0} for item in merged]
    return {"data": result}

def active_stores(db):
    active_stores = db.query(func.count()).filter(Stores.store_running == 1).scalar()
    return {"no_of_live_store": active_stores}


def get_camera_number(value):
    values = value.split(".")
    if len(values) == 2:
        return int(values[0])

    return int(values[-2])