from sqlalchemy import distinct, desc, or_, true, false
from model.model import Stores, StoresScoAutomationConfig, ScoConfigChangeLogs, StoresInternalDev
from datetime import datetime

HYPERCARE_STORES = [35, 69, 9, 51, 54, 170, 195, 190, 125, 134, 150, 25, 7, 158, 136, 188, 154, 43, 183, 48, 204, 206,
                    210]


# def get_issue_category(db):
#     categorys = db.query(IssueCategory).all()
#     return categorys


# def store_issue_data(db, store_id, region_id, area_id, category_id, page):
#     per_page = 10
#     results = db.query(Stores.name, Stores.region_id, Stores.area_id,
#                        IssueDescription.problem, IssueDescription.comment,
#                        IssueCategory.category_name)\
#     .join(IssueDescription, Stores.id == IssueDescription.store_id)\
#     .join(IssueCategory, IssueDescription.category == IssueCategory.id) \
#     .filter(
#         or_(IssueDescription.store_id == store_id, true() if store_id is None else false()),
#         or_(Stores.region_id == region_id, true() if region_id is None else false()),
#         or_(Stores.area_id == area_id, true() if area_id is None else false()),
#         or_(IssueDescription.category == category_id, true() if category_id is None else false()),
#     )
#     count = {"total_count": results.count()}
#     return results.limit(10).offset((page - 1) * per_page).all(), count


# def store_map_data(db, store_id, region_id, area_id):
#     per_page = 10
#     result = db.query(Stores.name, Stores.region_id, Stores.area_id,
#              IssueDescription.problem, IssueDescription.comment,
#              IssueCategory.category_name, Stores.latitude, Stores.longitude, IssueCategory.category_color) \
#         .join(IssueDescription, Stores.id == IssueDescription.store_id) \
#         .join(IssueCategory, IssueDescription.category == IssueCategory.id) \
#         .filter(
#         or_(IssueDescription.store_id == store_id, true() if store_id is None else false()),
#         or_(Stores.region_id == region_id, true() if region_id is None else false()),
#         or_(Stores.area_id == area_id, true() if area_id is None else false()),
#     ) \
#         .all()
#     return result


# def add_issue_comment(db, details):
#     try:
#         store_id = details["store_id"]
#         comment = details["comment"]
#         db.query(IssueDescription).filter_by(store_id=store_id).update({IssueDescription.comment: comment})
#         db.commit()
#         return {
#             "message": "Comment added successfully",
#             "status_code":200
#         }
#     except:
#         return {
#             "message": "Internal server error",
#             "status_code":500
#         }


def fetch_store_details(db, store_id = None, region_id=None, zone=None):
    result = db.query(
        Stores
    ).filter(
        or_(Stores.id == store_id, true() if not store_id else False),
        or_(Stores.company_region_id == region_id, true() if not region_id else false()),
        or_(Stores.zone == zone, true() if not zone else false()),
        Stores.store_running == 1
    ).all()

    return result


def fetch_store_regions(db, store_id=None, zone=None):
    result = db.query(
        distinct(Stores.company_region_id).label('region_id')
    ).filter(
        Stores.store_running == 1,
        or_(Stores.id == store_id, true() if not store_id else False),
        or_(Stores.zone == zone, true() if not zone else false()),
    ).order_by(Stores.company_region_id).all()

    return result


def fetch_store_zones(db, store_id=None, region_id=None):
    result = db.query(
        distinct(Stores.zone).label('zone')
    ).filter(
        Stores.store_running == 1,
        or_(Stores.company_region_id == region_id, true() if not region_id else false()),
        or_(Stores.id == store_id, true() if not store_id else False),
    ).order_by(Stores.zone).all()

    return result


def fetch_searched_store(query, region_id, zone, hyper_care_stores, db):
    query = db.query(
        Stores.id, Stores.name, Stores.company_region_id, Stores.zone
    ).filter(
        Stores.store_running == 1,
        or_(Stores.company_region_id == region_id, true() if not region_id else false()),
        or_(Stores.zone == zone, true() if not zone else false()),
        or_(Stores.name.like(f'%{query}%'), Stores.name.like(f'%{query}'), Stores.name.like(f'{query}%'))
    )

    if hyper_care_stores:
        query = query.filter(Stores.id.in_(HYPERCARE_STORES))

    result = query.order_by(Stores.name).all()

    return result


def fetch_all_stores_sco_alerts_details(db,store_id):
    result = db.query(
        StoresScoAutomationConfig.store_id,
        Stores.name,
        StoresScoAutomationConfig.miss_scan,
        StoresScoAutomationConfig.in_basket,
        StoresScoAutomationConfig.in_hand,
        StoresScoAutomationConfig.incomplete_payment,
        StoresScoAutomationConfig.item_stacking,
        StoresScoAutomationConfig.item_switching,
        StoresScoAutomationConfig.on_scanner
    ).join(
        Stores, StoresScoAutomationConfig.store_id == Stores.store_actual_id
    ).filter(
        or_(Stores.store_actual_id== store_id, true() if not store_id else false()),
    ).order_by(
        Stores.name
    ).all()

    return result


def update_stores_sco_alerts_details(store_id, data, db):
    db.query(
        StoresScoAutomationConfig
    ).filter(
        StoresScoAutomationConfig.store_id == store_id
    ).update(
        {
            "miss_scan": data["miss_scan"],
            "item_switching": data["item_switching"],
            "item_stacking": data["item_stacking"],
            "on_scanner": data["on_scanner"],
            "in_hand": data["in_hand"],
            "in_basket": data["in_basket"],
            "incomplete_payment": data["incomplete_payment"]
        }
    )

    db.commit()

    return {"message": "Done"}


def insert_sco_change_logs(store_id, data, db):
    # print(data)
    data = dict(
        store_id=store_id,
        email=data['email'],
        past_value=data['pastValue'],
        field=data['field'],
        new_value=data['currentValue'],
        changed_at= datetime.utcnow()
    )
    
    log_data = ScoConfigChangeLogs(**data)

    db.add(log_data)
    db.commit()


def fetch_all_sco_config_update_log(db,page):
    per_page = 10
    count = {"total_count": 0}
    
    result = db.query(
        ScoConfigChangeLogs.store_id,
        Stores.name,
        ScoConfigChangeLogs.email,
        ScoConfigChangeLogs.past_value,
        ScoConfigChangeLogs.field,
        ScoConfigChangeLogs.new_value,
        ScoConfigChangeLogs.changed_at
    ).join(
        Stores, ScoConfigChangeLogs.store_id == Stores.store_actual_id
    ).order_by(desc(ScoConfigChangeLogs.changed_at))

    count["total_count"]= result.count()
    return result.limit(10).offset((page - 1) * per_page).all(), count


def fetch_xml_stores_details(db_xml_dev):
    result = db_xml_dev.query(
        StoresInternalDev.store_id,
        StoresInternalDev.store_name,
        StoresInternalDev.store_num
    ).all()

    return result
