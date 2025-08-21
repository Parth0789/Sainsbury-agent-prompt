
from sqlalchemy import func, and_, distinct, desc, or_, true, false, asc, extract, text,Integer, case
from sqlalchemy.sql.expression import cast
from model.model import Transactions, Stores, Transaction_items, Operators, Comments, TransactionsLossNoLossData
from sqlalchemy.orm import Session, aliased
from datetime import datetime, timedelta
import itertools
import pandas as pd



from utils import current_date_time

def get_data_by_clubcard(db: Session, store_id, region_id, area_id, start_time, end_time, page,sortOrder):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()
    per_page = 10
    max_begin_date_subquery = db.query(
        func.max(Transactions.begin_date).label('max_date'),
        Transactions.clubcard
    ) \
        .filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false()),
        Transactions.clubcard.isnot(None),
        Transactions.hidden == 0,
        Transactions.description != "",
        Transactions.missed_scan == 1,
        func.date(Transactions.begin_date) >= start_time,
        func.date(Transactions.begin_date) <= end_time
    ) \
        .group_by(Transactions.clubcard) \
        .subquery()

    main_sco_data = db.query(func.count(Transactions.id).label(
                                       'total'),Transactions.clubcard,Transactions.name,Transactions.staffcard, max_begin_date_subquery.c.max_date) \
                .join(
                max_begin_date_subquery,
                max_begin_date_subquery.c.clubcard == Transactions.clubcard
            ) \
        .filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false()),
        Transactions.clubcard.isnot(None)
        )\
        .filter(Transactions.hidden == 0, Transactions.description != "", Transactions.missed_scan == 1)\
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.clubcard) \
        .having(func.count(Transactions.id) > 1)
    count = {"total_count": main_sco_data.count()}

    if sortOrder == "DESC" or sortOrder == "desc":
        main_sco_data = main_sco_data.order_by(desc("total"))
    elif sortOrder=="ASC" or sortOrder == "asc":
        main_sco_data = main_sco_data.order_by(asc("total"))

    return main_sco_data.limit(10).offset((page - 1) * per_page).all(), count




def get_data_by_clubcard_trigger(db: Session, store_id, region_id, area_id, start_time, end_time, trigger, page,sortOrder):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()
    per_page = 10
    max_begin_date_subquery = db.query(
        func.max(Transactions.begin_date).label('max_date'),
        Transactions.clubcard
    ) \
        .filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false()),
        Transactions.description != "",
        Transactions.triggers == trigger,
        func.date(Transactions.begin_date) >= start_time,
        func.date(Transactions.begin_date) <= end_time
    ) \
        .group_by(Transactions.clubcard) \
        .subquery()

    main_sco_data = db.query(func.count(Transactions.id).label(
                                       'total'),Transactions.clubcard,Transactions.name,Transactions.staffcard, max_begin_date_subquery.c.max_date) \
                .join(
                max_begin_date_subquery,
                max_begin_date_subquery.c.clubcard == Transactions.clubcard
            ) \
        .filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false()),
        Transactions.clubcard.isnot(None)
        )\
        .filter(Transactions.description != "") \
        .filter(func.date(Transactions.begin_date) >= start_time) \
        .filter(func.date(Transactions.begin_date) <= end_time) \
        .group_by(Transactions.clubcard) \
        .having(func.count(Transactions.id) >= 1)
    count = {"total_count": main_sco_data.count()}

    if sortOrder == "DESC" or sortOrder == "desc":
        main_sco_data = main_sco_data.order_by(desc("total"))
    elif sortOrder=="ASC" or sortOrder == "asc":
        main_sco_data = main_sco_data.order_by(asc("total"))

    return main_sco_data.limit(10).offset((page - 1) * per_page).all(), count


def get_clubcard_detail(db: Session,  store_id, region_id, area_id, start_time, end_time,clubcard, page):
    if start_time == None or start_time == "":
        start_time, end_time = current_date_time()
    per_page = 10
    description = func.coalesce(Transactions.checked_items, 0) - func.coalesce(Transactions.total_number_of_items, 0)

    data1 = db.query(
        Stores.name.label("store"), Transactions.transaction_id,Transactions.sequence_no,Transactions.counter_no,
        description.label("description"),Transactions.missed_scan,Transactions.clubcard,Transactions.video_link, 
        Transactions.begin_date
    ).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Transactions.description != "",
        Transactions.missed_scan == 1,
        Transactions.clubcard == clubcard,
        func.date(Transactions.begin_date) >= start_time,
        func.date(Transactions.begin_date) <= end_time
    ).order_by(
        desc(func.date(Transactions.begin_date))
    )

    count = {"total_count": data1.count()}
    data1 = data1.order_by(desc(Transactions.begin_date))
    return data1.limit(10).offset((page - 1) * per_page).all(), count


def get_clubcard_detail_trigger(db: Session, store_id, region_id, area_id, start_time, end_time, clubcard, nudge_type,
                                sort_field, comment, hidden, sort_order, page, client_region_id, updated, operator_id):
    if start_time is None or start_time == "":
        start_time, end_time = current_date_time()
    per_page = 10
    description = func.coalesce(Transactions.checked_items, 0) - func.coalesce(Transactions.total_number_of_items, 0)

    data1 = db.query(
        Stores.name.label("store"), 
        Transactions.transaction_id, 
        Transactions.sequence_no, 
        Transactions.counter_no,
        Operators.operator_id, 
        description.label("description"), 
        Transactions.missed_scan, 
        # Transactions.clubcard,
        case(
            [
                (Transactions.clubcard == "Rectified", "Corrected"),
                (Transactions.clubcard == "Non-Rectified", "Failed"),
                (Transactions.clubcard == "Not-Present", "Monitored"),
            ],
            else_=Transactions.clubcard
        ).label("clubcard"),
        Transactions.video_link, 
        Transactions.begin_date, 
        Transactions.video_link_1,
        Transactions.description.label("nudge_type"), 
        func.count(Transaction_items.transaction_id).label("nudge_count")
    ).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.company_region_id == client_region_id, true() if client_region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).join(
        Transaction_items, and_(Transaction_items.transaction_id == Transactions.transaction_id,
                                Transaction_items.missed == 1), isouter=True
    ).join(
        Operators, Transactions.operator_id == Operators.id, isouter=True
    ).filter(
        Transactions.description != "",
        Transactions.hidden == hidden,
        Transactions.transaction_updated == updated,
        Stores.store_running == 1,
        or_(Transactions.description == nudge_type, true() if not nudge_type else false()),
        or_(Transactions.clubcard == clubcard, true() if not clubcard else false()),
        or_(Operators.operator_id == operator_id, Operators.operator_id.is_(None) if operator_id is None else false()),
        func.date(Transactions.begin_date) >= start_time,
        func.date(Transactions.begin_date) <= end_time
    )

    transactions_to_check = db.query(
        Comments.transaction_id
    ).filter(
        or_(Comments.sai_comments.like(f"{comment}%"), Comments.body.like(f"{comment}%"))
    ).all()
    transactions_to_check = [record["transaction_id"] for record in transactions_to_check]

    # if clubcard and clubcard == "Non-Rectified":
    #     if clubcard == "Non-Rectified":
    #         data1 = data1.filter(
    #             or_(
    #                 Transactions.clubcard == clubcard,
    #                 Transactions.transaction_id.in_(loss_transaction_list)
    #             )
    #         )
    # if clubcard:
    #     if clubcard == "Rectified":
    #         data1 = data1.filter(
    #             Transactions.clubcard == clubcard,
    #             Transactions.transaction_id.not_in(loss_transaction_list)
    #         )
    #     else:
    #         data1 = data1.filter(Transactions.clubcard == clubcard)

    if clubcard:
        data1 = data1.filter(Transactions.transaction_id.not_in(transactions_to_check))
    elif comment:
        data1 = data1.filter(Transactions.transaction_id.in_(transactions_to_check))

    data1 = data1.group_by(Transactions.transaction_id)

    if sort_field == "store":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(Stores.name))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(Stores.name))
    elif sort_field == "sequence_no":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(cast(Transactions.sequence_no, Integer)))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(cast(Transactions.sequence_no, Integer)))
    elif sort_field == "counter_no":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(cast(Transactions.counter_no, Integer)))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(cast(Transactions.counter_no, Integer)))
    elif sort_field == "nudge_type":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(Transactions.description))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(Transactions.description))
    elif sort_field == "begin_date":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(Transactions.begin_date))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(Transactions.begin_date))

    count = {"total_count": data1.count()}
    # if clubcard == "Non-Rectified":
    #     count["total_count"] += len(loss)

    data1 = data1.all()
    if sort_field == "nudge_count":
        data1.sort(key=lambda x: x["nudge_count"], reverse=False if sort_order == "ASC" else True)

    # return data1.limit(10).offset((page - 1) * per_page).all(), count
    return data1[(page-1)*per_page: page*per_page], count


def get_clubcard_detail_trigger_new(db: Session,  store_id, region_id, area_id, start_time, end_time, clubcard, nudge_type,
                                sort_field, comment, hidden, sort_order, page, client_region_id):
    if start_time is None or start_time == "":
        start_time, end_time = current_date_time()
    per_page = 10
    description = func.coalesce(Transactions.checked_items, 0) - func.coalesce(Transactions.total_number_of_items, 0)

    data1 = db.query(
        Stores.name.label("store"), Transactions.transaction_id, Transactions.sequence_no, Transactions.counter_no,
        Operators.operator_id, description.label("description"), Transactions.missed_scan, Transactions.clubcard,
        Transactions.video_link, Transactions.begin_date, Transactions.video_link_1,
        Transactions.description.label("nudge_type"), func.count(Transaction_items.transaction_id).label("nudge_count")
    ).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == client_region_id, true() if client_region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).join(
        Transaction_items, and_(Transaction_items.transaction_id == Transactions.transaction_id,
                                Transaction_items.missed == 1), isouter=True
    ).join(
        Operators, Transactions.operator_id == Operators.id, isouter=True
    ).filter(
        Transactions.description != "",
        Transactions.hidden == hidden,
        Stores.store_running == 1,
        or_(Transactions.description == nudge_type, true() if not nudge_type else false()),
        or_(Transactions.clubcard == clubcard, true() if not clubcard else false()),
        func.date(Transactions.begin_date) >= start_time,
        func.date(Transactions.begin_date) <= end_time
    ).group_by(Transactions.transaction_id)

    transactions_to_check = db.query(
        Comments.transaction_id
    ).filter(
        or_(Comments.sai_comments.like(f"{comment}%"), Comments.body.like(f"{comment}%"))
    ).all()
    transactions_to_check = [record["transaction_id"] for record in transactions_to_check]

    if clubcard:
        data1 = data1.filter(Transactions.transaction_id.not_in(transactions_to_check))
    elif comment:
        data1 = data1.filter(Transactions.transaction_id.in_(transactions_to_check))

    if sort_field == "store":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(Stores.name))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(Stores.name))
    elif sort_field == "sequence_no":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(cast(Transactions.sequence_no, Integer)))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(cast(Transactions.sequence_no, Integer)))
    elif sort_field == "counter_no":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(cast(Transactions.counter_no, Integer)))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(cast(Transactions.counter_no, Integer)))
    elif sort_field == "nudge_type":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(Transactions.description))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(Transactions.description))
    elif sort_field == "operator_id":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(Transactions.operator_id))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(Transactions.operator_id))
    elif sort_field == "nudge_count":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(func.count(Transaction_items.transaction_id)))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(func.count(Transaction_items.transaction_id)))
    elif sort_field == "begin_date":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(Transactions.begin_date))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(Transactions.begin_date))

    count = {"total_count": data1.count()}
    data1 = data1.limit(10).offset((page - 1) * per_page).all()
    # if sort_field == "nudge_count":
    #     data1.sort(key=lambda x: x["nudge_count"], reverse=False if sort_order == "ASC" else True)

    # return data1.limit(10).offset((page - 1) * per_page).all(), count
    return data1, count

def get_clubcard_detail_trigger_to_update(
        db: Session, store_id, region_id, area_id, start_time, end_time, clubcard, nudge_type, sort_field, comment,
        hidden, sort_order, page, client_region_id, updated
):
    if start_time is None or start_time == "":
        start_time, end_time = current_date_time()
    per_page = 10
    description = func.coalesce(Transactions.checked_items, 0) - func.coalesce(Transactions.total_number_of_items, 0)

    data1 = db.query(
        Stores.name.label("store"), Transactions.transaction_id, Transactions.sequence_no, Transactions.counter_no,
        description.label("description"), Transactions.missed_scan, Transactions.clubcard, Transactions.video_link,
        Transactions.begin_date, Transactions.video_link_1, Transactions.description.label("nudge_type"),
        func.count(Transaction_items.transaction_id).label("nudge_count")
    ).filter(
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
        or_(Stores.company_region_id == client_region_id, true() if client_region_id is None else false()),
        or_(Stores.area_id == area_id, true() if area_id is None else false())
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).join(
        Transaction_items, and_(Transaction_items.transaction_id == Transactions.transaction_id,
                                Transaction_items.missed == 1), isouter=True
    ).filter(
        Transactions.hidden == hidden,
        Transactions.transaction_updated == updated,
        Stores.store_running == 1,
        or_(Transactions.description == nudge_type, true() if not nudge_type else false()),
        or_(Transactions.clubcard == clubcard, true() if not clubcard else false()),
        func.date(Transactions.begin_date) >= start_time,
        func.date(Transactions.begin_date) <= end_time
    ).group_by(Transactions.transaction_id)

    transactions_to_check = db.query(
        Comments.transaction_id
    ).filter(
        or_(Comments.sai_comments.like(f"{comment}%"), Comments.body.like(f"{comment}%"))
    ).all()
    transactions_to_check = [record["transaction_id"] for record in transactions_to_check]

    if clubcard:
        data1 = data1.filter(Transactions.transaction_id.not_in(transactions_to_check))
    elif comment:
        data1 = data1.filter(Transactions.transaction_id.in_(transactions_to_check))

    if sort_field == "store":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(Stores.name))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(Stores.name))
    elif sort_field == "sequence_no":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(cast(Transactions.sequence_no, Integer)))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(cast(Transactions.sequence_no, Integer)))
    elif sort_field == "counter_no":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(cast(Transactions.counter_no, Integer)))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(cast(Transactions.counter_no, Integer)))
    elif sort_field == "nudge_type":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(Transactions.description))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(Transactions.description))
    elif sort_field == "begin_date":
        if sort_order == "ASC":
            data1 = data1.order_by(asc(Transactions.begin_date))
        elif sort_order == "DESC":
            data1 = data1.order_by(desc(Transactions.begin_date))

    count = {"total_count": data1.count()}
    data1 = data1.all()
    if sort_field == "nudge_count":
        data1.sort(key=lambda x: x["nudge_count"], reverse=False if sort_order == "ASC" else True)

    # return data1.limit(10).offset((page - 1) * per_page).all(), count
    return data1[(page-1)*per_page: page*per_page], count
