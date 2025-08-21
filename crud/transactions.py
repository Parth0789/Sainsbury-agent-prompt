from sqlalchemy import or_, and_, true, false, case, select, desc, distinct
from sqlalchemy.sql import func, expression
from sqlalchemy.orm import Session, aliased
from datetime import date, timedelta, datetime

from model.model import (
    Transactions, Stores, Transaction_items, CameraInfo, Comments, StoreHours, OverallReportAggregatedResult, Operators,
    TransactionDetailsSCOInternalDev, TransactionSCOAlertInternalDev, StoresInternalDev
)
from utils.s3_utils import transfer_video_in_s3


def fetch_incomplete_payments_transactions(store_id: int, start_date: str, end_date: str, db: Session):
    result = db.query(
        Transactions.transaction_id.label('transaction_id'), Stores.id.label('dashboard_store_id'),
        Comments.id.label('comment_id'),
        Stores.name.label('store_name'), Stores.store_num.label("store_id"),
        CameraInfo.camera_ip.label('camera_id'),
        Transactions.counter_no.label('camera_location'), Transactions.sequence_no.label('sequence_no'),
        Stores.store_start_time.label("recording_start_timestamp"),
        Stores.store_end_time.label("recording_end_timestamp"),
        Transactions.begin_date.label("transaction_start_timestamp"),
        Transactions.end_date.label("transaction_end_timestamp"),
        Transactions.end_date.label("intervention_notification_timestamp"),
        expression.literal("").label("intervention_notification_cleared_timestamp"),
        Transactions.description.label("intervention_notification_type"),
        Transactions.transaction_key.label("transaction_key"),
        expression.literal("").label("mis_scanned_item_information"),
        expression.literal("").label("scan_id"),
        expression.literal("").label("next_item_scanned_scan_id"),
        expression.literal("").label("second_item_scanned_scan_id"),
        expression.literal("").label("rectified_not_rectified"),
        expression.literal("").label("price"),
        Comments.sai_comments.label("sai_comments"),
        Comments.body.label("sainsburys_comments")
    ).join(
        Stores, Stores.id == Transactions.store_id
    ).join(
        CameraInfo, and_(Transactions.counter_no == CameraInfo.counter_no,
                         Transactions.store_id == CameraInfo.store_id), isouter=True
    ).join(
        Comments,Transactions.transaction_id == Comments.transaction_id, isouter=True
    ).filter(
        or_(Stores.id == store_id, true() if store_id is None else false()),
        Transactions.begin_date >= start_date,
        Transactions.begin_date <= end_date,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Persistent Memory"]),
        Transactions.description == "Incomplete Payment",
        or_(CameraInfo.store_id == store_id, true() if store_id is None else false())
    ).all()

    return result


def fetch_incomplete_payments_transactions_download(store_id: int, start_date: str, end_date: str, db: Session):
    result = db.query(
        Stores.name.label('Store Name'), Stores.store_num.label("Store Id"),
        CameraInfo.camera_ip.label('Camera ID'),
        Transactions.counter_no.label('Camera Location'), Transactions.sequence_no.label('Sequence No.'),
        Stores.store_start_time.label('Recording Start Timestamp'),
        Stores.store_end_time.label("Recording End Timestamp"),
        Transactions.begin_date.label("Transaction Start Timestamp"),
        Transactions.end_date.label("Transaction End Timestamp"),
        Transactions.end_date.label("Intervention / Notification Timestamp"),
        expression.literal("").label("Intervention / Notification Cleared Timestamp"),
        Transactions.description.label("Intervention / Notification Type"),
        Transactions.transaction_key.label("Transaction Key"),
        expression.literal("").label("Mis-Scanned Item information"),
        expression.literal("").label("Scan id"),
        expression.literal("").label("Next item scanned (Scan ID)"),
        expression.literal("").label("2nd item scanned (Scan ID)"),
        expression.literal("").label("Rectified/not rectified"),
        expression.literal("").label("Price"),
        Comments.sai_comments.label("Sai Comments"),
        Comments.body.label("Sainsbury comments")
    ).join(
        Stores, Stores.id == Transactions.store_id
    ).join(
        CameraInfo, and_(Transactions.counter_no == CameraInfo.counter_no,
                         Transactions.store_id == CameraInfo.store_id), isouter=True
    ).join(
        Comments,Transactions.transaction_id == Comments.transaction_id, isouter=True
    ).filter(
        or_(Stores.id == store_id, true() if store_id is None else false()),
        Transactions.begin_date >= start_date,
        Transactions.begin_date <= end_date,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Persistent Memory"]),
        Transactions.description == "Incomplete Payment",
        or_(CameraInfo.store_id == store_id, true() if store_id is None else false())
    ).all()

    return result


def fetch_missed_items_report(store_id: int, start_date: date, end_date: date, page: int, per_page: int, db: Session):
    start_date = date.strftime(start_date, "%Y-%m-%d")
    end_date = date.strftime(end_date, "%Y-%m-%d")

    result = db.query(
        Transaction_items.id.label('transaction_items_id'),
        Transactions.transaction_id.label('transaction_id'), Stores.id.label('dashboard_store_id'),
        Comments.id.label('comment_id'), Stores.name.label('store_name'), Stores.store_num.label("store_id"),
        CameraInfo.camera_ip.label('camera_id'), Transactions.counter_no.label('camera_location'),
        Transactions.sequence_no.label('sequence_no'),
        Operators.operator_id.label("operator_id"),
        StoreHours.opening_time.label("recording_start_timestamp"),
        StoreHours.closing_time.label("recording_end_timestamp"),
        Transactions.begin_date.label("transaction_start_timestamp"),
        Transactions.end_date.label("transaction_end_timestamp"),
        Transaction_items.begin_date_time.label("intervention_notification_timestamp"),
        expression.literal("").label("intervention_notification_cleared_timestamp"),
        Transactions.description.label("intervention_notification_type"),
        Transactions.transaction_key.label("transaction_key"),
        Transaction_items.name.label("mis_scanned_item_information"),
        Transaction_items.item_id.label("scan_id"),
        expression.literal("").label("next_item_scanned_name"),
        expression.literal("").label("next_item_scanned_scan_id"),
        expression.literal("").label("second_item_scanned_name"),
        expression.literal("").label("second_item_scanned_scan_id"),
        case(
            [
                (Transactions.clubcard == "Rectified", "No Loss"),
                (Transactions.clubcard == "Non-Rectified", "Loss"),
                (Transactions.clubcard == "Not-Present", "Monitored")
            ],
            else_=Transactions.clubcard
        ).label("rectified_not_rectified"),
        Transaction_items.regular_sales_unit_price.label("price"),
        Comments.sai_comments.label("sai_comments"),
        Comments.body.label("sainsburys_comments"),
        Stores.monitor_type.label("control_trial")
    ).filter(
        or_(Stores.id == store_id, true() if store_id is None else false()),
        Transactions.begin_date >= start_date,
        Transactions.begin_date <= end_date,
        Transactions.description != "",
        Transaction_items.missed == 1,
        Transactions.hidden == 0,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Not-Present"])
    ).join(
        Transactions, Transactions.transaction_id == Transaction_items.transaction_id
    ).join(
        Stores, Stores.id == Transactions.store_id
    ).join(
        Operators, Transactions.operator_id == Operators.id, isouter=True
    ).join(
        CameraInfo, 
        and_(
            Transactions.counter_no == CameraInfo.counter_no,
            Transactions.store_id == CameraInfo.store_id
        ), 
        isouter=True
    ).join(
        Comments, Transactions.transaction_id == Comments.transaction_id, isouter=True
    ).join(
        StoreHours, 
        and_(
            Transactions.store_id == StoreHours.store_id,
            func.dayofweek(Transactions.begin_date) == StoreHours.day_of_week
        ), 
        isouter=True
    ).all()

    # incomplete_payment_result = fetch_incomplete_payments_transactions(store_id, start_date, end_date, db)
    # result += incomplete_payment_result
    return result


def fetch_missed_items_report_download(store_id: int, start_date: date, end_date: date, db: Session):
    start_date = date.strftime(start_date, "%Y-%m-%d")
    end_date = date.strftime(end_date, "%Y-%m-%d")

    ti2 = aliased(Transaction_items)

    next_item_subquery = (
        select([ti2.name])
        .where(and_(
            ti2.transaction_id == Transaction_items.transaction_id,
            ti2.name != 'item',
            ti2.begin_date_time > Transaction_items.begin_date_time
        ))
        .order_by(ti2.begin_date_time)
        .limit(1)
        .correlate(Transaction_items)
        .as_scalar()
    )

    next_item_scan_data_subquery = (
        select([ti2.scan_data])
        .where(and_(
            ti2.transaction_id == Transaction_items.transaction_id,
            ti2.name != 'item',
            ti2.begin_date_time > Transaction_items.begin_date_time
        ))
        .order_by(ti2.begin_date_time)
        .limit(1)
        .correlate(Transaction_items)
        .as_scalar()
    )

    second_item_subquery = (
        select([ti2.name])
        .where(and_(
            ti2.transaction_id == Transaction_items.transaction_id,
            ti2.name != 'item',
            ti2.begin_date_time > Transaction_items.begin_date_time
        ))
        .order_by(ti2.begin_date_time)
        .limit(1).offset(1)
        .correlate(Transaction_items)
        .as_scalar()
    )

    second_item_scan_data_subquery = (
        select([ti2.scan_data])
        .where(and_(
            ti2.transaction_id == Transaction_items.transaction_id,
            ti2.name != 'item',
            ti2.begin_date_time > Transaction_items.begin_date_time
        ))
        .order_by(ti2.begin_date_time)
        .limit(1).offset(1)
        .correlate(Transaction_items)
        .as_scalar()
    )

    query = (
        db.query(
            Stores.id.label('dashboard_store_id'),
            Stores.name.label('Store Name'),
            Stores.store_num.label('Store ID'),
            CameraInfo.camera_ip.label('Camera ID'),
            Transactions.counter_no.label('Camera Location'),
            Transactions.sequence_no.label('Sequence No.'),
            Operators.operator_id.label("Operator ID"),
            StoreHours.opening_time.label('Recording Start Timestamp'),
            StoreHours.closing_time.label("Recording End Timestamp"),
            Transactions.begin_date.label('Transaction Start Timestamp'),
            Transactions.end_date.label('Transaction End Timestamp'),
            Transaction_items.begin_date_time.label('Intervention / Notification Timestamp'),
            expression.literal('').label('Intervention / Notification Cleared Timestamp'),
            Transactions.description.label('Intervention / Notification Type'),
            Transactions.transaction_key.label('Transaction Key'),
            Transaction_items.name.label('Mis-Scanned Item Information'),
            Transaction_items.item_id.label('Scan ID'),
            # case(
            #     [
            #         (Transactions.clubcard == 'Rectified', Transaction_items.name)
            #     ],
            #     else_=next_item_subquery
            # ).label('Next item scanned (Scan ID)'),
            # case(
            #     [
            #         (Transactions.clubcard == 'Rectified', next_item_subquery)
            #     ],
            #     else_=second_item_subquery
            # ).label('2nd item scanned (Scan ID)'),
            next_item_subquery.label('Next Item Scanned (Name)'),
            next_item_scan_data_subquery.label('Next Item Scanned (Scan ID)'),
            second_item_subquery.label('2nd Item Scanned (Name)'),
            second_item_scan_data_subquery.label('2nd Item Scanned (Scan ID)'),
            case(
                [
                    (Transactions.clubcard == 'Rectified', 'No Loss'),
                    (Transactions.clubcard == 'Non-Rectified', 'Loss'),
                    (Transactions.clubcard == 'Not-Present', 'Monitored')
                ],
                else_=Transactions.clubcard
            ).label('No Loss/Loss'),
            Transaction_items.regular_sales_unit_price.label('Price'),
            Comments.sai_comments.label('SAI Comments'),
            Comments.body.label('Sainsburys Comments'),
            Stores.monitor_type.label('Control/Trial')
        )
        .join(Transaction_items, Transactions.transaction_id == Transaction_items.transaction_id)
        .join(Stores, Stores.id == Transactions.store_id)
        .join(Operators, Transactions.operator_id == Operators.id, isouter=True)
        .outerjoin(Comments, Transactions.transaction_id == Comments.transaction_id)
        .outerjoin(CameraInfo, and_(
            Transactions.counter_no == CameraInfo.counter_no,
            Transactions.store_id == CameraInfo.store_id
        ))
        .outerjoin(
            StoreHours, and_(Transactions.store_id == StoreHours.store_id,
                             func.dayofweek(Transactions.begin_date) == StoreHours.day_of_week)
        )
        .filter(
            or_(Stores.id == store_id, true() if store_id is None else false()),
            Transactions.begin_date >= start_date,
            Transactions.begin_date <= end_date,
            Transactions.description != "",
            Transaction_items.missed == 1,
            Transactions.hidden == 0,
            Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Persistent Memory", "Not-Present"])
    ))

    result = query.all()
    # incomplete_payment_result = fetch_incomplete_payments_transactions_download(store_id, start_date, end_date, db)
    # result += incomplete_payment_result
    return result


def fetch_next_missed_item(db, time_stamp, transaction_id):
    result = db.query(
        Transaction_items.name, Transaction_items.scan_data
    ).filter(
        Transaction_items.transaction_id == transaction_id,
        Transaction_items.name != "item",
        Transaction_items.begin_date_time > time_stamp
    ).order_by(Transaction_items.begin_date_time).all()

    # if result:
    #     if item_number == 1:
    #         return result[0].name
    #     elif item_number == 2 and len(result) > 1:
    #         return result[1].name
    #     else:
    #         return ""

    return result


def update_clubcard_value(store_id, transaction_id, clubcard_value, db):
    db.query(
        Transactions
    ).filter_by(
        transaction_id = transaction_id,
        store_id = store_id
    ).update({
        "clubcard": clubcard_value
    })

    db.commit()


def update_transaction_data_in_db(transaction_id, transaction_data, db):
    db.query(
        Transactions
    ).filter_by(
        transaction_id = transaction_id
    ).update(transaction_data)

    db.commit()

def fetch_outcome_nudges(store_id, region_id, zone, start_date, end_date, db):
    query = db.query(
        Transactions.clubcard
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.description != "",
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.triggers == 5,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Not-Present"]),
        # Transactions.description.in_(["Item Missed Scan", "On Scanner", "In Hand", "Incomplete Payment"]),
        # func.date(Transactions.begin_date) >= start_date,
        # func.date(Transactions.begin_date) <= end_date,
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false()),
        or_(Stores.zone == zone, true() if zone is None else false()),
    )

    if start_date and end_date:
        query = query.filter(
            func.date(Transactions.begin_date) >= start_date,
            func.date(Transactions.begin_date) <= end_date
        )

    result = query.all()

    return result


def fetch_causes_nudges(store_id, region_id, zone, start_date, end_date, db):
    query = db.query(
        Transactions.description, func.count(Transactions.id).label("trigger_count")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified"]),
        Transactions.description != "",
        Transactions.triggers == 5,
        # Transactions.description.in_(["Item Missed Scan", "On Scanner", "Incomplete Payment", "In Hand"]),
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false()),
        or_(Stores.zone == zone, true() if zone is None else false()),
    )

    if start_date and end_date:
        query = query.filter(
            func.date(Transactions.begin_date) >= start_date,
            func.date(Transactions.begin_date) <= end_date
        )

    result = query.group_by(Transactions.description).all()

    return result


def fetch_triggered_nudges(store_id, region_id, start_date, end_date, db):
    result = db.query(
        Transactions.description
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Not-Present"]),
        Transactions.description.in_(["Item Missed Scan", "On Scanner", "Item Switching", "In Hand"]),
        func.date(Transactions.begin_date) >= start_date,
        func.date(Transactions.begin_date) <= end_date,
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
    ).all()

    return result


def fetch_muted_nudges(store_id, region_id, zone, start_date, end_date, db):
    query = db.query(
        Transactions.description, func.count(Transactions.id).label("trigger_count")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard.in_(["Not-Present"]),
        Transactions.description != "",
        Transactions.triggers == 5,
        # Transactions.description.in_(["Basket", "Item Switching", "Item Stacking", "Floor", "Trolley", "In Hand", "On Scanner", "Item Missed Scan"]),
        # func.date(Transactions.begin_date) >= start_date,
        # func.date(Transactions.begin_date) <= end_date,
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false()),
        or_(Stores.zone == zone, true() if zone is None else false()),
    )

    if start_date and end_date:
        query = query.filter(
            func.date(Transactions.begin_date) >= start_date,
            func.date(Transactions.begin_date) <= end_date
        )

    result = query.group_by(Transactions.description).all()

    return result


def fetch_monitored_nudges(store_id, region_id, start_date, end_date, db):
    result = db.query(
        Transactions.description
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Not-Present"]),
        Transactions.description.in_(["Basket", "Item Switching"]),
        func.date(Transactions.begin_date) >= start_date,
        func.date(Transactions.begin_date) <= end_date,
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
    ).all()

    return result


def fetch_overall_nudges(store_id, region_id, zone, start_date, end_date, db):
    query = db.query(
        Transactions.clubcard, func.count(Transactions.id).label("count")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified"]),
        # Transactions.description.in_(["Item Missed Scan","Missed Scan", "On Scanner", "In Hand", "Incomplete Payment"]),
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false()),
        or_(Stores.zone == zone, true() if zone is None else false()),
    )

    if start_date and end_date:
        query = query.filter(
            func.date(Transactions.begin_date) >= start_date,
            func.date(Transactions.begin_date) <= end_date
        )

    result = query.group_by(Transactions.clubcard).all()

    return result


def fetch_sco_main_bank_nudges_count(store_id, region_id, zone, start_date, end_date, db):
    query = db.query(
        func.sum(
            case(
                [(Transactions.source_id == 1, 1)],
                else_=0
            )
        ).label("main_bank_count"),
        func.sum(
            case(
                [(Transactions.source_id == 2, 1)],
                else_=0
            )
        ).label("sco_count")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard.in_(["Non-Rectified"]),
        Transactions.description.in_(["Item Missed Scan","Missed Scan", "On Scanner", "In Hand", "Incomplete Payment"]),
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false()),
        or_(Stores.zone == zone, true() if zone is None else false()),
    )

    if start_date and end_date:
        query = query.filter(
            func.date(Transactions.begin_date) >= start_date,
            func.date(Transactions.begin_date) <= end_date
        )

    result = query.one()

    return result


def fetch_nudges_per_store(store_id, region_id, start_date, end_date, db):
    query = db.query(
        Stores.name, func.count(Transactions.id).label("total_count"),
        func.sum(case(
            [
                (Transactions.description.in_(["Item Missed Scan", "On Scanner", "Item Switching", "In Hand"]), 1)
            ],
            else_=0
        )).label("trigger_count")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.triggers == 5,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Not-Present"]),
        # Transactions.description.in_(["Item Missed Scan", "On Scanner", "Item Switching", "In Hand"]),
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false()),
    )

    if start_date and end_date:
        query = query.filter(func.date(Transactions.begin_date) >= start_date,
                             func.date(Transactions.begin_date) <= end_date)

    result = query.group_by(
        Transactions.store_id
    ).all()

    return result


def fetch_nudges_details_per_store(store_id, region_id, zone, start_date, end_date, db):
    query = db.query(
        Stores.id.label("store_id"), 
        Stores.name.label("store"), 
        Stores.company_region_id.label("region"),
        Stores.zone.label("zone"),
        func.sum(
            case(
                [
                    # and_(
                    #     Transactions.clubcard == "Rectified",
                    #     Transactions.description.in_(
                    #         ["Item Missed Scan", "On Scanner", "In Hand", "Incomplete Payment"]
                    #     ),
                    # )
                    (Transactions.clubcard == "Rectified", 1)
                ],
                else_=0
            )
        ).label("protected"),
        func.sum(
            case(
                [
                    # and_(
                    #     Transactions.clubcard.in_(["Rectified", "Non-Rectified"]),
                    #     Transactions.description.in_(
                    #         ["Item Missed Scan", "On Scanner", "In Hand", "Incomplete Payment"])
                    # )
                    (Transactions.clubcard.in_(["Rectified", "Non-Rectified"]), 1)
                ],
                else_=0
            )
        ).label("nudges")
        # func.sum(
        #     case(
        #         (
        #             (
        #                 Transactions.clubcard.in_(
        #                     ["Rectified", "Non-Rectified", "Not-Present"]
        #                 ),
        #                 1
        #             )
        #         ),
        #         else_=0
        #     )
        # ).label("nudges")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.triggers == 5,
        Transactions.description != "",
        # Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Not-Present"]),
        # Transactions.description.in_(["Item Missed Scan", "On Scanner", "In Hand", "Incomplete Payment"]),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false()),
        or_(Stores.zone == zone, true() if zone is None else false()),
    )

    if store_id:
        query = query.filter(Transactions.store_id.in_(store_id))
    if start_date and end_date:
        query = query.filter(func.date(Transactions.begin_date) >= start_date,
                             func.date(Transactions.begin_date) <= end_date)

    result = query.group_by(Transactions.store_id).all()

    return result


def fetch_total_transactions(store_ids, start_date, end_date, db_xml):
    query = db_xml.query(
        func.sum(OverallReportAggregatedResult.transaction_count).label("total_transactions")
    )

    if store_ids:
        if isinstance(store_ids, list):
            query = query.filter(OverallReportAggregatedResult.store_id.in_(store_ids))
        else:
            query = query.filter(OverallReportAggregatedResult.store_id == store_ids)

    if start_date and end_date:
        query = query.filter(func.date(OverallReportAggregatedResult.begin_date) >= start_date,
                               func.date(OverallReportAggregatedResult.begin_date) <= end_date)

    result = query.scalar()

    return result


def fetch_total_transactions_store_wise(store_ids, start_date, end_date, db_xml):
    query = db_xml.query(
        OverallReportAggregatedResult.store_id,
        func.sum(OverallReportAggregatedResult.transaction_count).label("total_transactions")
    ).filter(
        OverallReportAggregatedResult.store_id.in_(store_ids)
    )

    if start_date and end_date:
        query = query.filter(func.date(OverallReportAggregatedResult.begin_date) >= start_date,
                               func.date(OverallReportAggregatedResult.begin_date) <= end_date)

    result = query.group_by(OverallReportAggregatedResult.store_id).all()

    return result


def fetch_transactions_with_nudges(store_id, region_id, zone, start_date, end_date, db):
    query = db.query(
        func.count(Transactions.id).label("total"),
        func.sum(
            case(
                [
                    # and_(
                    #     Transactions.description.in_(["Basket", "Item Switching", "Item Stacking", "Floor", "Trolley"]),
                    #     Transactions.clubcard == "Not-Present"
                    # )
                    (Transactions.clubcard == "Not-Present", 1)
                ],
                else_=0
            )
        ).label("muted"),
        func.sum(
            case(
                [
                    # and_(
                    #     Transactions.description.in_(["Item Missed Scan", "On Scanner", "Incomplete Payment", "In Hand"]),
                    #     (Transactions.clubcard.in_(["Rectified", "Non-Rectified"]))
                    # )
                    (Transactions.clubcard.in_(["Rectified", "Non-Rectified"]), 1)
                ],
                else_=0
            )
        ).label("triggered"),
        func.sum(
            case(
                [
                    # and_(
                    #     Transactions.description.in_(["Item Missed Scan", "On Scanner", "Incomplete Payment", "In Hand"]),
                    #     (Transactions.clubcard.in_(["Rectified", "Non-Rectified"]))
                    # )
                    (Transactions.clubcard.in_(["Non-Rectified"]), 1)
                ],
                else_=0
            )
        ).label("loss")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Not-Present"]),
        Transactions.description != "",
        Transactions.triggers == 5,
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false()),
        or_(Stores.zone == zone, true() if zone is None else false()),
    )

    if start_date and end_date:
        query = query.filter(func.date(Transactions.begin_date) >= start_date,
                               func.date(Transactions.begin_date) <= end_date)

    result = query.one()

    return result


def fetch_nudges_days_wise(store_id, region_id, nudge_type, days, db):
    result = db.query(
        func.date(Transactions.begin_date).label("date"),
        func.count(Transactions.id).label("nudges_count")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard == nudge_type,
        func.date(Transactions.begin_date).in_(days),
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.region_id == region_id, true() if region_id is None else false()),
    ).group_by(func.date(Transactions.begin_date)).order_by(desc(func.date(Transactions.begin_date))).all()

    return result


def fetch_missed_items_per_day(store_id, region_id, start_date, end_date, db):
    query = db.query(
        func.dayname(func.date(Transactions.begin_date)).label("date"),
        func.count(Transactions.id).label("missed_items_count")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified"]),
        Transactions.description.in_(["Item Missed Scan", "On Scanner", "Incomplete Payment", "In Hand"]),
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false()),
    )

    if start_date and end_date:
        query = query.filter(
            func.date(Transactions.begin_date) >= start_date,
            func.date(Transactions.begin_date) <= end_date,
        )

    result = query.group_by(
        func.dayname(func.date(Transactions.begin_date))
    ).order_by(
        desc(func.dayname(func.date(Transactions.begin_date)))
    ).all()

    return result


def fetch_missed_items_per_week(store_id, region_id, start_date, end_date, db):
    query = db.query(
        func.year(Transactions.begin_date).label("year"),
        func.week(Transactions.begin_date).label("week"),
        func.count(Transactions.id).label("missed_items_count")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified"]),
        Transactions.description.in_(["Item Missed Scan", "On Scanner", "Incomplete Payment", "In Hand"]),
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false()),
    )

    if start_date and end_date:
        query = query.filter(
            func.date(Transactions.begin_date) >= start_date,
            func.date(Transactions.begin_date) <= end_date,
        )

    result = query.group_by(
        func.year(Transactions.begin_date), func.week(Transactions.begin_date)
    ).order_by(
        desc(func.year(Transactions.begin_date)), desc(func.week(Transactions.begin_date))
    ).all()

    return result


def fetch_missed_items_per_hour(store_id, region_id, start_date, end_date, db):
    query = db.query(
        func.date(Transactions.begin_date).label("date"),
        func.hour(Transactions.begin_date).label("hour"),
        func.count(Transactions.id).label("missed_items_count")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified"]),
        # Transactions.description.in_(["Item Missed Scan", "On Scanner", "Incomplete Payment", "In Hand"]),
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false()),
    )

    if start_date and end_date:
        query = query.filter(
            func.date(Transactions.begin_date) >= start_date,
            func.date(Transactions.begin_date) <= end_date,
        )

    result = query.group_by(
        func.hour(Transactions.begin_date)
    ).order_by(
        desc(func.hour(Transactions.begin_date))
    ).all()

    return result

def fetch_nudge_types(db):
    result = db.query(
        distinct(Transactions.description).label("nudge_type")
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Not-Present"]),
        Transactions.description.in_(["Item Missed Scan", "On Scanner", "Basket", "In Hand", "Item Stacking",
                                      "Incomplete Payment", "Item Switching", "In Basket"])
    ).all()

    return result


def fetch_triggers_week(store_id, region_id, db):
    result = db.query(
        func.dayname(func.date(Transactions.begin_date)).label("day_of_week"),
        func.count(Transactions.id).label("total_count"),
        func.sum(
            case(
                [
                    (Transactions.description.in_(["Item Missed Scan", "On Scanner", "Item Switching", "In Hand"]), 1)
                ],
                else_=0
            )
        ).label("trigger_count")
    ).join(
        Stores, Stores.id == Transactions.store_id
    ).filter(
        func.week(func.date(Transactions.begin_date), 0) == func.week(func.current_date(), 0),  # Current week (Sunday start)
        func.year(func.date(Transactions.begin_date)) == func.year(func.current_date()),
        Transactions.hidden == 0,
        Stores.store_running == 1,
        Transactions.triggers == 5,
        Transactions.clubcard.in_(["Rectified", "Non-Rectified", "Not-Present"]),
        # Transactions.description.in_(["Item Missed Scan", "On Scanner", "Item Switching", "In Hand"]),
        or_(Transactions.store_id == store_id, true() if store_id is None else false()),
        or_(Stores.company_region_id == region_id, true() if region_id is None else false())
    ).group_by(
        func.dayname(func.date(Transactions.begin_date))
    ).all()

    return result


def fetch_triggered_percentage_data(db, store_id, region_id, start_date, end_date, time_range):
    """
    Fetch triggered percentage data for graphs.
    
    Args:
        db: Database session
        store_id: ID of the store (optional)
        region_id: ID of the region (optional)
        client_region_id: ID of the client region (optional)
        start_date: Start datetime for filtering
        end_date: End datetime for filtering
        time_range: Time range for the data (1W, 1M, 1Y)
        
    Returns:
        list: Triggered percentage data for graphs
    """
    from route.transactions import TimeRange
    from utils.datetime_utils import get_week_start_dates
    from datetime import datetime, timedelta
    
    # Calculate date difference in days
    date_diff = (end_date.date() - start_date.date()).days
    
    # Base query to get all transactions and nudges
    base_query = db.query(
        Transactions.store_id,
        Transactions.begin_date,
        Transactions.description,
        Transactions.clubcard
    ).join(
        Stores, Transactions.store_id == Stores.id
    ).filter(
        Stores.store_running == 1,
        Transactions.hidden == 0,
        Transactions.triggers == 5,
        Transactions.description != "",
        # Use exact datetime format with time component
        Transactions.begin_date >= start_date.strftime("%Y-%m-%d %H:%M:%S"),
        Transactions.begin_date <= end_date.strftime("%Y-%m-%d %H:%M:%S"),
        Transactions.clubcard.in_(["Rectified", "Non-Rectified"])
    )
    
    # Apply store and region filters if provided
    if store_id:
        base_query = base_query.filter(Transactions.store_id == store_id)
    if region_id:
        base_query = base_query.filter(Stores.company_region_id == region_id)
    
    # Execute query to get all transactions
    all_transactions = base_query.all()
    
    # Special case: If date range is >= 365 days or no dates were provided (using default), group by year
    if date_diff >= 365 or (start_date.month == 1 and start_date.day == 1 and end_date.date() == datetime.now().date() and time_range == TimeRange.ONE_YEAR):
        # Group by year only
        year_data = {}
        for transaction in all_transactions:
            # Convert string date to datetime
            transaction_datetime = datetime.strptime(transaction.begin_date, "%Y-%m-%d %H:%M:%S")
            year = transaction_datetime.year
            
            if year not in year_data:
                year_data[year] = {"total": 0, "triggered": 0}
            
            year_data[year]["total"] += 1
            if transaction.clubcard in ["Rectified", "Non-Rectified"]:
               # transaction.description in ["Item Missed Scan", "On Scanner", "Incomplete Payment", "In Hand"]:
                year_data[year]["triggered"] += 1
        
        # Calculate percentages
        result = []
        for year in sorted(year_data.keys()):
            if year_data[year]["total"] > 0:
                percentage = (year_data[year]["triggered"] / year_data[year]["total"]) * 100
            else:
                percentage = 0
            
            # Return year as a string for route handler
            result.append((str(year), round(percentage, 2)))
        
        return result
    
    # Process data based on time_range
    if time_range == TimeRange.ONE_WEEK:
        # For time ranges less than 7 days - show daily data for EXACT date range
        # Group by actual date
        date_data = {}
        
        # Initialize data structure for all dates in the range - using EXACT start_date and end_date
        current_date = start_date.date()
        while current_date <= end_date.date():
            date_str = current_date.strftime("%Y-%m-%d")
            date_data[date_str] = {"total": 0, "triggered": 0}
            current_date += timedelta(days=1)
        
        # Process transactions by actual date
        for transaction in all_transactions:
            # Convert string date to datetime
            transaction_datetime = datetime.strptime(transaction.begin_date, "%Y-%m-%d %H:%M:%S")
            transaction_date_str = transaction_datetime.date().strftime("%Y-%m-%d")
            
            if transaction_date_str in date_data:
                date_data[transaction_date_str]["total"] += 1
                if transaction.clubcard in ["Rectified", "Non-Rectified"]:
                   # transaction.description in ["Item Missed Scan", "On Scanner", "Incomplete Payment", "In Hand"]:
                    date_data[transaction_date_str]["triggered"] += 1
        
        # Calculate percentages for each date
        result = []
        for date_str in sorted(date_data.keys()):  # Sort chronologically
            if date_data[date_str]["total"] > 0:
                percentage = (date_data[date_str]["triggered"] / date_data[date_str]["total"]) * 100
            else:
                percentage = 0
            
            # Parse back to a date object for the route handler
            result.append((datetime.strptime(date_str, "%Y-%m-%d").date(), round(percentage, 2)))
        
    elif time_range == TimeRange.ONE_MONTH:
        # For time ranges less than 31 days - show weekly data based on proper week boundaries
        # Calculate week start and end dates
        week_ranges = get_week_start_dates(start_date.date(), end_date.date())
        
        # Group transactions by week
        week_data = {}
        for transaction in all_transactions:
            # Convert string date to datetime and get date part
            transaction_datetime = datetime.strptime(transaction.begin_date, "%Y-%m-%d %H:%M:%S")
            transaction_date = transaction_datetime.date()
            
            # Find which week this transaction belongs to
            week_range = None
            for week_start, week_end in week_ranges:
                if week_start <= transaction_date <= week_end:
                    week_range = (week_start, week_end)
                    break
            
            if week_range and week_range not in week_data:
                week_data[week_range] = {"total": 0, "triggered": 0}
            
            if week_range:
                week_data[week_range]["total"] += 1
                if transaction.clubcard in ["Rectified", "Non-Rectified"]:
                   # transaction.description in ["Item Missed Scan", "On Scanner", "Incomplete Payment", "In Hand"]:
                    week_data[week_range]["triggered"] += 1
        
        # Calculate percentages
        result = []
        for week_range in week_ranges:
            if week_range in week_data and week_data[week_range]["total"] > 0:
                percentage = (week_data[week_range]["triggered"] / week_data[week_range]["total"]) * 100
            else:
                percentage = 0
            
            # Return the week range and percentage
            result.append((week_range, round(percentage, 2)))
        
    elif time_range == TimeRange.ONE_YEAR:
        # For time ranges less than 365 days - show monthly data
        # Group by month and year
        month_year_data = {}
        for transaction in all_transactions:
            # Convert string date to datetime
            transaction_datetime = datetime.strptime(transaction.begin_date, "%Y-%m-%d %H:%M:%S")
            month = transaction_datetime.month
            year = transaction_datetime.year
            month_year_key = (year, month)  # Use tuple of (year, month) as key
            
            if month_year_key not in month_year_data:
                month_year_data[month_year_key] = {"total": 0, "triggered": 0}
            
            month_year_data[month_year_key]["total"] += 1
            if transaction.clubcard in ["Rectified", "Non-Rectified"]:
               # transaction.description in ["Item Missed Scan", "On Scanner", "Incomplete Payment", "In Hand"]:
                month_year_data[month_year_key]["triggered"] += 1
        
        # Calculate percentages only for months within the date range
        result = []
        
        # Generate all month-year combinations in the date range
        current_date = datetime(start_date.year, start_date.month, 1)
        end_month_date = datetime(end_date.year, end_date.month, 1)
        
        while current_date <= end_month_date:
            year = current_date.year
            month = current_date.month
            month_year_key = (year, month)
            
            if month_year_key in month_year_data and month_year_data[month_year_key]["total"] > 0:
                percentage = (month_year_data[month_year_key]["triggered"] / month_year_data[month_year_key]["total"]) * 100
            else:
                percentage = 0
                
            # Store the year and month for the route handler to use when formatting labels
            result.append(((year, month), round(percentage, 2)))
            
            # Move to next month
            if month == 12:
                current_date = datetime(year + 1, 1, 1)
            else:
                current_date = datetime(year, month + 1, 1)
        
        # Result is already sorted chronologically by construction
    
    return result


def fetch_upload_transaction_details(stores_id_list, start_time, end_time, db_xml_dev, nudge_type):
    query = db_xml_dev.query(
        TransactionSCOAlertInternalDev.transactionId,
        StoresInternalDev.name.label("store_name"),
        StoresInternalDev.store_num.label("store_id")
    ).join(
        StoresInternalDev, TransactionSCOAlertInternalDev.storeId == StoresInternalDev.id
    ).filter(
        TransactionSCOAlertInternalDev.Entrystatus == 0,
        TransactionSCOAlertInternalDev.skip == False,
        # Use correlated subquery to count items - more efficient than JOIN
        # (
        #     select([func.count(TransactionDetailsSCOInternalDev.TransactionID)])
        #     .where(TransactionDetailsSCOInternalDev.TransactionID == TransactionSCOAlertInternalDev.transactionId)
        #     .as_scalar()
        # ) <= 5
        # TransactionSCOAlertInternalDev.type != "Not Attended"
        # func.date(TransactionSCOAlertInternalDev.beginDate) < datetime.today().date()
    )

    if nudge_type == "not_attended":
        query = query.filter(
            TransactionSCOAlertInternalDev.type == "Not Attended"
        )
    else:
        query = query.filter(
            TransactionSCOAlertInternalDev.type != "Not Attended"
        )

    if start_time and end_time:
        query = query.filter(
            TransactionSCOAlertInternalDev.beginDate >= start_time,
            TransactionSCOAlertInternalDev.beginDate <= end_time
        )

    if stores_id_list:
        query = query.filter(
            TransactionSCOAlertInternalDev.storeId.in_(stores_id_list)
        )

    transaction_to_show = query.order_by(
        desc(TransactionSCOAlertInternalDev.beginDate)
    ).limit(1).all()

    transaction_id = transaction_to_show[0].transactionId if transaction_to_show else None
    store_name = transaction_to_show[0].store_name if transaction_to_show else None
    store_id = transaction_to_show[0].store_id if transaction_to_show else None

    missed_items_details = db_xml_dev.query(
        TransactionSCOAlertInternalDev
    ).filter(
        TransactionSCOAlertInternalDev.transactionId == transaction_id
    ).all()

    item_details = db_xml_dev.query(
        TransactionDetailsSCOInternalDev
    ).filter(
        TransactionDetailsSCOInternalDev.TransactionID == transaction_id
    ).all()
    
    return transaction_id, item_details, missed_items_details, store_name, store_id


def update_transaction_entry_status(transaction_id, db_xml_dev):
    db_xml_dev.query(
        TransactionSCOAlertInternalDev
    ).filter(
        TransactionSCOAlertInternalDev.transactionId == transaction_id
    ).update({"Entrystatus": 1})

    result = db_xml_dev.commit()
    
    return result

    
def insert_transaction_to_dashboard_db(transaction_id, input_description, input_clubcard, actual_store_id, db, 
                                       db_xml_dev,nudge_type):
    transaction_result = db.query(Transactions).filter(Transactions.transaction_id == transaction_id).first()
    if transaction_result:
        return
    
    item_details = db_xml_dev.query(
        TransactionDetailsSCOInternalDev
    ).filter(
        TransactionDetailsSCOInternalDev.TransactionID == transaction_id
    ).all()

    missed_items_details = db_xml_dev.query(
        TransactionSCOAlertInternalDev
    ).filter(
        TransactionSCOAlertInternalDev.transactionId == transaction_id
    ).all()

    operator_number = item_details[0].operator_id if item_details else None
    operator_id = 55
    if operator_number:
        operator_result = db.query(Operators.id).filter(Operators.operator_id == operator_number).first()
        operator_id = operator_result.id if operator_result else 55

    date = missed_items_details[0].beginDate[:10]

    transaction_obj = Transactions(
        transaction_id=item_details[0].TransactionID if item_details else None,
        sequence_no=item_details[0].sequence_no if item_details else None,
        store_id=item_details[0].store_id if item_details else None,
        operator_id=operator_id,
        counter_no=item_details[0].counterno if item_details else None,
        source_id=2,
        extended_total_amount="",
        total_number_of_items=len(item_details),
        checked_items=len(item_details),
        missed_scan=0,
        over_scan=0,
        triggers=0,
        description=input_description if input_description else missed_items_details[0].type,
        clubcard=input_clubcard,
        bag_quantity=0,
        bag_price=0,
        first_item_at=5,
        hidden=1,
        highlighted=0,
        video_link=f"{transaction_id}.mp4",
        begin_date=min([item.BeginDateTime for item in item_details]).replace("T", " "),
        end_date=min([item.BeginDateTime for item in item_details]).replace("T", " "),
        final_status=0,
        staffcard="",
        transaction_key=item_details[0].transaction_key if item_details else None
    )

    transaction_item_obj_list = []
    for item in item_details:
        transaction_item_obj_list.append(
            Transaction_items(
                name=item.Name,
                transaction_id=item.TransactionID,
                transaction_type="Sale",
                pos_item_id="",
                item_id=item.ItemID,
                regular_sales_unit_price=item.RegularSalesUnitPrice,
                actual_sales_unit_price=item.ActualSalesUnitPrice,
                extended_amount=item.ExtendedAmount,
                quantity=item.Quantity,
                checked_quantity=item.Quantity,
                missed=0,
                overscan=0,
                trigger_id=0,
                scan_data=item.ScanData,
                begin_date_time=item.BeginDateTime.replace("T", " "),
                end_date_time=item.BeginDateTime.replace("T", " ")
            )
        )

    for missed_item in missed_items_details:
        transaction_item_obj_list.append(
            Transaction_items(
                name="Item",
                transaction_id=missed_item.transactionId,
                transaction_type="Sale",
                pos_item_id="0000",
                item_id="0000",
                regular_sales_unit_price=2,
                actual_sales_unit_price=2,
                extended_amount=2,
                quantity=1,
                checked_quantity=1,
                missed=1,
                overscan=0,
                trigger_id=0,
                scan_data="0000",
                begin_date_time=missed_item.beginDate.replace("T", " "),
                end_date_time=missed_item.beginDate.replace("T", " ")
            )
        )


    if nudge_type == "not_attended":
        transfer_status = transfer_video_in_s3(actual_store_id, date, transaction_id, "not_attended")
    else:
        transfer_status = transfer_video_in_s3(actual_store_id, date, transaction_id, "alerts")
    print("Video transfer status: ", transfer_status)

    db.add(transaction_obj)
    db.add_all(transaction_item_obj_list)
    db.commit()

    return True


def upload_transaction_skip_op(transaction_id, db_xml_dev):
    db_xml_dev.query(
        TransactionSCOAlertInternalDev
    ).filter(   
        TransactionSCOAlertInternalDev.transactionId == transaction_id
    ).update({"skip": True})
    
    db_xml_dev.commit()


def fetch_transaction_status_details(store_ids, start_time, end_time, db_xml_dev, nudge_type):
    query = db_xml_dev.query(
        StoresInternalDev.id,
        StoresInternalDev.name,
        func.count(distinct(TransactionSCOAlertInternalDev.transactionId)).label("entry_status_count"),
    ).join(
        StoresInternalDev, TransactionSCOAlertInternalDev.storeId == StoresInternalDev.id
    ).filter(
            TransactionSCOAlertInternalDev.Entrystatus == 1
    )

    if nudge_type == "not_attended":
        query = query.filter(
            TransactionSCOAlertInternalDev.type == "Not Attended"
        )
    else:
        query = query.filter(
            TransactionSCOAlertInternalDev.type != "Not Attended"
        )

    if store_ids:
        query = query.filter(
            TransactionSCOAlertInternalDev.storeId.in_(store_ids)
        )
    
    if start_time and end_time:
        # start_time = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
        # end_time = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")
        query = query.filter(
            func.str_to_date(TransactionSCOAlertInternalDev.beginDate, "%Y-%m-%d %H:%i:%s") >= start_time,
            func.str_to_date(TransactionSCOAlertInternalDev.beginDate, "%Y-%m-%d %H:%i:%s") <= end_time
        )
        
    entry_status_result = query.group_by(
        StoresInternalDev.id
    ).all()

    query = db_xml_dev.query(
        StoresInternalDev.id,
        StoresInternalDev.name,
        func.count(distinct(TransactionSCOAlertInternalDev.transactionId)).label("skip_count"),
    ).join(
        StoresInternalDev, TransactionSCOAlertInternalDev.storeId == StoresInternalDev.id
    ).filter(
            TransactionSCOAlertInternalDev.skip == True
    )

    if nudge_type == "not_attended":
        query = query.filter(
            TransactionSCOAlertInternalDev.type == "Not Attended"
        )
    else:
        query = query.filter(
            TransactionSCOAlertInternalDev.type != "Not Attended"
        )

    if store_ids:
        query = query.filter(
            TransactionSCOAlertInternalDev.storeId.in_(store_ids)
        )
        
    if start_time and end_time:
        query = query.filter(
            TransactionSCOAlertInternalDev.beginDate >= start_time,
            TransactionSCOAlertInternalDev.beginDate <= end_time
        )
        
    skip_result = query.group_by(
        StoresInternalDev.id
    ).all()

    return entry_status_result, skip_result
