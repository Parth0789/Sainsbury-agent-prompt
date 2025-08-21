from sqlalchemy import func, distinct, between, or_, true, false, case

from model.model import (
    Transactions, TransactionDetailsSco, Stores, Comments, OverallReportAggregatedResult, Operators
)


def get_overall_details_report(db, db_xml, start_time = None, end_time = None, store_id = None):
    query = db.query(
        Transactions.store_id, func.count(Transactions.id).label("nudge_count"), Transactions.clubcard
    ).filter(
        Transactions.triggers.in_([5, 6, 7]),
        Transactions.description != "",
        Transactions.hidden == 0,
        or_(Transactions.store_id == store_id, true() if not store_id else false())
    )

    if start_time and end_time:
        query = query.filter(
            between(func.date(Transactions.begin_date), start_time, end_time),
        )

    result_transaction = query.group_by(Transactions.store_id, Transactions.clubcard).all()

    query = db_xml.query(
        TransactionDetailsSco.store_id,
        func.count(distinct(TransactionDetailsSco.TransactionID)).label("total_transactions"),
        func.count(distinct(func.date(TransactionDetailsSco.BeginDateTime))).label("days")
    ).filter(
        TransactionDetailsSco.counterno != 99,
        or_(TransactionDetailsSco.store_id == store_id, true() if not store_id else false())
    )

    if start_time and end_time:
        query = query.filter(
            between(func.date(TransactionDetailsSco.BeginDateTime), start_time, end_time)
        )

    result_transaction_details_sco = query.group_by(TransactionDetailsSco.store_id).all()

    query = db.query(
        Transactions.store_id, Transactions.clubcard, func.count(Transactions.id).label("comment_count")
    ).join(
        Comments, Comments.transaction_id == Transactions.transaction_id
    ).where(
        Transactions.triggers.in_([5, 6, 7]),
        Transactions.description != "",
        Transactions.hidden == 0,
        or_(Transactions.store_id == store_id, true() if not store_id else false()),
        or_(Comments.sai_comments.like(f"Customer shopping%"), Comments.body.like(f"Customer shopping%"))
    )

    if start_time and end_time:
        query = query.filter(
            between(func.date(Transactions.begin_date), start_time, end_time),
        )

    result_transaction_comment = query.group_by(Transactions.store_id).all()

    return result_transaction, result_transaction_details_sco, result_transaction_comment


def get_overall_details_report_v2(db, db_xml, start_time = None, end_time = None, store_id = None, region_id = None, 
                                  zone = None):
    query = db.query(
        Transactions.store_id, func.count(Transactions.id).label("nudge_count"), Transactions.clubcard
    ).join(
        Stores, Stores.id == Transactions.store_id
    ).filter(
        Transactions.triggers.in_([5, 6, 7]),
        Transactions.description != "",
        Transactions.hidden == 0,
        or_(Transactions.store_id == store_id, true() if not store_id else false()),
        or_(Stores.company_region_id == region_id, true() if not region_id else false()),
        or_(Stores.zone == zone, true() if not zone else false())
    )

    if start_time and end_time:
        query = query.filter(
            between(func.date(Transactions.begin_date), start_time, end_time),
        )

    result_transaction = query.group_by(Transactions.store_id, Transactions.clubcard).all()

    query = db_xml.query(
        OverallReportAggregatedResult.store_id,
        func.count(distinct(OverallReportAggregatedResult.begin_date)).label("days_count"),
        func.sum(OverallReportAggregatedResult.transaction_count).label("total_transactions")
    ).filter(
        or_(OverallReportAggregatedResult.store_id == store_id, true() if not store_id else false())
    )

    if start_time and end_time:
        query = query.filter(
            between(OverallReportAggregatedResult.begin_date, start_time, end_time)
        )

    result_overall_report_sco = query.group_by(OverallReportAggregatedResult.store_id).all()

    query = db.query(
        Transactions.store_id, Transactions.clubcard, func.count(Transactions.id).label("comment_count")
    ).join(
        Comments, Comments.transaction_id == Transactions.transaction_id
    ).join(
        Stores, Stores.id == Transactions.store_id
    ).where(
        Transactions.triggers.in_([5, 6, 7]),
        Transactions.description != "",
        Transactions.hidden == 0,
        or_(Transactions.store_id == store_id, true() if not store_id else false()),
        or_(Stores.company_region_id == region_id, true() if not region_id else false()),
        or_(Comments.sai_comments.like(f"Customer shopping%"), Comments.body.like(f"Customer shopping%"))
    )

    if start_time and end_time:
        query = query.filter(
            between(func.date(Transactions.begin_date), start_time, end_time),
        )

    result_transaction_comment = query.group_by(Transactions.store_id).all()

    return result_transaction, result_overall_report_sco, result_transaction_comment


def get_overall_details_report_v3(db, db_xml, start_time = None, end_time = None, store_id = None):
    query = db.query(
        Transactions.store_id, func.count(Transactions.id).label("nudge_count"), Transactions.description,
        func.sum(Transactions.missed_item_count).label("missed_item_count")
    ).filter(
        Transactions.triggers.in_([5, 6, 7]),
        Transactions.description != "",
        Transactions.hidden == 0,
        or_(Transactions.store_id == store_id, true() if not store_id else false())
    )

    if start_time and end_time:
        query = query.filter(
            between(func.date(Transactions.begin_date), start_time, end_time),
        )

    result_transaction = query.group_by(Transactions.store_id, Transactions.description).all()

    query = db_xml.query(
        OverallReportAggregatedResult.store_id,
        func.count(distinct(OverallReportAggregatedResult.begin_date)).label("days_count"),
        func.sum(OverallReportAggregatedResult.transaction_count).label("total_transactions")
    ).filter(
        or_(OverallReportAggregatedResult.store_id == store_id, true() if not store_id else false())
    )

    if start_time and end_time:
        query = query.filter(
            between(OverallReportAggregatedResult.begin_date, start_time, end_time)
        )

    result_overall_report_sco = query.group_by(OverallReportAggregatedResult.store_id).all()


    return result_transaction, result_overall_report_sco


def fetch_operator_losses_data(db, store_id = None, start_time = None, end_time = None):
    query = db.query(
        Stores.id.label("sai_store_id"),
        Stores.name.label("store_name"),
        Stores.store_num.label("store_id"),
        case(
            ((Operators.operator_id.isnot(None), Operators.operator_id)),
            else_=""
        ).label("operator_id"),
        func.sum(
            case(
                ((Transactions.clubcard == "Rectified", 1)),
                else_=0
            )
        ).label("transactions_with_no_loss"),
        func.sum(
            case(
                ((Transactions.clubcard == "Non-Rectified", 1)),
                else_=0
            )
        ).label("transactions_with_loss")
    ).join(
        Stores, Stores.id == Transactions.store_id
    ).join(
        Operators, Transactions.operator_id == Operators.id, isouter=True
    ).filter(
        Transactions.clubcard.in_(["Rectified", "Non-Rectified"]),
        Transactions.description != "",
        Transactions.hidden == 0,
        Transactions.triggers == 5,
        Stores.store_running == 1
    )

    if store_id:
        query = query.filter(Stores.id == store_id)
    if start_time and end_time:
        query = query.filter(
            func.date(Transactions.begin_date) >= start_time,
            func.date(Transactions.begin_date) <= end_time
        )

    result = query.group_by(Transactions.store_id, Transactions.operator_id).all()

    return result
