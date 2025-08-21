import requests
import json
from dateutil import parser
from sqlalchemy.orm import Session

from model.model import (
    TransactionSCO, Transaction_Main, TransactionDetailsSco, Transaction_Details_Main, Stores, Transactions,
    Transaction_items, Operators
)


def update_transactions_table(db2, db, sco, transaction_item_sco, transaction_id, counter_type):
    store_id = transaction_item_sco.store_id
    o_id = transaction_item_sco.operator_id
    if o_id == "":
        operator_id = 150
    else:
        operator_id = db.query(
            Operators
        ).filter(
            Operators.operator_id == o_id
        ).first()

        if operator_id:
            operator_id = operator_id.id
        else:
            o_val = Operators(operator_id=o_id, store_id=store_id, name="SAINS")
            db.add(o_val)
            db.commit()
            operator_id = o_val.id
    
    data = dict(
        transaction_id=sco.transactionId,
        sequence_no=transaction_item_sco.sequence_no,
        store_id=sco.storeId,
        counter_no=sco.counterNo,
        source_id=counter_type,
        operator_id=operator_id,
        description="",
        begin_date=sco.beginDate,
        end_date=sco.endDate,
        staffcard="",
        missed_scan=1,
        video_link=str(transaction_id) + ".mp4",
        extended_total_amount=sco.extendedTotalAmount,
        total_number_of_items=sco.totalNumberOfItems
    )
    transaction = Transactions(**data)
    db.add(transaction)

    transaction_items = []
    if counter_type == 2:
        transaction_items = db2.query(
            TransactionDetailsSco
        ).filter(
            TransactionDetailsSco.TransactionID == transaction_id
        ).all()
    elif counter_type == 1:
        transaction_items = db2.query(
            Transaction_Details_Main
        ).filter(
            Transaction_Details_Main.TransactionID == transaction_id
        ).all()

    transaction_items_data = [
        {
            "name": val.Name,
            "transaction_id": val.TransactionID,
            "transaction_type": val.transaction_type,
            "pos_item_id": val.POSItemID,
            "item_id": val.ItemID,
            "regular_sales_unit_price": val.RegularSalesUnitPrice,
            "actual_sales_unit_price": val.ActualSalesUnitPrice,
            "extended_amount": val.ExtendedAmount,
            "quantity": val.Quantity,
            "checked_quantity": val.Quantity,
            "missed": 0,
            "overscan": 0,
            "trigger_id": 0,
            "scan_data": val.ScanData,
            "begin_date_time": val.BeginDateTime,
            "end_date_time": val.EndDateTime if val.EndDateTime else val.BeginDateTime
        }
        for val in transaction_items
    ]

    transactions_items_data_insert = [Transaction_items(**row) for row in transaction_items_data]
    db.add_all(transactions_items_data_insert)
    db.commit()

    return data, transaction_items_data


def move_data_in_s3(date, store_actual, count, video_name):
    url = "https://k9ga2mekwk.execute-api.eu-west-2.amazonaws.com/s1/copy_file"
    headers = {"Content-Type": "application/json"}

    parsed_date = parser.parse(date)
    begin_date = parsed_date.date()

    data = {
        "date": str(begin_date),
        "store_id": store_actual,
        "count": count,
        "bucket_name": "sainsbury-zip-bucket",
        "video_name": video_name
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))


def upload_transaction(db2: Session, db: Session, transaction_id, counter_type):
    is_exists = db.query(
        Transactions
    ).filter(
        Transactions.transaction_id == transaction_id,
        Transactions.source_id == counter_type
    ).first()

    if is_exists:
        beginDate = is_exists.begin_date
        count = is_exists.total_number_of_items
        video_name = transaction_id + ".mp4"

        store_actual_id = db.query(
            Stores.store_actual_id
        ).filter(
            Stores.id == is_exists.store_id
        ).first()

        move_data_in_s3(beginDate, store_actual_id[0], count, video_name)
        return {"message": "Transaction already exists"}

    if counter_type == 2:
        sco = db2.query(
            TransactionSCO
        ).filter(
            TransactionSCO.transactionId == transaction_id
        ).first()

        if sco:
            beginDate = sco.beginDate
            count = sco.totalNumberOfItems
            video_name = transaction_id + ".mp4"

            store_actual_id = db.query(
                Stores.store_actual_id
            ).filter(
                Stores.id == sco.storeId
            ).first()
            # move_data_in_s3(beginDate, store_actual_id[0], count, video_name)

        transaction_item_sco = db2.query(TransactionDetailsSco.store_id, TransactionDetailsSco.operator_id,
                                         TransactionDetailsSco.sequence_no).filter(
            TransactionDetailsSco.TransactionID == transaction_id).first()
        print(sco)
        data = update_transactions_table(db2, db, sco, transaction_item_sco, transaction_id, counter_type)

        return data
    elif counter_type == 1:
        main = db2.query(
            Transaction_Main
        ).filter(
            Transaction_Main.transactionId == transaction_id
        ).first()

        if main:
            beginDate = main.beginDate
            count = main.totalNumberOfItems
            video_name = transaction_id + ".mp4"
            store_actual_id = db2.query(Stores.store_actual_id).filter(Stores.id == main.storeId).first()
            move_data_in_s3(beginDate, store_actual_id[0], count, video_name)

        transaction_item_sco = db2.query(
            Transaction_Details_Main
        ).filter(
            Transaction_Details_Main.TransactionID == transaction_id
        ).first()

        data = update_transactions_table(db2, db, main, transaction_item_sco, transaction_id, counter_type)
        return data

def update_transaction_and_items(details, transaction_id, db):
    transaction = details["transaction_details"]
    all_transaction_items = details["transaction_items"]
    db.query(
        Transactions
    ).filter(
        Transactions.transaction_id == transaction_id
    ).update(
        {
            "description": transaction["description"],
             "checked_items": transaction["checked_items"],
             "bag_quantity": transaction["bag_quantity"],
             "bag_price": transaction["bag_price"],
             "first_item_at": transaction["first_item_at"],
             "hidden": transaction["hidden"],
             "highlighted": transaction["highlighted"]
        }
    )

    db.commit()

    for transaction_items in all_transaction_items:
        if transaction_items.get("db_id", None):
            db.query(
                Transaction_items
            ).filter(
                Transaction_items.id == transaction_items["db_id"]
            ).update(
                {
                    "name": transaction_items["name"],
                     "quantity": transaction_items["quantity"],
                     "begin_date_time": transaction_items["begin_date_time"],
                     "end_date_time": transaction_items["begin_date_time"],
                     "transaction_type": transaction_items["transaction_type"],
                     "regular_sales_unit_price": transaction_items["regular_sales_unit_price"],
                    "actual_sales_unit_price": transaction_items["regular_sales_unit_price"],
                     "missed": transaction_items["missed"],
                     "overscan": 0,
                     "extended_amount": (eval(transaction_items["regular_sales_unit_price"]) *
                                         eval(transaction_items["quantity"])),
                }
            )
            db.commit()
        else:
            val = {
                "name": transaction_items["name"],
                "transaction_id": transaction_id,
                "quantity": transaction_items["quantity"],
                "begin_date_time": transaction_items["begin_date_time"],
                "end_date_time": transaction_items["begin_date_time"],
                "transaction_type": transaction_items["transaction_type"] if "transaction_type" in transaction_items else None,
                "regular_sales_unit_price": transaction_items["regular_sales_unit_price"],
                "actual_sales_unit_price": transaction_items["regular_sales_unit_price"],
                "missed": transaction_items['missed'],
                "overscan": 0,
                "extended_amount": (eval(transaction_items["regular_sales_unit_price"]) *
                                   eval(transaction_items["quantity"])),
            }
            transac_items = Transaction_items(**val)
            db.add(transac_items)
            db.commit()

    return {"message": "Done"}
