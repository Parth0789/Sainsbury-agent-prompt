from sqlalchemy import func, and_, distinct, desc, or_, true, false, asc, extract, text
from model.model import Transactions, Stores, Transaction_items, Sources #, AppUsers, Outage,
from sqlalchemy.orm import Session, aliased
from datetime import datetime, timedelta
import requests
import json
from utils import current_date_time
from pprint import pprint

BASE_URL = "https://api.saivalentine.com/"
# def get_notification_details(db, store_id=None, region_id=None, area_id=None, start_time=None, end_time=None):
#
#     # try:
#     if start_time == None or start_time == "":
#         start_time, end_time = current_date_time()
#
#     url_ext = "get_notification_detail_new?"
#     if store_id:
#         url_ext = url_ext+"&store_id={}".format(store_id)
#     if start_time:
#         url_ext = url_ext+"&start_data={}".format(start_time)
#     if end_time:
#         url_ext = url_ext+"&end_date={}".format(end_time)
#     # print(BASE_URL+url_ext)
#     # "?store_id=1&start_time=2022-12-01&end_time=2022-12-31"
#     payload = {}
#     headers = {
#         'accept': 'application/json'
#     }
#     response = requests.post(BASE_URL+url_ext, headers=headers, data=payload, verify=False)
#     if response.status_code == 200:
#         response = response.json()
#     else:
#         response = {}
#         response["data"] = []
#
#     # pprint(response)
#     result = db.query(AppUsers, Stores.name).join(Stores, Stores.id == AppUsers.store_id).filter(
#         or_(AppUsers.store_id == store_id, true() if store_id is None else false()),
#         or_(Stores.region_id == region_id, true() if region_id is None else false()),
#         or_(Stores.area_id == area_id, true() if area_id is None else false())
#     ).all()
#     # print(result[0][0].__dict__)
#     result_dict = [dict(row[0].__dict__, store_name=row[1]) for row in result]
#     # print(result_dict)
#     output = []
#     for i in result_dict:
#         d = dict()
#         d["store_id"] = i["store_id"]
#         d["store_name"] = i["store_name"]
#         del i["store_id"]
#         del i["store_name"]
#         for key, val in zip(list(i.keys()),list(i.values())):
#             if val in response["data"]:
#                 val = dict(tabel_val = val,
#                 green_status = True)
#             else:
#                 val = dict(tabel_val=val,
#                            green_status=False)
#             d[key] = val
#         output.append(d)
#
#     # except Exception as e:
#     #     print(e)
#     #     return {"message": "Something went wrong"}, 400
#     return output

def get_user_data(user):
    """
    :param user: 
    :return: json user_status is 1 means active 0 means inactive
    """
    url_ext = "get_user_data"
    payload = json.dumps({
        "user": user
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", BASE_URL+url_ext, headers=headers, data=payload, verify=False)
    return response.json()

def update_user_password(details):
    url_ext = "update_user_password_new"
    payload = json.dumps({
        "user": details["user"],
        "user_password": details["user_password"]
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", BASE_URL + url_ext, headers=headers, data=payload, verify=False)
    return response.json()

def update_mobie_number(details):
    url_ext = "update_user_mobile_number_new"
    payload = json.dumps({
        "user": details["user"],
        "mobile_number": details["mobile_number"],
        "country_code": details["country_code"]
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", BASE_URL + url_ext, headers=headers, data=payload, verify=False)
    return response.json()

def update_user_status(details):
    url_ext = "update_user_status_new"
    payload = json.dumps({
        "user": details["user"],
        "user_status": details["user_status"]
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", BASE_URL + url_ext, headers=headers, data=payload, verify=False)
    return response.json()