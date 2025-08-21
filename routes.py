import base64

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic.fields import defaultdict

from crud.tender import (
    get_data_by_clubcard, get_clubcard_detail, get_data_by_clubcard_trigger, get_clubcard_detail_trigger,
    get_clubcard_detail_trigger_new, get_clubcard_detail_trigger_to_update
)
from sqlalchemy.orm import Session
from datetime import date
from dateutil.relativedelta import relativedelta
import base64
import pandas as pd
pd.set_option('display.max_columns', None)
from fastapi.responses import StreamingResponse
from io import BytesIO

from route.reports import HYPERCARE_STORES
from database import get_db, get_xml_db
from serializers.request.comments import UpdateCommentRequestModel
from serializers.request.user import UserLoginRequestModel
from serializers.request.clubcard import UpdateTransactionClubcard
from crud.sco_main import (get_count_sco_main,
                           get_drop_down_info_stores,
                            get_drop_down_info_stores_v2,
                           get_top5_store_area,
                           get_top5_store_region,
                           get_main_count_data,
                           get_count_sco_main_by_month,
                           # get_sco_main_active_status,
                           performance_comparison,
                           get_transaction_data,
                           get_drop_down_info_region,
                           get_drop_down_info_area,
                           get_top5_main_theft_data,
                           get_top5_theft_sco_res,
                           get_top5_theft_main_res,
                           get_top5_employee_performance_res,
                           add_comment_of_transection_id,
                            update_comment_of_comments_id,
                           all_region, all_area,
                           report_top10_sco_main,
                           list_of_sco_main_theft,
                           get_comment_transaction_id,
                           update_video_error_flag,
                           total_scan_error_on_dashboard,
                           total_scan_main_sco_error_on_dashboard,
                           total_scan_error_on_potential_fraud,
                           total_scan_error_on_confirmed_fraud,
                           total_scan_error_on_viewed_no_action,
                           tracker,
                           link_ceration,
                           link_gen_data,
                           incorrect_items,
                           staff_card,
                           sydenham_self_scanning,
                           sco_main_report_count,
                           get_tobacco,
                            add_user_view,
                            get_user_view,
                           # get_sco_main_stats_for_month,
                            fetch_main_bank_detail
                        )
from crud.aisle import (get_count_aisle,
                        get_top5_aisle_store_region,
                        get_top5_aisle_store_area,
                        get_count_aisle_stats,
                        get_top3_theft_current_month_data,
                        get_top3_intervention_current_month_data,
                        get_aisle_count_data,
                        get_ailse_transaction_data_by_id,
                        report_top10_aisle,
                        report_top10_intervention,
                        list_of_aisle_theft,
                        active_stores
                        )

from crud.search import search_by_id_res, fetch_last_update_time
from crud.mobile import (#get_notification_details,
                         get_user_data,
                         update_user_password,
                         update_mobie_number,
                         update_user_status)
from crud.transactions import (fetch_missed_items_report, fetch_missed_items_report_download, fetch_next_missed_item,
                               update_clubcard_value)
from crud.logout import logout_user
from utils import set_permissions, get_last_month_from_current_date
from crud.login import AuthHandler, user_validate
# from crud.store import (get_issue_category,
#                         store_issue_data,
#                         store_map_data,
#                         add_issue_comment)
from secure_payload import return_encoded_data
from utils.general import get_serialized_object

router = APIRouter()
auth_handler = AuthHandler()

@router.get("/health_check", status_code=status.HTTP_200_OK)
def health_check():
    return {"message": "ok"}


@router.get("/get_count_sco_main", status_code=status.HTTP_200_OK)
def get_count_sco_main_(db: Session = Depends(get_db), store_id: int = None, region: int = None, area: int = None,
                        start_time: str = None, end_time: str = None, token_data=Depends(auth_handler.auth_wrapper)):
    return get_count_sco_main(db,store_id,region,area,start_time,end_time)


@router.get("/get_count_aisle", status_code=status.HTTP_200_OK)
def get_count_aisle_(db: Session = Depends(get_db), store_id: int = None, region:int = None, area: int = None,
                     start_time: str = None, end_time: str = None, token_data=Depends(auth_handler.auth_wrapper)):
    return get_count_aisle(db,store_id,region,area,start_time,end_time)


@router.get("/get_dropdown_info_stores", status_code=status.HTTP_200_OK)
def get_one_post(db: Session = Depends(get_db), region_id: int = None, token_data=Depends(auth_handler.auth_wrapper)):
    data = get_drop_down_info_stores(db=db, region_id=region_id)
    data["store_data"] = get_serialized_object(data["store_data"])
    return return_encoded_data(data)


@router.get("/get_dropdown_info_stores_v2", status_code=status.HTTP_200_OK)
def get_dropdown_info_stores_v2(club_card: bool = False, db: Session = Depends(get_db), region_id: int = None,
                                client_region_id: int = None, token_data=Depends(auth_handler.auth_wrapper)):
    data = get_drop_down_info_stores_v2(db=db, region_id=region_id, client_region_id=client_region_id)
    data["store_data"] = get_serialized_object(data["store_data"])
    return return_encoded_data(data)


@router.get("/get_dropdown_info_region", status_code=status.HTTP_200_OK)
def get_one_post(db: Session = Depends(get_db), store_name: str = None, store_id: int = None,
                 token_data=Depends(auth_handler.auth_wrapper)):
    return get_drop_down_info_region(db, store_name, store_id)


@router.get("/get_dropdown_info_area", status_code=status.HTTP_200_OK)
def get_area(db: Session = Depends(get_db), store_name:str=None, region:str = None,
             token_data=Depends(auth_handler.auth_wrapper)):
    return get_drop_down_info_area(db=db, store_name=store_name, region=region)


@router.get("/get_top5_store_by_area", status_code=status.HTTP_200_OK)
def get_one_post(db: Session = Depends(get_db), area: int = None, start_time: str = None, end_time: str = None,
                 token_data=Depends(auth_handler.auth_wrapper)):
    return get_top5_store_area(db=db, area_id=area, start_time=start_time, end_time=end_time)


@router.get("/get_top5_store_by_region", status_code=status.HTTP_200_OK)
def get_one_post(db: Session = Depends(get_db), region: int = None, start_time: str = None, end_time: str = None,
                 token_data=Depends(auth_handler.auth_wrapper)):
    return get_top5_store_region(db=db,region_id=region, start_time=start_time, end_time=end_time)


@router.get("/get_sco_main_data", status_code=status.HTTP_200_OK)
def get_count_sco_main_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None, area_id: int = None,
                        type: int = None, start_time: str = None, end_time: str = None, page: int = 1,
                        token_data=Depends(auth_handler.auth_wrapper)):
    """
    :param db:
    :param store_id:
    :param region_id:
    :param area_id:
    :param type:
    :param start_time:
    :param end_time:
    :param page:
    :param seen_status: (0, 1) 0--> which is not seen yet and 1 --> which seens
    :return:
    """
    return get_main_count_data(db, store_id, region_id, area_id, type, start_time, end_time, page)


@router.get("/get_transaction_data", status_code=status.HTTP_200_OK)
def get_transaction_data_by_ID(db: Session = Depends(get_db), transaction_id: str = None):
    data = get_transaction_data(db, transaction_id)
    
    data["info"] = get_serialized_object(data["info"])
    data["items"] = get_serialized_object(data["items"])
    return return_encoded_data(data)


@router.get("/get_shared_transaction_data", status_code=status.HTTP_200_OK)
def get_shared_transaction_data_by_ID(db: Session = Depends(get_db), transaction_id: str = None):
    transaction_id_bytes = transaction_id.encode("ascii")
    transaction_id_bytes = base64.b64decode(transaction_id_bytes)
    transaction_id = transaction_id_bytes.decode("ascii")
    data = get_transaction_data(db, transaction_id)
    data["info"] = get_serialized_object(data["info"])
    data["items"] = get_serialized_object(data["items"])
    return return_encoded_data(data)


@router.get("/get_sco_main_stats", status_code=status.HTTP_200_OK)
def get_count_sco_main_by_month_(db: Session = Depends(get_db), store_id: int = None, region: int = None,
                                 area: int = None, start_time: str = None, end_time: str = None,
                                 token_data=Depends(auth_handler.auth_wrapper)):
    return get_count_sco_main_by_month(db,store_id,region,area,start_time,end_time)


@router.get("/get_top5_theft_sco", status_code=status.HTTP_200_OK)
def get_top5_theft_sco(db: Session=Depends(get_db), start_time: str = None, end_time: str = None,
                       token_data=Depends(auth_handler.auth_wrapper)):
    return get_top5_theft_sco_res(db, start_time,end_time)


@router.get("/get_top5_theft_main", status_code=status.HTTP_200_OK)
def get_top5_theft_main(db: Session=Depends(get_db), start_time: str = None, end_time: str = None,
                        token_data=Depends(auth_handler.auth_wrapper)):
    return get_top5_theft_main_res(db, start_time,end_time)


@router.get("/get_top5_employee_performance", status_code=status.HTTP_200_OK)
def get_top5_employee_performance(db: Session=Depends(get_db), store_id: int = None, region_id: int = None,
                                  area_id: int = None, start_time: str = None, end_time: str = None,
                                  token_data=Depends(auth_handler.auth_wrapper)):
    return get_top5_employee_performance_res(db, store_id, region_id, area_id, start_time, end_time)


@router.post("/add_comment_of_transection_id", status_code=status.HTTP_200_OK)
def add_comment_of_transection_id_(details:dict, db: Session = Depends(get_db),
                                   token_data=Depends(auth_handler.auth_wrapper)):
    add_comment_of_transection_id(db, details)
    return details


@router.put("/update_comment_of_transection_id", status_code=status.HTTP_200_OK)
def update_comment_of_transection_id_(details: UpdateCommentRequestModel, db: Session = Depends(get_db),
                                      token_data=Depends(auth_handler.auth_wrapper)):
    details = details.dict()
    return update_comment_of_comments_id(db, details)


#Aisle
@router.get("/get_top5_aisle_store_by_region", status_code=status.HTTP_200_OK)
def get_top5_aisle_store_region_data(db: Session = Depends(get_db), region: int = None, start_time: str = None,
                                     end_time: str = None, token_data=Depends(auth_handler.auth_wrapper)):
    return get_top5_aisle_store_region(db,region, start_time, end_time)


@router.get("/get_top5_aisle_store_by_area", status_code=status.HTTP_200_OK)
def get_top5_aisle_store_area_data(db: Session = Depends(get_db), area: int = None, start_time: str = None,
                                   end_time: str = None, token_data=Depends(auth_handler.auth_wrapper)):
    return get_top5_aisle_store_area(db,area, start_time, end_time)


# @router.get("/get_sco_main_active_status", status_code=status.HTTP_200_OK)
# def get_sco_main_active_status_data(db: Session = Depends(get_db),store_id:int=None):
#     return get_sco_main_active_status(db,store_id)


@router.get("/get_sco_main_performance_comparison", status_code=status.HTTP_200_OK)
def get_sco_main_performance_comparison_data(db: Session = Depends(get_db), store_id: int = None,
                                             start_time: str = None, end_time: str = None,
                                             token_data=Depends(auth_handler.auth_wrapper)):
    return performance_comparison(db,store_id,start_time, end_time)


# @router.get("/get_sco_main_stats_for_month", status_code=status.HTTP_200_OK)
# def get_test(db:Session=Depends(get_db), store_id:int=None, start_date:str=None, end_date:str=None):
#     return get_sco_main_stats_for_month(db, store_id, start_date, end_date)

@router.get("/get_count_aisle_stats", status_code=status.HTTP_200_OK)
def get_count_aisle_stats_data(db: Session = Depends(get_db), store_id: int = None, region: int = None,
                               area: int = None, start_time: str = None, end_time: str = None,
                               token_data=Depends(auth_handler.auth_wrapper)):
    return get_count_aisle_stats(db, store_id, region, area, start_time, end_time)


@router.get("/get_top5_sco_main_theft", status_code=status.HTTP_200_OK)
def get_top5_main_theft_fun(db: Session=Depends(get_db), store_id: str = None, region_id: str = None,
                            area_id: str = None, start_time: str = None, end_time: str = None,
                            token_data=Depends(auth_handler.auth_wrapper)):
    return get_top5_main_theft_data(db, store_id, region_id, area_id, start_time, end_time)


@router.get("/get_top3_aisle_theft_current_month", status_code=status.HTTP_200_OK)
def get_top3_theft_current_month(db: Session=Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    return get_top3_theft_current_month_data(db)


@router.get("/get_top3_aisle_intervention_current_month", status_code=status.HTTP_200_OK)
def get_top3_intervention_current_month(db:Session=Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    return get_top3_intervention_current_month_data(db)


@router.get("/get_aisle_intervention_data", status_code=status.HTTP_200_OK)
def get_count_aisle_intervention_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                                  area_id: int = None, is_intervention: int = 0, start_time: str = None,
                                  end_time: str = None, page: int = 1, seen_status: int = 0,
                                  token_data=Depends(auth_handler.auth_wrapper)):
    """
    :param db:
    :param store_id:
    :param region_id:
    :param area_id:
    :param is_intervention:
    :param start_time:
    :param end_time:
    :param page:
    :param seen_status: (0, 1) 0--> which is not seen yet and 1 --> which seens
    :return:
    """
    return get_aisle_count_data(db, store_id, region_id, area_id, is_intervention, start_time, end_time, page, seen_status)


# Search API
@router.get("/search_by_id", status_code=status.HTTP_200_OK)
def search_by_id(search_query: str=None, db: Session = Depends(get_db), page: int = 1, store_id: int = None,
                       region_id: int = None, area_id: int = None, sortField: str = None, sortOrder: str = "DESC",
                       token_data=Depends(auth_handler.auth_wrapper)):
    data, count = search_by_id_res(db, search_query, page, store_id, region_id, area_id, sortField, sortOrder)
    data = get_serialized_object(data)
    return return_encoded_data([data, count])

#App Notifications Details
# @router.get("/get_notification_details")
# def get_notification_details_(db: Session = Depends(get_db), store_id:int=None, region_id:int=None, area_id:int=None, start_time:str=None,end_time:str=None):
#     return get_notification_details(db, store_id, region_id, area_id, start_time, end_time)

@router.post("/get_user_data", status_code=status.HTTP_200_OK)
def get_user_data_(details:dict, token_data=Depends(auth_handler.auth_wrapper)):
    return get_user_data(details["user"])


@router.post("/update_user_password")
def update_user_password_(details: dict, token_data=Depends(auth_handler.auth_wrapper)):
    return update_user_password(details)


@router.post("/update_mobile_no")
def update_mobie_number_(details: dict, token_data=Depends(auth_handler.auth_wrapper)):
    return update_mobie_number(details)


@router.post("/update_user_status")
def update_user_status_(details:dict, token_data=Depends(auth_handler.auth_wrapper)):
    return update_user_status(details)

# Store Issue
# @router.get("/get_issue_category")
# def issue_category_(db: Session = Depends(get_db)):
#     return get_issue_category(db)

# @router.get("/get_store_issue_data")
# def store_issue_data_(db: Session = Depends(get_db), store_id:int=None, region_id:int=None, area_id:int=None, category_id:int=None, page:int=1):
#     return store_issue_data(db, store_id, region_id, area_id, category_id, page)
#
# @router.get("/get_store_map_data")
# def store_map_data_(db: Session = Depends(get_db), store_id:int=None, region_id:int=None, area_id:int=None):
#     return store_map_data(db, store_id, region_id, area_id)

@router.get("/get_all_region")
def all_region_(db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    data = all_region(db)
    print(data)
    return return_encoded_data(all_region(db))


@router.get("/get_all_area")
def all_area_(db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    return all_area(db)

# @router.post("/add_comment_of_store_issue",status_code=status.HTTP_200_OK)
# def add_issue_comment_(details:dict, db: Session = Depends(get_db)):
#     return add_issue_comment(db, details)


@router.post("/get_ailse_transaction_data", status_code=status.HTTP_200_OK)
def get_ailse_transaction_data_by_id_(details:dict, db: Session = Depends(get_db),
                                      token_data=Depends(auth_handler.auth_wrapper)):
    return get_ailse_transaction_data_by_id(details, db)


@router.get("/report_top10_sco_main", status_code=status.HTTP_200_OK)
def report_top10_sco_main_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                           area_id: int = None, start_time: str = None, end_time: str = None,
                           token_data=Depends(auth_handler.auth_wrapper)):
    return report_top10_sco_main(db, store_id, region_id, area_id, start_time, end_time)


@router.get("/report_top10_aisle", status_code=status.HTTP_200_OK)
def report_top10_aisle_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None, area_id: int = None,
                        start_time: str = None, end_time: str = None, token_data=Depends(auth_handler.auth_wrapper)):
    return report_top10_aisle(db, store_id, region_id, area_id, start_time, end_time)\


@router.get("/report_top10_intervention", status_code=status.HTTP_200_OK)
def report_top10_intervention_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                               area_id: int = None, start_time: str = None,end_time: str = None,
                               token_data=Depends(auth_handler.auth_wrapper)):
    return report_top10_intervention(db, store_id, region_id, area_id, start_time, end_time)


@router.get("/report_list_of_aisle_theft", status_code=status.HTTP_200_OK)
def list_of_aisle_theft_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                         area_id: int = None, start_time: str = None, end_time: str = None,
                         token_data=Depends(auth_handler.auth_wrapper)):
    return list_of_aisle_theft(db, store_id, region_id, area_id, start_time, end_time)


@router.get("/report_list_of_sco_main_theft", status_code=status.HTTP_200_OK)
def list_of_sco_main_theft_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                            area_id: int = None, start_time: str = None, end_time: str = None,
                            token_data=Depends(auth_handler.auth_wrapper)):
    return list_of_sco_main_theft(db, store_id, region_id, area_id, start_time, end_time)


# from crud.login import AuthHandler, user_validate
# from schemas import AuthDetails
# auth_handler = AuthHandler()
# @router.post("/login")
# def login_(details:dict, db:Session=Depends(get_db)):
#     is_valid = user_validate(details, db)
#     if is_valid:
#         hash_pwd = is_valid.password
#         if auth_handler.verify_password(details["password"], hash_pwd):
#             token = {"access_token": auth_handler.access_encode_token(details["email"]),
#                      "refresh_token": auth_handler.refresh_encode_token(details["email"])
#                      }
#             return token
#         else:
#             return {"message":"password is not valid"}
#     else:
#         return HTTPException(status_code=404, detail="User not found")

@router.post("/login")
def login_(details: UserLoginRequestModel, db:Session=Depends(get_db)):
    details = dict(details)
    is_valid = user_validate(details, db)
    if is_valid:
        hash_pwd = is_valid.password
        if auth_handler.verify_password(details["password"], hash_pwd):
            token = {"access_token": auth_handler.access_encode_token(details["email"], is_valid.roles),
                     "refresh_token": auth_handler.refresh_encode_token(details["email"], is_valid.roles)
                     }
            roles = set_permissions(is_valid.roles)
            return {"tokens":token, "roles":roles, "email":details["email"]}
        else:
            return {"message":"password is not valid"}
    else:
        return HTTPException(status_code=404, detail="User not found")


@router.post("/logout", status_code=status.HTTP_200_OK)
def logout_(db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    if logout_user(db, token_data):

        return JSONResponse(status_code=200, content={"message": "Successfully logged out!"})
    else:
        return HTTPException(status_code=404, detail="No data found !")


@router.get("/active_stores", status_code=status.HTTP_200_OK)
def active_stores_(db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    return active_stores(db)


@router.post("/get_comment_transaction_id", status_code=status.HTTP_200_OK)
def get_comment_(details: dict, db: Session = Depends(get_db)):
    data = get_comment_transaction_id(details, db)
    res = []
    for record in data:
        record = dict(record)
        record["created_at"] = record["created_at"].strftime("%Y-%m-%d %H:%M:%S") if record["created_at"] else None
        res.append(record)
    # data = get_serialized_object(data)
    return return_encoded_data(res)


@router.post("/update_video_error_flag", status_code=status.HTTP_200_OK)
def update_video_error_flag_(details: dict, db: Session = Depends(get_db),
                             token_data=Depends(auth_handler.auth_wrapper)):
    return update_video_error_flag(details, db)


@router.get("/total_scan_error_on_dashboard", status_code=status.HTTP_200_OK)
def total_scan_error_on_dashboard_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                                   area_id: int = None, start_time: str = None, end_time: str = None, page: int = 1,
                                   sortField: str = "begin_date", sortOrder: str = "desc",
                                   token_data=Depends(auth_handler.auth_wrapper)):
    return total_scan_error_on_dashboard(db, store_id, region_id, area_id, start_time, end_time, page, sortField, sortOrder)


@router.get("/total_scan_main_sco_error_on_dashboard", status_code=status.HTTP_200_OK)
def total_scan_main_sco_error_on_dashboard_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                                            area_id: int = None, type: int = None, start_time: str = None,
                                            end_time: str = None, page: int = 1, sortField: str = "begin_date",
                                            sortOrder: str = "desc", token_data=Depends(auth_handler.auth_wrapper)):
    return total_scan_main_sco_error_on_dashboard(db, store_id, region_id, area_id, type, start_time, end_time, page, sortField, sortOrder)


@router.get("/total_scan_error_on_potential_fraud", status_code=status.HTTP_200_OK)
def total_scan_error_on_potential_fraud_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                                         area_id: int = None, start_time: str = None, end_time: str = None, page: int = 1,
                                         sortField: str = "begin_date", sortOrder: str = "desc",
                                         token_data=Depends(auth_handler.auth_wrapper)):
    return total_scan_error_on_potential_fraud(db, store_id, region_id, area_id, start_time, end_time, page, sortField, sortOrder)


@router.get("/total_scan_error_on_confirmed_fraud", status_code=status.HTTP_200_OK)
def total_scan_error_on_confirmed_fraud_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                                   area_id: int = None, start_time: str = None, end_time: str = None, page: int = 1,
                                   sortField: str = "begin_date", sortOrder: str = "desc",
                                         token_data=Depends(auth_handler.auth_wrapper)):
    return total_scan_error_on_confirmed_fraud(db, store_id, region_id, area_id, start_time, end_time, page, sortField, sortOrder)


@router.get("/total_scan_error_on_viewed_no_action", status_code=status.HTTP_200_OK)
def total_scan_error_on_viewed_no_action_(db:Session=Depends(get_db), store_id:int=None, region_id:int=None,
                                   area_id:int=None,
                                   start_time:str=None,end_time:str=None, page:int=1,
                                   sortField: str = "begin_date",
                                   sortOrder: str = "desc", token_data=Depends(auth_handler.auth_wrapper)
                                          ):
    return total_scan_error_on_viewed_no_action(db, store_id, region_id, area_id, start_time, end_time, page, sortField, sortOrder)


@router.get("/tracker", status_code=status.HTTP_200_OK)
def tracker_(db:Session=Depends(get_db), store_id:int=None, region_id:int=None,
                                   area_id:int=None,
                                   start_time:str=None,end_time:str=None, page:int=1,
             token_data=Depends(auth_handler.auth_wrapper)):
    return tracker(db, store_id, region_id, area_id, start_time, end_time, page)


@router.get("/link_creation", status_code=status.HTTP_200_OK)
def link_ceration_(db:Session=Depends(get_db), transaction_id:str=None, token_data=Depends(auth_handler.auth_wrapper)):
    return link_ceration(db, transaction_id)


@router.get("/link_genrated_data", status_code=status.HTTP_200_OK)
def link_gen_data_(db:Session=Depends(get_db), transaction_id:str=None):
    return link_gen_data(db, transaction_id)


@router.get("/shared_link_genrated_data", status_code=status.HTTP_200_OK)
def link_gen_data_(db:Session=Depends(get_db), transaction_id:str=None):
    transaction_id_bytes = transaction_id.encode("ascii")
    transaction_id_bytes = base64.b64decode(transaction_id_bytes)
    transaction_id = transaction_id_bytes.decode("ascii")
    return link_gen_data(db, transaction_id)

@router.get("/shared_link_genrated_data_v2", status_code=status.HTTP_200_OK)
def link_gen_data_(db:Session=Depends(get_db), transaction_id:str=None):
    transaction_id_bytes = transaction_id.encode("ascii")
    transaction_id_bytes = base64.b64decode(transaction_id_bytes)
    transaction_id = transaction_id_bytes.decode("ascii")
    result = link_gen_data(db, transaction_id)
    result = dict(result)
    video_link = result["video_link"]
    result["video_link1"] = None
    if video_link:
        result["video_link1"] = create_presigned_url(f"missed_videos/{video_link}")

    return return_encoded_data(result)


@router.get("/incorrect_items", status_code=status.HTTP_200_OK)
def incorrect_items_(db:Session=Depends(get_db), store_id:int=None, region_id:int=None,
                                   area_id:int=None,
                                   start_time:str=None,end_time:str=None, page:int=1,
                                   sortField: str = "begin_date",
                                   sortOrder: str = "desc", token_data=Depends(auth_handler.auth_wrapper)):
    return incorrect_items(db, store_id, region_id, area_id, start_time, end_time, page, sortField, sortOrder)


@router.get("/staff_card", status_code=status.HTTP_200_OK)
def staff_card_(db:Session=Depends(get_db), store_id:int=None, region_id:int=None,
                                   area_id:int=None,
                                   start_time:str=None,end_time:str=None, page:int=1,
                                   sortField: str = "begin_date",
                                   sortOrder: str = "desc", token_data=Depends(auth_handler.auth_wrapper)):
    return staff_card(db, store_id, region_id, area_id, start_time, end_time, page, sortField, sortOrder)


@router.get("/sydenham_self_scanning", status_code=status.HTTP_200_OK)
def sydenham_self_scanning_(db:Session=Depends(get_db), store_id:int=None, region_id:int=None,
                                   area_id:int=None,
                                   start_time:str=None,end_time:str=None, page:int=1,
                                   sortField: str = "begin_date",
                                   sortOrder: str = "desc", token_data=Depends(auth_handler.auth_wrapper)):
    return sydenham_self_scanning(db, store_id, region_id, area_id, start_time, end_time, page, sortField, sortOrder)


@router.get("/sco_main_report_count", status_code=status.HTTP_200_OK)
def sco_main_report_count_(db:Session=Depends(get_db), store_id:int=None, region_id:int=None,
                                   area_id:int=None,
                                   start_time:str=None,end_time:str=None,
                           token_data=Depends(auth_handler.auth_wrapper)):
    return sco_main_report_count(db, store_id, region_id, area_id, start_time, end_time)


@router.get("/saml_login", status_code=status.HTTP_200_OK)
def saml_login_(db:Session=Depends(get_db), url:str=None):
    if url == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    try:
        details = {"email": base64.b64decode(url).decode()}
        is_valid = user_validate(details, db)
        if is_valid:
            token = {"access_token": auth_handler.access_encode_token(details["email"], is_valid.roles),
                    "refresh_token": auth_handler.refresh_encode_token(details["email"], is_valid.roles)
                    }
            roles = set_permissions(is_valid.roles)
            return {"tokens": token, "roles": roles, "email": details["email"]}
        return HTTPException(status_code=404, detail="User not found")
    except:
        return HTTPException(status_code=404, detail="User not found")


@router.get("/get_tobacco", status_code=status.HTTP_200_OK)
def get_tobacco_(db:Session=Depends(get_db), store_id:int=None, region_id:int=None,
                                   area_id:int=None,
                                   start_time:str=None,end_time:str=None, page:int=1,
                                   sortField: str = "begin_date",
                                   sortOrder: str = "desc", token_data=Depends(auth_handler.auth_wrapper)):
    return get_tobacco(db, store_id, region_id, area_id, start_time, end_time, page, sortField, sortOrder)


#new changes
@router.get("/get_data_by_clubcard", status_code=status.HTTP_200_OK) # Authorization Done
def get_data_by_clubcard_(db: Session = Depends(get_db),
                        store_id:int=None,region_id:int=None,area_id:int=None,
                        start_time:str=None,end_time:str=None, page:int=1,
                        sortOrder: str = "desc", token_data=Depends(auth_handler.auth_wrapper)
                        ):

    return get_data_by_clubcard(db, store_id, region_id, area_id, start_time, end_time, page,sortOrder)


@router.get("/get_data_by_clubcard_trigger", status_code=status.HTTP_200_OK) # Authorization Done
def get_data_by_clubcard_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                          area_id: int = None, start_time: str = None, end_time: str = None, page: int = 1,
                          trigger: int = None, sortOrder: str = "desc", token_data=Depends(auth_handler.auth_wrapper)):
    return get_data_by_clubcard_trigger(db, store_id, region_id, area_id, start_time, end_time, trigger, page,sortOrder)


@router.get("/get_clubcard_detail",status_code=status.HTTP_200_OK)
def get_clubcard_detail_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                         area_id: int = None, start_time: str = None, end_time: str = None, clubcard: str = None,
                         page: int = 1, token_data=Depends(auth_handler.auth_wrapper)):
    return get_clubcard_detail(db, store_id, region_id, area_id, start_time, end_time,clubcard, page)


@router.get("/get_clubcard_detail_trigger",status_code=status.HTTP_200_OK)
def get_clubcard_detail_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                         client_region_id: int = None, area_id: int = None, start_time: str = None,
                         end_time: str = None, clubcard: str = None, nudge_type: str = None, comment: str = None,
                         hidden: int = 0, updated: int = 0, operator_id: str = None, sortField: str = "begin_date", 
                         sortOrder: str = "DESC", page: int = 1,token_data=Depends(auth_handler.auth_wrapper)):
    data, count = get_clubcard_detail_trigger(db, store_id, region_id, area_id, start_time, end_time, clubcard, nudge_type,
                                       sortField, comment, hidden, sortOrder, page, client_region_id, updated, operator_id)
    return [data, count]
    # data = get_serialized_object(data)
    # from pprint import pprint
    # pprint([data, count])
    # return return_encoded_data([data, count])


@router.get("/get_clubcard_detail_trigger_new",status_code=status.HTTP_200_OK)
def get_clubcard_detail_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                         client_region_id: int = None, area_id: int = None, start_time: str = None,
                         end_time: str = None, clubcard: str = None, nudge_type: str = None, comment: str = None,
                         hidden: int = 0, sortField: str = "begin_date", sortOrder: str = "DESC", page: int = 1,
                         token_data=Depends(auth_handler.auth_wrapper)):
    clubcard_mapping = {
        "Rectified": "Corrected",
        "Non-Rectified": "Failed",
        "Not-Present": "Monitored"
    }
    data, count = get_clubcard_detail_trigger_new(db, store_id, region_id, area_id, start_time, end_time, clubcard, nudge_type,
                                       sortField, comment, hidden, sortOrder, page, client_region_id)
    res = []
    for record in data:
        record = dict(record)
        record["clubcard"] = clubcard_mapping[record["clubcard"]] if record["clubcard"] in clubcard_mapping else record["clubcard"]
        res.append(record)

    res = get_serialized_object(res)
    return [res, count]
    return return_encoded_data([res, count])


@router.get("/get_clubcard_detail_trigger_to_update",status_code=status.HTTP_200_OK)
def get_clubcard_detail_(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                         client_region_id: int = None, area_id: int = None, start_time: str = None,
                         end_time: str = None, clubcard: str = None, nudge_type: str = None, comment: str = None,
                         hidden: int = 0, updated: int = 0,sortField: str = "begin_date", sortOrder: str = "DESC", page: int = 1,
                         token_data=Depends(auth_handler.auth_wrapper)):
    data, count = get_clubcard_detail_trigger_to_update(
        db, store_id, region_id, area_id, start_time, end_time,  clubcard, nudge_type, sortField, comment, hidden,
        sortOrder, page, client_region_id, updated)
    data = get_serialized_object(data)
    return return_encoded_data([data, count])


@router.get("/get-mainbank-details", status_code=status.HTTP_200_OK)
def get_mainbank_details(db: Session = Depends(get_db), store_id: int = None, region_id: int = None,
                         client_region_id: int = None, area_id: int = None, start_time: str = None,
                         end_time: str = None, clubcard: str = None, nudge_type: str = None, comment: str = None,
                         sortField: str = "begin_date", sortOrder: str = "DESC", page: int = 1,
                         token_data=Depends(auth_handler.auth_wrapper)):
    data, count = fetch_main_bank_detail(db, store_id, region_id, area_id, nudge_type, start_time, end_time, sortField,
                                         sortOrder, page, client_region_id)
    data = get_serialized_object(data)
    return return_encoded_data([data, count])


@router.post("/update-transaction-clubcard", status_code=status.HTTP_200_OK)
def update_transaction_clubcard(body: UpdateTransactionClubcard, db: Session = Depends(get_db),
                                token_data=Depends(auth_handler.auth_wrapper)):
    body = dict(body)
    store_id = body.get("store_id")
    transaction_id = body.get("transaction_id")
    clubcard_value = body.get("clubcard_value")
    # try:
    update_clubcard_value(store_id, transaction_id, clubcard_value, db)
    # except Exception as e:
    #     return HTTPException(400, detail="Transaction not found")

    return {"message": "Updated"}


@router.post("/add_user_view", status_code=status.HTTP_200_OK)
def add_user_view_(details: dict, db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)):
    return add_user_view(details, db)


@router.get("/get_user_view")
def all_region_(db: Session = Depends(get_db), transaction_id: str = None,
                token_data=Depends(auth_handler.auth_wrapper)):
    return get_user_view(db, transaction_id)


@router.get("/get-missed-items-report", status_code=status.HTTP_200_OK)
def get_missed_items_report_(
    start_time: date = None, end_time: date = None, store_id: int = None, hyper_care_stores: bool = False, 
    page: int = 1, per_page: int = 20,  db: Session = Depends(get_db), token_data=Depends(auth_handler.auth_wrapper)
):
    if not start_time and not end_time:
        start_time, end_time = get_last_month_from_current_date()
    else:
        end_time = end_time + relativedelta(days=1)

    result = fetch_missed_items_report(store_id, start_time, end_time, page, per_page, db)\
    
    result_hyper_care = []
    if hyper_care_stores:
        for record in result:
            if record["dashboard_store_id"] in HYPERCARE_STORES:
                result_hyper_care.append(record)

        result = result_hyper_care

    count = len(result)
    result.sort(key=lambda x: x["transaction_start_timestamp"], reverse=True)
    result = result[per_page * (page - 1): per_page * page]
    res = []
    for record in result:
        i = dict(record)
        
        if (i["rectified_not_rectified"] == "Loss" and i["mis_scanned_item_information"] == "Item") or \
            (i["rectified_not_rectified"] == "No Loss") or (i["rectified_not_rectified"] == "Monitored"):
            missed_items = fetch_next_missed_item(db, i["intervention_notification_timestamp"], i["transaction_id"])
            i["next_item_scanned_name"] = missed_items[0].name if len(missed_items) > 0 else ""
            i["next_item_scanned_scan_id"] = missed_items[0].scan_data if len(missed_items) > 0 else ""
            i["second_item_scanned_name"] = missed_items[1].name if len(missed_items) > 1 else ""
            i["second_item_scanned_scan_id"] = missed_items[1].scan_data if len(missed_items) > 1 else ""

        res.append(i)

    return {"data": res, "total": count}
    # return return_encoded_data({"data": res, "total": count})


@router.get("/get-missed-items-report-download", status_code=status.HTTP_200_OK)
def get_missed_items_report_(start_time: date = None, end_time: date = None, store_id: int = None,
                             hyper_care_stores: bool = False, db: Session = Depends(get_db)):
    if not start_time and not end_time:
        start_time, end_time = get_last_month_from_current_date()
    else:
        end_time = end_time + relativedelta(days=1)

    result = fetch_missed_items_report_download(store_id, start_time, end_time, db)
    result.sort(key=lambda x: x["Transaction Start Timestamp"], reverse=True)

    result_hyper_care = []
    if hyper_care_stores:
        for record in result:
            if record["dashboard_store_id"] in HYPERCARE_STORES:
                result_hyper_care.append(record)

        result = result_hyper_care

    data = []
    for record in result:
        record = dict(record)
        data.append({
            "Store Name": record["Store Name"],
            "Store ID": record["Store ID"],
            "Camera ID": record["Camera ID"],
            "Camera Location": record["Camera Location"],
            "Sequence No.": record["Sequence No."],
            "Operator ID": record["Operator ID"],
            "Recording Start Timestamp": record["Recording Start Timestamp"],
            "Recording End Timestamp": record["Recording End Timestamp"],
            "Transaction Start Timestamp": record["Transaction Start Timestamp"],
            "Transaction End Timestamp": record["Transaction End Timestamp"],
            "Trigger Timestamp": record["Intervention / Notification Timestamp"],
            "Trigger Cleared Timestamp": record["Intervention / Notification Cleared Timestamp"],
            "Trigger Type": record["Intervention / Notification Type"],
            "Transaction Key": record["Transaction Key"],
            "Mis-Scanned Item Information": record["Mis-Scanned Item Information"],
            "Scan ID": record["Scan ID"],
            "Next Item Scanned (Name)": record["Next Item Scanned (Name)"],
            "Next Item Scanned (Scan ID)": record["Next Item Scanned (Scan ID)"],
            "2nd Item Scanned (Name)": record["2nd Item Scanned (Name)"],
            "2nd Item Scanned (Scan ID)": record["2nd Item Scanned (Scan ID)"],
            "No Loss/Loss": record["No Loss/Loss"],
            "Price": record["Price"],
            "Sainsburys Comments": record["Sainsburys Comments"],
            "Sai Comments": record["SAI Comments"],
            "Control/Trial": record["Control/Trial"]
        })

    columns = [
        "Store Name", "Store ID", "Camera ID", "Camera Location", "Sequence No.", "Operator ID",
        "Recording Start Timestamp", "Recording End Timestamp", "Transaction Start Timestamp", 
        "Transaction End Timestamp", "Trigger Timestamp", "Trigger Cleared Timestamp", "Trigger Type", 
        "Transaction Key", "Mis-Scanned Item Information", "Scan ID", "Next Item Scanned (Scan ID)", 
        "Next Item Scanned (Name)", "2nd Item Scanned (Scan ID)", "2nd Item Scanned (Name)", "No Loss/Loss", 
        "Price", "Sainsburys Comments", "Sai Comments", "Control/Trial"
    ]

    df = pd.DataFrame(data, columns=columns)

    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)

    buffer.seek(0)
    
    return StreamingResponse(
        buffer, 
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=Video Analytics Data Requirements Sainsbury.xlsx"
        }
    )


@router.get("/last-update")
def get_last_update_time(type: str = "Aisle", db: Session = Depends(get_db),
                         token_data=Depends(auth_handler.auth_wrapper)):
    result = fetch_last_update_time(db, type)
    result = dict(result)
    return return_encoded_data(result)
