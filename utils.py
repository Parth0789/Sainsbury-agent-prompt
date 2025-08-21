import os
import datetime as dt
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import boto3
from dotenv import load_dotenv
from pprint import pprint
import itertools
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from config import config


load_dotenv()
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
AWS_BUCKET_NAME = config.BUCKET_NAME
AWS_REGION = config.AWS_REGION


def get_default_time_range():
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    return start_date, end_date


def current_date_time():
    todayDate = dt.date.today()
    # if todayDate.day > 30:
    #     todayDate += dt.timedelta(7)
    #     print(todayDate)

    # Get the start date of the current month
    # start_time = todayDate.replace(day=1)

    # Get the number of days in the current month
    # _, last_day = calendar.monthrange(now.year, now.month)

    # Get the end date of the current month
    end_time = datetime.now().strftime("%Y-%m-%d")

    # Calculate the start time as 30 days before the current date
    start_time = (todayDate - dt.timedelta(days=365))

    return start_time, end_time


def get_last_3_months_from_current_date():
    todayDate = dt.date.today() # NOQA
    three_months = dt.date.today() + relativedelta(months=-2) # NOQA
    start_time = three_months.replace(day=1)
    end_time = datetime.now().strftime("%Y-%m-%d")
    return start_time, end_time


def get_last_month_from_current_date():
    todayDate = dt.date.today() # NOQA
    last_month = dt.date.today() + relativedelta(months=-1) # NOQA
    start_time = last_month.replace(day=1)
    end_time = datetime.now()
    return start_time, end_time


def get_last_hour_timestamp():
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=5)
    return start_time, end_time


def get_current_year_timestamp():
    current_time = datetime.now()
    start_time = current_time.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    end_time = current_time
    print(start_time, end_time)
    return start_time, end_time


def convert_seconds_to_hhmmss(seconds):
    return str(timedelta(seconds=seconds))


def add_values_stats(main, sco):
    if len(main) > 0 and len(sco) > 0:
        total_data = []
        counts = {}
        for year, month, count, total in main + sco:
            year_month = (year, month)
            if year_month in counts:
                counts[year_month]['count'] += count
                counts[year_month]['total'] += total
            else:
                counts[year_month] = {'count': count, 'total': total}

        for i, (year, month, count, total) in enumerate(main):
            year_month = (year, month)
            if year_month in counts:
                main[i] = (year, month, counts[year_month]['count'], counts[year_month]['total'])

        for year_month_val, counts_val in counts.items():
            total_data.append(
                {
                    "Year": year_month_val[0],
                    "Month": year_month_val[1],
                    "count": counts_val.get("count", 0),
                    "total": counts_val.get("total", 0)
                }
            )
        return total_data
    elif len(main) == 0:
        return sco
    elif len(sco) == 0:
        return main


def sort_by_year_month(item):
    return item['Year'], item['Month']


def sort_by_year_month_week(item):
    return item["Year"], item['Month'], item["Week"]


def sort_by_year_month_day(item):
    return item["Year"], item['Month'], item["Day"]


def merge_dicts(a, b):
    store_names = set(map(lambda x: x["store_name"], a + b))
    result = []
    for store_name in store_names:
        a_store = next((x for x in a if x["store_name"] == store_name), {})
        b_store = next((x for x in b if x["store_name"] == store_name), {})
        result.append(
            {
                "store_name": store_name,
                "main_count": a_store.get("main_count", 0),
                "main_total": b_store.get("main_total", 0)
            }
        )
    return result

def merge_list_of_aisle_theft(aisle_list, intervention_list):
    result = []
    for aisle_item in aisle_list:
        for intervention_item in intervention_list:
            if aisle_item["name"] == intervention_item["name"]:
                result.append({**aisle_item, **intervention_item})
    for aisle_item in aisle_list:
        flag = False # Assume not in the other list
        for result_item in result:
            if aisle_item["name"] == result_item["name"]:
                flag = True # Found in the list
                break
        if flag is False:
            result.append(
                {
                    "id": aisle_item["id"],
                    "name": aisle_item['name'],
                    "intervention_no_of_theft": 0,
                    "intervention_total": 0,
                    "aisle_no_of_theft": aisle_item['aisle_no_of_theft'],
                    "aisle_total": aisle_item['aisle_total']
                }
            )
    return result


def set_permissions(role):
    permissions = {"admin": False, "stores": False, "region": False, "area": False}
    permissions[role] = True
    return permissions


def cal_loss(main_total_count, main, type):
    for a_value in main_total_count:
        for b_value in main:
            if a_value[0] == b_value['Year'] and a_value[1] == b_value['Month']:
                b_value['loss'] = (b_value[f"{type}_count"]/float(a_value[2]))*100
            else:
                b_value['loss'] = "_"

    return main


def cal_loss_single_store(vals, res):
    for item in vals:
        if item['name'] in [x[1] for x in res]:
            b_count = next((x[2] for x in res if x[1] == item['name']), None)
            item['loss'] = (float(item["count"])/b_count)*100
        else:
            item['loss'] = "_"

    return vals


def get_boto3_client(service_name):
    client = boto3.client(
        service_name,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    return client


def get_s3_object_keys(prefix):
    s3 = get_boto3_client("s3")
    response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=prefix)

    if "Contents" not in response:
        return []

    keys = []
    for content in response["Contents"]:
        if len(content["Key"].split(".")) > 1:
            keys.append(content["Key"])

    return keys


class EmailSender:
    def __init__(self, region, aws_access_key, aws_secret_key):
        self.ses_client = boto3.client(
            'ses',
            region_name=region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

    def send_raw_email(self, source_email, destination_email, subject, body, attachment_file):
        today_date = dt.datetime.today().strftime("%Y-%m-%d")
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = source_email
            msg["To"] = ", ".join(destination_email)  # Ensure this is a list

            html_part = MIMEText(body)
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment_file)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f"attachment; filename=sai_store_status_{today_date}.xlsx")

            msg.attach(html_part)
            msg.attach(part)
            print(msg.as_string())
            # Send the email
            response = self.ses_client.send_raw_email(
                Source=source_email,
                Destinations=destination_email,  # List of recipients
                RawMessage={"Data": msg.as_string()}
            )
            print("Email sent! Message ID:", response['MessageId'])
            return response
        except Exception as e:
            print("Error sending email:", e)
            raise

def send_mail(encoded_file):
    # Initialize the email sender
    email_sender = EmailSender(
        region="eu-west-2"
    )

    # Send an email
    email_sender.send_raw_email(
        source_email="notifications@co.uk",
        destination_email=identities,
        subject=f"SAI Store Status {datetime.today().strftime('%Y-%m-%d')}",
        body="Please find attached Sainsbury's store status report. In the attached excel, one tab has a list of store servers that are not working and the other tab has a list of cameras that are not working",
        attachment_file=encoded_file
    )
