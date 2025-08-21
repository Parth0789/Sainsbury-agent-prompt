"""
Utilities package for the SAI Dashboard API.

This package contains various utility modules for different aspects of the application.
"""

# Import commonly used functions from status_utils
from utils.status_utils import determine_status_message, get_issue_message

# Import commonly used functions from general
from utils.general import (
    get_default_time_range,
    current_date_time,
    get_last_3_months_from_current_date,
    get_last_month_from_current_date,
    get_last_hour_timestamp,
    get_current_year_timestamp,
    convert_seconds_to_hhmmss,
    add_values_stats,
    sort_by_year_month,
    sort_by_year_month_week,
    sort_by_year_month_day,
    merge_dicts,
    merge_list_of_aisle_theft,
    set_permissions,
    cal_loss,
    cal_loss_single_store,
    get_boto3_client,
    get_s3_object_keys,
    send_mail
)

from .datetime_utils import (
    calculate_date_range_for_time_range,
    get_week_start_dates,
    format_date_range_label
)

__all__ = [
    'determine_status_message',
    'get_issue_message',
    'calculate_date_range_for_time_range',
    'get_week_start_dates',
    'format_date_range_label',
    'send_mail'
]
