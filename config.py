import os
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

class Config:    
    AWS_ACCESS_KEY_ID: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    AWS_REGION: Optional[str] = os.getenv("AWS_REGION", "")
    AWS_SES_ACCESS_KEY: Optional[str] = os.getenv("AWS_SES_ACCESS_KEY", "")
    AWS_SES_SECRET_KEY: Optional[str] = os.getenv("AWS_SES_SECRET_KEY", "") 
    BUCKET_NAME: Optional[str] = os.getenv("BUCKET_NAME", "")
    COMPANY: Optional[str] = os.getenv("COMPANY", "")

config = Config()

FRESHDESK_API_KEY = "ZDI0cm4yUHNqMjM0bm5kYzdidlA="
FRESHDESK_API_BASE_URL = "https://saigroup.freshdesk.com"