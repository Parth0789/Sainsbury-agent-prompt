import requests
import boto3
from botocore.exceptions import ClientError
from fastapi import HTTPException
from config import config

BUCKET_NAME = config.BUCKET_NAME
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
AWS_REGION = config.AWS_REGION


URL = "https://k9ga2.execute-api.eu-west-2.amazonaws.com/s1/copy_file"


def transfer_video_in_s3(store_id, date, transaction_id,video_folder="alerts"):
    headers = {
        "Content-Type": "application/json"
    }

    print(date)
    data = {
        "store_id": store_id,
        "date": date,
        "count": video_folder,
        "video_name": f"{transaction_id}.mp4",
        "bucket_name": "sainsbury-zip"
    }

    try:
        response = requests.post(URL, headers=headers, json=data, verify=False)
    except Exception as e:
        print(e)
        return False

    if response.status_code == 200:
        return True
    
    return False
    
def create_presigned_url(object_name):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID, 
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 
        region_name=AWS_REGION
    )

    try:
        response = s3_client.generate_presigned_url(
            'get_object', 
            Params={'Bucket': BUCKET_NAME, 'Key': object_name}, 
            ExpiresIn=60
        )
    except ClientError as e:
        print(e)
        raise HTTPException(status_code=500, detail="Error generating presigned URL")
    
    return response
