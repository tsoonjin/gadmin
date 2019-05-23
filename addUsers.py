import json
import boto3
import os
import logging

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def handler(event, context):
    object = s3.get_object(Bucket='gadmin-lambda', Key='client_secrets.json')
    serializedObject = object['Body']
    print(serializedObject)
    print(json.load(serializedObject))
    return {"status": 200}
