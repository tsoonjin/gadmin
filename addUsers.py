import boto3
import os
import logging

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    return {"status": 200}
