#!/usr/bin/python

import boto3
import logging
import os

### Read S3 Config from .env file 
def GetS3Session():

    access_key_id = str(os.environ["AWS_ACCESS_KEY_ID"])
    secret_access_key = str(os.environ["AWS_SECRET_ACCESS_KEY"])    
    #logging.info('AWS_ACCESS_KEY_ID: ' + str(access_key_id))
    #logging.info('AWS_SECRET_ACCESS_KEY: ' + str(secret_access_key))

    session = boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key)

    #logging.info('S3 session: ' + str(session))
    logging.info('S3 session activated...')
    return session
 
if __name__ == '__main__':
    GetS3Session()
