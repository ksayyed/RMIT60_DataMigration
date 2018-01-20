#!/usr/bin/python

import boto3
import botocore
import logging
import openpyxl
import csv
import psycopg2
import os
import sys
from datetime import datetime
from GetDBConfigParam import GetDBConfigParam 
from GetS3Session import GetS3Session 
from MigrateCampuses import process_Campuses_data
from MigrateCategories import process_Categories_data 
from MigratePhases import process_Phases_data
from MigratePOIs import process_POIs_data
from MigrateTasks import process_Tasks_data


def process_all_data():

    # read connection parameters
    params = GetDBConfigParam()

    # get S3 session established
    session = GetS3Session()

    # get the S3 bucket
    s3_resource = session.resource('s3')
    bucket_name = str(os.environ["AWS_BUCKET_NAME"])
    s3_bucket = s3_resource.Bucket(bucket_name)
    logging.info('S3 bucket name: ' + str(bucket_name))

    # check for the file and process it
    POIsFilename = 'Wayfinding Locations.xlsx'
    key_file_name = POIsFilename
    POIsLocalFilename = "-".join(t) + '_' + POIsFilename
    local_file_name = POIsLocalFilename
    file_count = 0
    try:
        s3_bucket.download_file(key_file_name, local_file_name)
        file_count = file_count + 1   # Make sure, data file is avaiable 

        logging.info('================================================================================')

        logging.info(str(key_file_name) + ' available for local processing as ' + str(local_file_name) + '.')
        archive_file_name = 'archive/' + "-".join(t) + '/' + local_file_name
        s3_bucket.upload_file(local_file_name, archive_file_name)
        logging.info(str(local_file_name) + ' is transferred to S3 bucket.')

        logging.info('================================================================================')


        logging.info('Pocessing Categories....')
        Categories_reconciliation_data = process_Categories_data(params, s3_bucket, local_file_name)
        ReconFilewriter.writerow([Categories_reconciliation_data[0], Categories_reconciliation_data[1], Categories_reconciliation_data[2],
                                 Categories_reconciliation_data[3], Categories_reconciliation_data[4], Categories_reconciliation_data[5]])


        recon_local_file_name = ReconFilename
        recon_archive_file_name = 'archive/' + "-".join(t) + '/' + recon_local_file_name
        s3_bucket.upload_file(recon_local_file_name, recon_archive_file_name)
        logging.info(str(ReconFilename) + ' is transferred to S3 bucket after processing data.')

        logging.info(str(LogFilename) + ' will be transferred to S3 bucket.')
        log_local_file_name = LogFilename
        log_archive_file_name = 'archive/' + "-".join(t) + '/' + log_local_file_name
        s3_bucket.upload_file(log_local_file_name, log_archive_file_name)

        logging.info('--------------------------------------------------------------------------------')

        logging.info('Processing Campuses...' )
        Campuses_reconciliation_data = process_Campuses_data(params, s3_bucket, local_file_name)
        ReconFilewriter.writerow([Campuses_reconciliation_data[0], Campuses_reconciliation_data[1], Campuses_reconciliation_data[2],
                                 Campuses_reconciliation_data[3], Campuses_reconciliation_data[4], Campuses_reconciliation_data[5]])


        recon_local_file_name = ReconFilename
        recon_archive_file_name = 'archive/' + "-".join(t) + '/' + recon_local_file_name
        s3_bucket.upload_file(recon_local_file_name, recon_archive_file_name)
        logging.info(str(ReconFilename) + ' is transferred to S3 bucket after processing data.')

        logging.info(str(LogFilename) + ' will be transferred to S3 bucket.')
        log_local_file_name = LogFilename
        log_archive_file_name = 'archive/' + "-".join(t) + '/' + log_local_file_name
        s3_bucket.upload_file(log_local_file_name, log_archive_file_name)

        logging.info('--------------------------------------------------------------------------------')

        logging.info('Processing POIs...' )
        POIs_reconciliation_data = process_POIs_data(params, s3_bucket, local_file_name)
        ReconFilewriter.writerow([POIs_reconciliation_data[0], POIs_reconciliation_data[1], POIs_reconciliation_data[2],
                                 POIs_reconciliation_data[3], POIs_reconciliation_data[4], POIs_reconciliation_data[5]])

        recon_local_file_name = ReconFilename
        recon_archive_file_name = 'archive/' + "-".join(t) + '/' + recon_local_file_name
        s3_bucket.upload_file(recon_local_file_name, recon_archive_file_name)
        logging.info(str(ReconFilename) + ' is transferred to S3 bucket after processing data.')

        logging.info(str(LogFilename) + ' will be transferred to S3 bucket.')
        log_local_file_name = LogFilename
        log_archive_file_name = 'archive/' + "-".join(t) + '/' + log_local_file_name
        s3_bucket.upload_file(log_local_file_name, log_archive_file_name)

        logging.info('--------------------------------------------------------------------------------')

        # Delete source files to stop processing twice. This file is copied to S3 additing timestamp
        logging.info(str(key_file_name) + ' will be deleted from S3 as ' + str(local_file_name) + ' is added to S3.')
        #s3_bucket.Object(key_file_name).delete()
        
    except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == "404":
                logging.info('================================================================================')                
                logging.info(str(key_file_name) + ' not available for processing.')
                logging.info('================================================================================')
                ReconFilewriter.writerow(['categories', 0, 0, 0, 0, 0])
                ReconFilewriter.writerow(['campuses', 0, 0, 0, 0, 0])
                ReconFilewriter.writerow(['POIs', 0, 0, 0, 0, 0])
            else:
                #raise
                logging.error(error)

    # check for the file and process it
    TasksFilename = 'Onboarding Tasks.xlsx'
    key_file_name = TasksFilename
    TasksLocalFilename = "-".join(t) + '_' + TasksFilename 
    local_file_name = TasksLocalFilename
    try:
        s3_bucket.download_file(key_file_name, local_file_name)
        file_count = file_count + 1   # Make sure, data file is avaiable
        logging.info('================================================================================')
        logging.info(str(key_file_name) + ' available for local processing as ' + str(local_file_name) + '.')
        archive_file_name = 'archive/' + "-".join(t) + '/' + local_file_name
        s3_bucket.upload_file(local_file_name, archive_file_name)
        logging.info(str(local_file_name) + ' is transferred to S3 bucket.')
        logging.info('================================================================================')

        logging.info('Processing Phases...')
        Phases_reconciliation_data = process_Phases_data(params, s3_bucket, local_file_name)
        ReconFilewriter.writerow([Phases_reconciliation_data[0], Phases_reconciliation_data[1], Phases_reconciliation_data[2],
                                 Phases_reconciliation_data[3], Phases_reconciliation_data[4], Phases_reconciliation_data[5]])

        recon_local_file_name = ReconFilename
        recon_archive_file_name = 'archive/' + "-".join(t) + '/' + recon_local_file_name
        s3_bucket.upload_file(recon_local_file_name, recon_archive_file_name)
        logging.info(str(ReconFilename) + ' is transferred to S3 bucket after processing data.')

        logging.info(str(LogFilename) + ' will be transferred to S3 bucket.')
        log_local_file_name = LogFilename
        log_archive_file_name = 'archive/' + "-".join(t) + '/' + log_local_file_name
        s3_bucket.upload_file(log_local_file_name, log_archive_file_name)

        logging.info('--------------------------------------------------------------------------------')

        logging.info('Processing Tasks...')
        Tasks_reconciliation_data = process_Tasks_data(params, s3_bucket, local_file_name)
        ReconFilewriter.writerow([Tasks_reconciliation_data[0], Tasks_reconciliation_data[1], Tasks_reconciliation_data[2],
                                 Tasks_reconciliation_data[3], Tasks_reconciliation_data[4], Tasks_reconciliation_data[5]])

        recon_local_file_name = ReconFilename
        recon_archive_file_name = 'archive/' + "-".join(t) + '/' + recon_local_file_name
        s3_bucket.upload_file(recon_local_file_name, recon_archive_file_name)
        logging.info(str(ReconFilename) + ' is transferred to S3 bucket after processing data.')

        logging.info(str(LogFilename) + ' will be transferred to S3 bucket.')
        log_local_file_name = LogFilename
        log_archive_file_name = 'archive/' + "-".join(t) + '/' + log_local_file_name
        s3_bucket.upload_file(log_local_file_name, log_archive_file_name)

        logging.info('--------------------------------------------------------------------------------')

        # Delete source files to stop processing twice. This file is copied to S3 additing timestamp
        logging.info(str(key_file_name) + ' will be deleted from S3 as ' + str(local_file_name) + ' is added to S3.')
        #s3_bucket.Object(key_file_name).delete()
            
    except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == "404":
                logging.info('================================================================================')                
                logging.info(str(key_file_name) + ' not available for processing.')
                logging.info('================================================================================')                

                ReconFilewriter.writerow(['phases', 0, 0, 0, 0, 0])
                ReconFilewriter.writerow(['tasks', 0, 0, 0, 0, 0])
            else:
                #raise
                logging.error(error)

    # Recon file close and move to S3 only if it has processed data file
    ReconFile.close()
    if file_count > 0:    
        recon_local_file_name = ReconFilename
        recon_archive_file_name = 'archive/' + "-".join(t) + '/' + recon_local_file_name
        s3_bucket.upload_file(recon_local_file_name, recon_archive_file_name)
        logging.info(str(ReconFilename) + ' is transferred to S3 bucket.')

    # Log file close and move to S3 only if it has processed data file
    logging.info(str(LogFilename) + ' will be closed and then transferred to S3 bucket.')
    logging.info('End of the Job')
    logging.shutdown()
    if file_count > 0:    
        log_local_file_name = LogFilename
        log_archive_file_name = 'archive/' + "-".join(t) + '/' + log_local_file_name
        s3_bucket.upload_file(log_local_file_name, log_archive_file_name)
    
    return

if __name__ == '__main__':

    # Get time
    t = (str(datetime.now().year), str(datetime.now().month), str(datetime.now().day),
         str(datetime.now().hour), str(datetime.now().minute), str(datetime.now().second))

    # Get Log file defined
    # Log level "DEBUG" (logging.DEBUG) generate many debug log messages from Boto3
    LogFilename = "-".join(t) + '_' + r'logfile.log'
    logging.basicConfig(level=logging.INFO, handlers=[logging.FileHandler(LogFilename, 'a+', 'utf-8')],
                            format="%(asctime)-15s - %(levelname)-8s %(message)s")
    logging.info('Log file: ' + str(LogFilename))

    # Recon file
    ReconFilename = "-".join(t) + '_' + r'reconfile.csv'
    ReconFile = open(ReconFilename, "w")
    ReconFilewriter = csv.writer(ReconFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
    logging.info('Recon file: ' + str(ReconFilename))

    # Recon file header
    ReconHeader = ['FileName' , 'File Total Rows' , 'DB Inserted Rows' , 'DB Updated Rows' , 'DB Deleted Rows' , 'File Nochange Rows']
    ReconFilewriter.writerow(ReconHeader)

    process_all_data()
