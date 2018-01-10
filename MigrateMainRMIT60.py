#!/usr/bin/python

import logging
import openpyxl
import psycopg2
import os
import sys
from datetime import datetime
from GetDBConfigParam import GetDBConfigParam 
from MigrateCampuses import process_Campuses_data
from MigrateCategories import process_Categories_data 
from MigratePhases import process_Phases_data
from MigratePOI import process_POIs_data
from MigrateTasks import process_Tasks_data


def process_all_data():

    # read connection parameters
    params = GetDBConfigParam()

    # Set File System Path as Current Work Directory (CWD)
    SourceDirectory = os.getcwd()
    logging.info('Source Location: ' + str(SourceDirectory))

    os.chdir(SourceDirectory + r'\RMIT60_FileSystem')
    DataDirectory = os.getcwd()
    #logging.info('File Location: ' + str(DataDirectory))

    POIsFilename = 'Wayfinding Locations.xlsx'
    if os.path.isfile(POIsFilename):
        logging.info(str(POIsFilename) + ' available for processing.')
        logging.info('--------------------------------------------------------------------------------')
        logging.info('Working Location for Categories: ' + str(os.getcwd()))
        Categories_reconciliation_data = process_Categories_data(params, DataDirectory)
        #logging.info(str(Categories_reconciliation_data))

        logging.info('--------------------------------------------------------------------------------')
        logging.info('Working Location for Campuses: ' + str(os.getcwd()))
        Campuses_reconciliation_data = process_Campuses_data(params, DataDirectory)
        #logging.info(str(Campuses_reconciliation_data))

        logging.info('--------------------------------------------------------------------------------')
        logging.info('Working Location for POIs: ' + str(os.getcwd()))
        POIs_reconciliation_data = process_POIs_data(params, DataDirectory)
        #logging.info(str(POIs_reconciliation_data))
    else:
        logging.info(str(POIsFilename) + ' not available for processing.')

    TasksFilename = 'Onboarding Tasks.xlsx'
    if os.path.isfile(TasksFilename):
        logging.info(str(TasksFilename) + ' available for processing.')
        logging.info('--------------------------------------------------------------------------------')
        logging.info('Working Location for Phases: ' + str(os.getcwd()))
        Phases_reconciliation_data = process_Phases_data(params, DataDirectory)
        #logging.info(str(Phases_reconciliation_data))

        logging.info('--------------------------------------------------------------------------------')
        logging.info('Working Location for Tasks: ' + str(os.getcwd()))
        Tasks_reconciliation_data = process_Tasks_data(params, DataDirectory)
        #logging.info(str(Tasks_reconciliation_data))
    else:
        logging.info(str(TasksFilename) + ' not available for processing.')

    #logging.info(os.getcwd(), '\n')

    return

if __name__ == '__main__':

    # Get Log file defined
    t = (str(datetime.now().year), str(datetime.now().month), str(datetime.now().day),
         str(datetime.now().hour), str(datetime.now().minute), str(datetime.now().second))
    LogDirectory = os.getcwd() + r'\RMIT60_FileSystem'
    LogFullPathBase = LogDirectory + r'\logfile.log'
    split_LogFullPathBase = LogFullPathBase.split('.')
    LogFullPath = ".".join(split_LogFullPathBase[:-1]) + '_' + "-".join(t) + '.' + ".".join(split_LogFullPathBase[-1:])

    logging.basicConfig(level=logging.DEBUG, filename=LogFullPath, filemode="a+",
                            format="%(asctime)-15s - %(levelname)-8s %(message)s")
    logging.info('Log file: ' + str(LogFullPath))

    process_all_data()



    #split_filename = filename.split('.')
    #os.rename(filename, split_filename[:-1] + '_' + '-'.join(t))
    #logging.basicConfig(level=logging.DEBUG, filename="logfile", filemode="a+",
    #                       format=' %(asctime)s - %(levelname)s - %(message)s')

