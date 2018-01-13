#!/usr/bin/python

import logging
import openpyxl
import csv
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

    #logging.info(str(os.getcwd()))

    #----------------------Start Reconciliation Data---------------------------------
    #All recon data
    if os.path.isfile(POIsFilename):
        ReconFilewriter.writerow([Categories_reconciliation_data[0], Categories_reconciliation_data[1], Categories_reconciliation_data[2],
                                 Categories_reconciliation_data[3], Categories_reconciliation_data[4], Categories_reconciliation_data[5]])
        ReconFilewriter.writerow([Campuses_reconciliation_data[0], Campuses_reconciliation_data[1], Campuses_reconciliation_data[2],
                                 Campuses_reconciliation_data[3], Campuses_reconciliation_data[4], Campuses_reconciliation_data[5]])
        ReconFilewriter.writerow([POIs_reconciliation_data[0], POIs_reconciliation_data[1], POIs_reconciliation_data[2],
                                 POIs_reconciliation_data[3], POIs_reconciliation_data[4], POIs_reconciliation_data[5]])
    else:
        ReconFilewriter.writerow(['categories', 0, 0, 0, 0, 0])
        ReconFilewriter.writerow(['campuses', 0, 0, 0, 0, 0])
        ReconFilewriter.writerow(['POIs', 0, 0, 0, 0, 0])

    if os.path.isfile(TasksFilename):
        ReconFilewriter.writerow([Phases_reconciliation_data[0], Phases_reconciliation_data[1], Phases_reconciliation_data[2],
                                 Phases_reconciliation_data[3], Phases_reconciliation_data[4], Phases_reconciliation_data[5]])
        ReconFilewriter.writerow([Tasks_reconciliation_data[0], Tasks_reconciliation_data[1], Tasks_reconciliation_data[2],
                                 Tasks_reconciliation_data[3], Tasks_reconciliation_data[4], Tasks_reconciliation_data[5]])
    else:
        ReconFilewriter.writerow(['phases', 0, 0, 0, 0, 0])
        ReconFilewriter.writerow(['tasks', 0, 0, 0, 0, 0])

    ReconFile.close()
    #------------------------End Reconciliation Data---------------------------------

    return

if __name__ == '__main__':

    # Get Log file defined
    t = (str(datetime.now().year), str(datetime.now().month), str(datetime.now().day),
         str(datetime.now().hour), str(datetime.now().minute), str(datetime.now().second))
    LogDirectory = os.getcwd() + r'\RMIT60_FileSystem'
    LogFullPathBase = LogDirectory + r'\logfile.log'
    split_LogFullPathBase = LogFullPathBase.split('.')
    LogFullPath = ".".join(split_LogFullPathBase[:-1]) + '_' + "-".join(t) + '.' + ".".join(split_LogFullPathBase[-1:])

    logging.basicConfig(level=logging.DEBUG, handlers=[logging.FileHandler(LogFullPath, 'a+', 'utf-8')],
                            format="%(asctime)-15s - %(levelname)-8s %(message)s")
    logging.info('Log file: ' + str(LogFullPath))

    ReconDirectory = os.getcwd() + r'\RMIT60_FileSystem'
    ReconFullPathBase = ReconDirectory + r'\reconfile.csv'
    split_ReconFullPathBase = ReconFullPathBase.split('.')
    ReconFullPath = ".".join(split_ReconFullPathBase[:-1]) + '_' + "-".join(t) + '.' + ".".join(split_ReconFullPathBase[-1:])

    
    ReconFile = open(ReconFullPath, "w")
    ReconFilewriter = csv.writer(ReconFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
    ReconHeader = ['FileName' , 'File Total Rows' , 'DB Inserted Rows' , 'DB Updated Rows' , 'DB Deleted Rows' , 'File Nochange Rows']
    ReconFilewriter.writerow(ReconHeader)

    process_all_data()



    #split_filename = filename.split('.')
    #os.rename(filename, split_filename[:-1] + '_' + '-'.join(t))
    #logging.basicConfig(level=logging.DEBUG, filename="logfile", filemode="a+",
    #                       format=' %(asctime)s - %(levelname)s - %(message)s')

