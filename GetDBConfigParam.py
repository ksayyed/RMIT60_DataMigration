#!/usr/bin/python

import boto3
import logging
import os
import psycopg2
from urllib import parse
from configparser import ConfigParser

### Read Database Config 
#def GetDBConfigParam(filename='MyConfig.ini', section='postgresql'):
#   # create a parser
#    parser = ConfigParser()
#    # read config file
#    parser.read(filename)
 
#    # get section, default to postgresql
#    db = {}
#    if parser.has_section(section):
#        params = parser.items(section)
#        for param in params:
#            db[param[0]] = param[1]
#    else:
#        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

#    logging.info('params: ' + str(db))
#    return db

### Read Database Config from .env file 
def GetDBConfigParam():

    parse.uses_netloc.append("postgres")
    url = parse.urlparse(os.environ["DATABASE_URL"])
    logging.info('DATABASE_URL: ' + str(url))

    return url
 
if __name__ == '__main__':
    GetDBConfigParam()
