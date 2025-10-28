import sys
from io import BytesIO,StringIO
import logging
import csv
import psycopg2
from psycopg2 import sql
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from configparser import ConfigParser
import json

config = ConfigParser()

numOfArgs=len(sys.argv)
if numOfArgs < 2 :
    print('Improper ')
    exit()

configFilePath=sys.argv[1]
print(config.read(configFilePath))

ot_config_file=open(sys.argv[2])
ot_config=json.load(ot_config_file)

logger=logging.getLogger()
logger.setLevel(logging.INFO)

def db_connnection(config):
    env=config.get('Environment','env')
    if env == 'nonprod':
        print('')
        schema_name=config.get('Database','schema_name_np')
        host = config.get('Database','host_np')
        port=config.get('Database','port_np')
        database=config.get('Database','database_np')
        username=config.get('Database','user_np')
        password=config.get('Database','password_np')
    elif env == 'prod' :
        schema_name = config.get('Database', 'schema_name_prod')
        host = config.get('Database', 'host_prod')
        port = config.get('Database', 'port_prod')
        database = config.get('Database', 'database_prod')
        username = config.get('Database', 'user_prod')
        password = config.get('Database', 'password_prod')
    else :
        logger.error('')
        exit()

    conn = psycopg2.connect(host=host,port=port,database=database,user=username,password=password,options=f'-c search_path={schema_name}')
    return conn


def validation(validationQueryList,flag,query_name,seq,cursor) :
    for i in range(len(validationQueryList)):
        queryName = str(validationQueryList[i]['query_name'])
        queryType=str(validationQueryList[i]['query_type'])
        draftQuery = str(validationQueryList[i]['draft_query'])
        paramList=validationQueryList[i]['param_key_list']
        runParamSet = validationQueryList[i]['run_param_set']
        outputSchema = validationQueryList[i]['output_schema']
        temp_query=str(draftQuery)


        if queryType == flag and queryName == query_name :
            print('')
            for j in range(len(runParamSet)) :
                if j ==seq :
                    run_obj = runParamSet[j]
                    expectedValues = runParamSet[j]['expected_value']
                    expectedLen = len(expectedValues)
                    if expectedLen==0 :
                        print()
                        return
                    for e in range(len(paramList)):
                        final_query = temp_query.replace('$'+paramList[e],run_obj[paramList[e]])
                        temp_query=final_query

                    cursor.execute(final_query)
                    data=cursor.fetchall()

                    df= pd.DataFrame(data=data,columns=outputSchema)

                    expectedCounter=0
                    flag='true'
                    for r,t in df.iterrows() :
                        for column in outputSchema :
                            expectedColumnValue = expectedValues[expectedCounter][str(column)]
                            dataValue = t[str(column)]

                            if expectedColumnValue != dataValue :
                                flag ='false'
                                print('')
                                exit()
                            print('')

                            expectedCounter +=1
                            if expectedCounter > expectedLen :
                                print('')
                                return
                            if flag=='true':
                                print('data match')



def data_ot() :

    table_list= ot_config['ot_params']['table_names']
    backup_flag = ot_config['ot_params']['backup']
    queries = ot_config['ot_params']['ot_queries']
    validationQueries = ot_config['ot_params']['validation_queries']
    validation_flag=ot_config['ot_params']['validations']
    length = len(queries)
    conn = db_connnection(config)
    cursor = conn.cursor()

    if backup_flag == 'true' :
        backup_query = str(ot_config['ot_params']['backup_query'])
        for t in range(len(table_list)) :
            backup_table_name = table_list[t]+'_backup_'+str(datetime.today().date())
            backup_table_name=str(backup_table_name).replace('-','_')
            print()
            backup_query_final=backup_query.replace('$backup_table_name',backup_table_name)
            backup_query_final=backup_query_final.replace('$table_name',str(table_list[t]))
            print(backup_query_final)
            cursor.execute(backup_query_final)
            conn.commit()
            print('Backup table created - '+backup_table_name)

    for i in range(length) :
        query_name=queries[i]['query_name']
        query_draft=queries[i]['draft_query']
        temp_query=str(query_draft)
        param_key_list=queries[i]['param_key_list']
        run_param_set=queries[i]['run_param_set']
        final_query=''

        run_set_length=len(run_param_set)
        for j in range(run_set_length) :
            run_obj=run_param_set[j]

            for e in range(len(param_key_list)):
                final_query=temp_query.replace('$'+param_key_list[e],run_obj[param_key_list[e]])
                temp_query=final_query
            print('')
            if validation_flag=='true' :
                print()
                validation(validationQueries,'pre',query_name,j,cursor)

            print('')
            cursor.execute(final_query)
            conn.commit()
            if validation_flag == 'true' :
                print('')
                validation(validationQueries,'post',query_name,j,cursor)

            print('')
            temp_query=query_draft

data_ot()