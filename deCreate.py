# deCreate.py

import FuelSDK as f
import teradata
import time
import json
import requests
import config
import os, ssl
import argparse

def request_token():

    # requestToken
    token_url = "https://auth.exacttargetapis.com/v1/requestToken"
    headers = {'content-type': 'application/json'}
    token_data = {"clientId": config.clientid, "clientSecret": config.clientsecret}

    response = requests.post(token_url, json=token_data, headers=headers)

    token = json.loads(response.text)

    # print("access token =" + str(token['accessToken']))
    return str(token['accessToken'])


def get_columns_str(table_name):

    '''
    Get cols and datatypes for table name. Tablename includes schema / db name
    :param table_name:
    :return: String of cols, comma separated
    '''

    dsn = 'TDDB'
    udaExec = teradata.UdaExec(appName="tdPyInterface", version="1.0",
                               logConsole=False, appConfigFile="tdPyInterface.ini")
    session = udaExec.connect(dsn)
    cols = []  # to store cols
    col_qry = "select * from "+table_name+' where 1=2'
    cursor = session.execute(col_qry)
    for row in cursor.description:
        cols.append(row[0])
    tbl_cols = []  # to store cols in expected format
    for x in cols:
        tbl_cols.append(x.strip())

    # convert to str preserving string quotes and delim
    col_str = ",".join(tbl_cols)
    return col_str
    # pass


def fetch_table_data_json(table_name):

    dsn = 'TDDB'  # 'TDDEV', 'TDDB'
    udaExec = teradata.UdaExec(appName="tdPyInterface", version="1.0",
                               logConsole=False, appConfigFile="tdPyInterface.ini")
    session = udaExec.connect(dsn)
    rows = []  # to store rows list
    cols = []

    qry = "select cast(JSON_Compose(" + get_columns_str(table_name) + ") as CLOB) PAYLOAD from " + table_name
    # print(qry)
    cursor = session.execute(qry)

    try:
        while True:
            row = cursor.fetchone().values
            rows.append(json.loads(row[0]))  # to convert str row to dict
            if row is None:
                break
    except Exception as e:  # to handle None
        print('Caught exception: ' + str(e))
        print("Payload ready")
    # print(rows)
    return rows
    # return table_data_list


def de_load_async(table_name, target_de, chunk_size):

    # request token
    auth_token = 'Bearer ' + request_token()  # Bearer Token
    # print("auth_token = " + auth_token)

    headers = {'content-type': 'application/json',
               'Authorization': auth_token}

    de_name = str(target_de)
    # print(de_name)
    # print(type(de_name))

    async_url = "https://www.exacttargetapis.com/data/v1/async/dataextensions/key:"+de_name+"/rows"

    src_table = str(table_name)
    # print("source table= " % src_table)

    # TODO: Chunk payload to {chunk_size} rows each request - COMPLETED

    # src_data = list()
    src_data = fetch_table_data_json(src_table)
    # print(src_data)

    print("Initiating chunking...")

    data = list()  # to store chunks
    items = len(src_data)

    chunk_count = 0
    chunk_size = int(chunk_size)

    for i in range(0, len(src_data), chunk_size):
        chunk_count += 1
        data = src_data[i:i+chunk_size]
        payload = {"items": data}
        # post async req
        response = requests.post(async_url, json=payload, headers=headers)
        # print(response.text)

        del data[:]  # empty the list

        if int(response.status_code) == 200:
            print("%s data chunk %d loaded successfully!" % (de_name, chunk_count))
        else:
            print("%s data chunk %d returned status: %s" % (de_name, chunk_count, response.status_code))

    print("Load complete.")


def fetch_table_data(table_name):

    dsn = 'TDDB'  # 'TDDEV', 'TDDB'
    udaExec = teradata.UdaExec(appName="tdPyInterface", version="1.0",
                               logConsole=False, appConfigFile="tdPyInterface.ini")
    session = udaExec.connect(dsn)
    cols = []  # to store cols
    vals = []  # to store data
    table_data_dict = {}
    table_data_list = []

    qry = "select * from "+table_name
    cursor = session.execute(qry)

    for row in cursor.description:
        cols.append(row[0])
    for row in cursor:
        vals.append(str(row))

    for item in vals:
        data = item[item.find('[') + 1:item.find(']')]
        # print("data = %s\n"% data)

        parts = data.split(',')
        # print("parts = %s\n"% parts)

        for x, y in zip(cols, parts):
            table_data_dict.update({x.strip(): y.strip()})
        # print("table_data_dict = %s\n"% table_data_dict)

        table_data_list.append(table_data_dict.copy())
        # need to append a copy, otherwise you are just adding references to the same dictionary over and over again

        # print("table_data_list = %s"% table_data_list)
    # print(table_data_list)
    return table_data_list


def get_columns(table_name):

    # TODO: Add datatype map
    '''
    Get cols and datatypes for table name. Tablename includes schema / db name
    :param table_name:
    :return: list of dicts. Each dict represents a col. e.g. {"Name": "Col1"},{"Name": "Col2"}
    '''

    dsn = 'TDDB'
    udaExec = teradata.UdaExec(appName="tdPyInterface", version="1.0",
                               logConsole=False, appConfigFile="tdPyInterface.ini")
    session = udaExec.connect(dsn)
    cols = []  # to store cols
    col_qry = "select * from "+table_name+' where 1=2'
    cursor = session.execute(col_qry)
    for row in cursor.description:
        cols.append(row[0])
    tbl_cols = []  # to store cols in expected format
    for x in cols:
        tbl_cols.append({"Name": x.strip()})

    # print(tbl_cols)
    return tbl_cols


def get_columns_with_datatypes(table_name):

    # TODO: Add datatype map - COMPLETED
    '''
    Get cols and datatypes for table name. Tablename includes schema / db name
    :param table_name:
    :return: list of dicts. Each dict represents a col. e.g. {"Name": "Col1"},{"Name": "Col2"}
    '''

    dsn = 'TDDB'
    udaExec = teradata.UdaExec(appName="tdPyInterface", version="1.0",
                               logConsole=False, appConfigFile="tdPyInterface.ini")
    session = udaExec.connect(dsn)
    cols = []  # to store cols
    dts = []  # data types
    col_width = []
    col_qry = "select * from "+table_name+' where 1=2'
    cursor = session.execute(col_qry)
    for row in cursor.description:
        cols.append(row[0])

        # if row[1].__name__ == "Decimal":
        #     row[1] = "decimal"
        # elif row[1].__name__ == 'str':
        #     row[1] = "text"
        # elif row[1].__name__ == 'datetime':
        #     row[1] = "datetime"
        # else:
        #     row[1] = "text"

        # [ <class 'decimal.Decimal'>, < class 'str' >, < class 'datetime.datetime' >, < class 'decimal.Decimal' >]
        dts.append(row[1].__name__)  # to get the name of the class
        col_width.append([3])

    # fix data type names in dts list
    for x, dt in enumerate(dts):
        if "datetime" in dt:
            dts[x] = 'Date'  # update list val to match SFMC data type for datetime col
        elif 'str' in dt:
            dts[x] = 'Text'  # change str to Text

    tbl_cols = []  # to store cols in expected format

    for x, y in zip(cols, dts):
        tbl_cols.append({"Name": x.strip(), "FieldType": y})
    #     tbl_cols.append(x.strip())

    # ['sk', 'id', 'dt', 'num_id']

    # [[3], [3], [3], [3]]
    # print(tbl_cols)
    return tbl_cols


def load_de(table_name, de_name):

    debug = False
    stubObj = f.ET_Client(False, debug)
    DE_NAME = de_name
    FOLDER_ID = 502  # API_GEN
    de_to_load = f.ET_DataExtension_Row()
    de_to_load.CustomerKey = de_name
    de_to_load.auth_stub = stubObj
    # de_to_load.props = {"Col1": "Value1", "Col2": "Value2"}
    de_to_load.props = fetch_table_data(table_name)
    de_loaded_response = de_to_load.post()

    # print('Post Status: ' + str(de_loaded_response.status))
    # print('Code: ' + str(de_loaded_response.code))
    # print('Message: ' + str(de_loaded_response.message))
    # print('Results: ' + str(de_loaded_response.results))


def de_create(de_name):
    try:
        debug = False
        stubObj = f.ET_Client(False, debug)
        target_de = de_name
        target_folder = 502  #API_GEN

    # Create  Data Extension
        print('Creating Data Extension %s' % target_de)
        de = f.ET_DataExtension()
        de.auth_stub = stubObj

        de.props = {"Name": target_de, "CustomerKey": target_de, "CategoryID": target_folder}
        # de.columns = get_columns(table_name)
        de.columns = get_columns_with_datatypes(table_name)  # switched to new version with data types
        # print(de.columns)

        # de.columns = [{'Name': 'PRODUCT_SK', 'FieldType': 'Decimal'}, {'Name': 'PRODSKU', 'FieldType': 'Text'}, {'Name': 'PRODUCTTYPE', 'FieldType': 'Text'}, {'Name': 'PRODUCTNAME', 'FieldType': 'Text'}, {'Name': 'PRODUCTDESC', 'FieldType': 'Text'}, {'Name': 'HIERLEVEL1', 'FieldType': 'Text'}, {'Name': 'HIERLEVEL2', 'FieldType': 'Text'}, {'Name': 'HIERLEVEL3', 'FieldType': 'Text'}, {'Name': 'REVENUEROLLUPFLAG', 'FieldType': 'Text'}, {'Name': 'UNITROLLUPFLAG', 'FieldType': 'Text'}, {'Name': 'PRODUCTCATEGORY', 'FieldType': 'Text'}, {'Name': 'PRODUCTSTATUS', 'FieldType': 'Text'}, {'Name': 'PCR_HIERLEVEL', 'FieldType': 'Text'}, {'Name': 'PLAN_RUN_ID', 'FieldType': 'Decimal'}, {'Name': 'SOURCE_SYSTEM_NAME', 'FieldType': 'Text'}, {'Name': 'CREATED_BY', 'FieldType': 'Text'}, {'Name': 'CREATED_DATE', 'FieldType': 'date'}, {'Name': 'MODIFIED_BY', 'FieldType': 'Text'}, {'Name': 'MODIFIED_DATE', 'FieldType': 'date'}, {'Name': 'PRODUCT_GROUP', 'FieldType': 'Text'}, {'Name': 'PCR_ALT_HIERLEVEL', 'FieldType': 'Text'}, {'Name': 'BRANDID', 'FieldType': 'Text'}]

        # de.columns = [{'Name': 'PRODUCT_SK', 'FieldType': 'Decimal'}, {'Name': 'PRODSKU', 'FieldType': 'Text'},
        #               {'Name': 'TESTDATE', 'FieldType': 'Date'}]

        # de.columns = [
        #     {"Name": "Field1", 'FieldType': 'Text / Decimal /Date'},
        #     {"Name": "Field2"},
        #     {"Name": "Field3"},
        #     {"Name": "Field4"},
        #     {"Name": "Field5"},
        # ]

        properties = de.props
        de.search_filter = {'Property': 'CustomerKey', 'SimpleOperator': 'equals', 'Value': target_de}
        filter = de.search_filter
        de_exists = de.get(properties, filter)

        if len(de_exists.results) == 0:  # If DE does not exist, post
            post_response = de.post()
            print('Post Status: ' + str(post_response.status))
            print('Code: ' + str(post_response.code))
            # print('Message: ' + str(post_response.message))
            # print('Results: ' + str(post_response.results))
        else:
            # pass
            # TODO: Drop and Recreate DE - [COMPLETED]
            print("Warning: DE exists. Deleting DE %s" % target_de)
            delResponse = de.delete()
            print('Delete Status: ' + str(delResponse.status))
            print('Code: ' + str(delResponse.code))

            print("Creating DE %s" % target_de)
            post_response = de.post()
            print('Post Status: ' + str(post_response.status))
            print('Code: ' + str(post_response.code))

    except Exception as e:
        print('Caught exception: ' + str(e))
        print(e)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="SFMC Async Data Load")

    parser.add_argument('-s', action="store", dest='table_name',
                        help="source teradata table")
    parser.add_argument('-t', action="store", dest='target_de',
                        help="target salesforce data extension")
    arg_val = parser.parse_args()

    # fix for SSL CERTIFICATE_VERIFY_FAILED exception  FuelSDK
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
            getattr(ssl, '_create_unverified_context', None)):
        ssl._create_default_https_context = ssl._create_unverified_context

    # table_name='cmdm.cmdm_product_d'
    table_name = arg_val.table_name
    # target_de = 'ODS_CMDM_PRODUCT_D'
    target_de = arg_val.target_de
    chunk_size = 5000

    start_time = time.time()

    de_create(target_de)

    de_load_async(table_name, target_de, chunk_size)

    # load_de('cmdm.cmdm_product_d','DE_API_SAT_005')
    # fetch_table_data_json('cmdm.cmdm_product_d')
    # get_columns_with_datatypes(table_name)
    # request_token()

    print("--- %s seconds ---" % (time.time() - start_time))
