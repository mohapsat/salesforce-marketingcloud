# deCreate.py

import FuelSDK as f
import teradata
import time
import json
import requests
import config
import os, ssl

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
    tbl_cols = [] #to store cols in expected format
    for x in cols:
        tbl_cols.append(x.strip())

    # convert to str preserving string quotes and delim
    col_str = ",".join(tbl_cols)
    return col_str
    # pass


def fetch_table_data_json(table_name):

    dsn = 'TDDB' #'TDDEV', 'TDDB'
    udaExec = teradata.UdaExec(appName="tdPyInterface", version="1.0",
                               logConsole=False, appConfigFile="tdPyInterface.ini")
    session = udaExec.connect(dsn)
    rows = [] # to store rows list
    cols = []

    qry = "select top 10000 cast(JSON_Compose(" + get_columns_str(table_name) + ") AS CLOB(500K)) PAYLOAD from " + table_name
    # print(qry)
    cursor = session.execute(qry)

    try:
        while True:
            row = cursor.fetchone().values
            rows.append(json.loads(row[0].strip())) # to convert str row to dict
            if row is None:
                break
    except Exception as e:  # to handle None
        # print('Caught exception: ' + str(e))
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

    # TODO: Chunk payload to {chunk_size} rows each request

    # src_data = list()
    src_data = fetch_table_data_json(src_table)
    # print(src_data)

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

    dsn = 'TDDB' #'TDDEV', 'TDDB'
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
        de.columns = get_columns(table_name)

        # de.columns = [
        #     {"Name": "Field1"},
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
            print("Warning: DE exists. Aborting Create.")

    except Exception as e:
        print('Caught exception: ' + str(e))
        print(e)


if __name__ == '__main__':

    # fix for SSL CERTIFICATE_VERIFY_FAILED exception  FuelSDK
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
            getattr(ssl, '_create_unverified_context', None)):
        ssl._create_default_https_context = ssl._create_unverified_context

    table_name='cmdm.cmdm_product_d'
    target_de = 'DE_API_SAT_007'
    chunk_size = 1000

    start_time = time.time()

    de_create(target_de)
    # de_load_async(table_name, target_de, chunk_size)

    # load_de('cmdm.cmdm_product_d','DE_API_SAT_005')
    # fetch_table_data_json('cmdm.cmdm_product_d')
    #get_columns_str('cmdm.cmdm_product_d')
    # request_token()

    print("--- %s seconds ---" % (time.time() - start_time))
