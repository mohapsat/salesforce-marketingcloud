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


def deQuery(target_de, qry):
    pass

if __name__ == '__main__':

    # fix for SSL CERTIFICATE_VERIFY_FAILED exception  FuelSDK
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
            getattr(ssl, '_create_unverified_context', None)):
        ssl._create_default_https_context = ssl._create_unverified_context

    qry = ''
    target_de = 'DE_TMAIL_COUNTS_SENLOG'

    deQuery(target_de, qry)

    start_time = time.time()

    print("--- %s seconds ---" % (time.time() - start_time))
