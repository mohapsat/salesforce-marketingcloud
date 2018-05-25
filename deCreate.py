# Sample config.python
    # [Web Services]
    # appsignature: ETL_API_GEN
    # clientid: zpv5xxxxxxxxxxxxxxxxxx
    # clientsecret: VMGYxxxxxxxxxxxxxxx
    # defaultwsdl: http://webservice.exacttarget.com/etframework.wsdl
    # authenticationurl: https://auth.exacttargetapis.com/v1/requestToken?legacy=1

# Sample tdPyInterface.ini
    ## Application Configuration
    # [CONFIG]
    # appName=CheckPoints
    # version=1
    # logConsole=False
    # dataSourceName=TDDEV
    # table=DBC.DBCInfo
    #
    # # Default Data Source Configuration
    # [DEFAULT]
    # method=odbc
    # charset=UTF8
    #
    # # Data Source Definition
    # [TDDEV]
    # dsn=TDDEV
    # username=crmp_su
    # password=****
    #
    # [TDDB]
    # dsn=TDDB
    # username=crmp_su
    # password=*******

import FuelSDK as f
import teradata
import time


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
        #need to append a copy, otherwise you are just adding references to the same dictionary over and over again

        # print("table_data_list = %s"% table_data_list)
    # print(table_data_list)
    return table_data_list


def get_columns(table_name):

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
    tbl_cols = [] #to store cols in expected format
    for x in cols:
        tbl_cols.append({"Name": x.strip()})

    return tbl_cols
    # pass


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


def de_create(table_name, de_name):
    try:
        debug = False
        stubObj = f.ET_Client(False, debug)
        DE_NAME = de_name
        FOLDER_ID = 502 #API_GEN

    # Create  Data Extension
    #     print('Creating Data Extension %s'% DE_NAME)
        de = f.ET_DataExtension()
        de.auth_stub = stubObj
        de.props = {"Name": DE_NAME, "CustomerKey": DE_NAME, "CategoryID": FOLDER_ID}
        de.columns = get_columns(table_name)

        # de.columns = [
        #     {"Name": "Field1"},
        #     {"Name": "Field2"},
        #     {"Name": "Field3"},
        #     {"Name": "Field4"},
        #     {"Name": "Field5"},
        # ]

        properties = de.props
        de.search_filter = {'Property': 'CustomerKey', 'SimpleOperator': 'equals', 'Value': DE_NAME}
        filter = de.search_filter
        de_exists = de.get(properties,filter)

        if len(de_exists.results) == 0: # If DE does not exist, post
            post_response = de.post()
            # insert a row into the de
            load_de(table_name)
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
    start_time = time.time()
    # de_create('cmdm.cmdm_user_d','DE_API_SAT_006')
    load_de('cmdm.cmdm_product_d','DE_API_SAT_005')
    # fetch_table_data('cmdm.cmdm_product_d')
    print("--- %s seconds ---" % (time.time() - start_time))