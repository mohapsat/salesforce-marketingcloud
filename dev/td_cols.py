import teradata
import json

data = []
vals = []  # to store data
cols = []  # to store cols
master = []  # to store tuples of both
dsn = 'TDDB'

query = "select top 2 USER_SK, USERID from cmdm.cmdm_user_d"

print("SQL ISSUED : %s" % query)

udaExec = teradata.UdaExec(appName="tdPyInterface", version="1.0",
                           logConsole=False, appConfigFile="tdPyInterface.ini")

session = udaExec.connect(dsn)
cursor = session.execute(query)

for row in cursor.description:
    cols.append(row[0])

for row in cursor:
    vals.append(str(row))

for item in vals:
    data = item[item.find('[') + 1:item.find(']')]
    parts = data.split(',')
    for x, y in zip(cols, parts):
        master.append({x.strip(): y.strip()})

# print(json.dumps({'data': master}, indent=0))
tbl_cols = []
for x in cols:
    tbl_cols.append({"name":x})


