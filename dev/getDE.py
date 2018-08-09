import FuelSDK as f

# try:
debug = False
stubObj = f.ET_Client(False, debug)
DE_NAME = "DE_SAT_001"

# Create  Data Extension
print('>>> Create Data Extension')
de = f.ET_DataExtension()
de.auth_stub = stubObj
# de.props = {"Name", "CustomerKey"}
# de.props = {
#     "Name": "MyDataExtension"
#     , "Description": "My first data extension"
#     , "IsSendable": True
#     , "SendableDataExtensionField": "CustomerID"
#     , "SendableSubscriberField": "_SubscriberKey"
# }
response = de.get()
print(response)

