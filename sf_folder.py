import FuelSDK as f


debug = False
stubObj = f.ET_Client(False, debug)

getFolder = f.ET_Folder()

getFolder.auth_stub = stubObj

getFolder.props = ["ID","ObjectID"]

response = getFolder.get()
print(response.results[144])



# response.results[144]
# (DataFolder){
#    PartnerKey = None
#    ID = 502
#    ObjectID = "b7284919-f105-4758-a35a-158933210e5c"
#  }