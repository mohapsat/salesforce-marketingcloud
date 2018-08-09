# Add a require statement to reference the Fuel SDK's functionality:
import FuelSDK

# Next, create an instance of the ET_Client class:
myClient = FuelSDK.ET_Client()

# Create an instance of the object type we want to work with:
list = FuelSDK.ET_List()

# Associate the ET_Client to the object using the auth_stub property:
list.auth_stub = myClient

# Utilize one of the ET_List methods:
response = list.get()

# Print out the results for viewing
print('Post Status: ' + str(response.status))
print('Code: ' + str(response.code))
print('Message: ' + str(response.message))
print('Result Count: ' + str(len(response.results)))
print('Results: ' + str(response.results))