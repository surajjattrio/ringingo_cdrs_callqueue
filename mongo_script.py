import pymongo
from config import mongo_url
# Connect to MongoDB
myclient = pymongo.MongoClient(mongo_url)

try:
    # Check if the client is connected by calling server_info()
    server_info = myclient.server_info()
    print("Connected to MongoDB server version:", server_info["version"])
except pymongo.errors.ServerSelectionTimeoutError as e:
    print("Failed to connect to MongoDB:", e)

mydb = myclient["db_pbxcc"]
ringgroup_collection = mydb['ring_group']
user_collection = mydb['user']
extension_collection = mydb['extensions']
tenant_collection = mydb['tenant']
all_ringgroups = ringgroup_collection.find()

for ringgroup_info in all_ringgroups:
    extension_list = ringgroup_info.get("extension_list", [])
    tenant = ringgroup_info.get("tenant_uuid")
    ringgroup = ringgroup_info.get("uuid")
    print("ringgroup_uuid",ringgroup)
    new_extension_list = []
    for extension_info in extension_list:
        if extension_info != "":
            extension_type = extension_info.get('extension_type')
            extension = extension_info.get("extension")
            print("Extension:", extension)
            print("Tenant_info",tenant)
            filter = {'uuid':tenant}
            tenant_info = tenant_collection.find_one(filter)
            if tenant_info != None:
                if int(extension_type) == 0:
                    filter = {'username':extension,'tenant_uuid':tenant}
                    extension_table_info = extension_collection.find_one(filter)
                    #print(extension_table_info)
                    if extension_table_info != None:
                        filter = {'default_extension':extension_table_info.get('uuid')}
                        user_info = user_collection.find_one(filter)
                        if user_info != None:
                            extension_array = {
                                'extension_type' :str(extension_type),
                                'extension' : str(extension),
                                'user_uuid' : str(user_info.get('uuid'))
                            }
                else:
                    extension_array = {
                        'extension_type' : str(extension_type),
                        'extension' : str(extension)
                    }
                new_extension_list.append(extension_array)
    if new_extension_list != None:
        print(new_extension_list)
        ringgroup_collection.update_one({"uuid":ringgroup_info.get("uuid")},{"$set":{"extension_list":new_extension_list}})
