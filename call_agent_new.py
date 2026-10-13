import time,datetime
from freeswitchESL import ESL
import pymongo
from config import mongo_url

# Define a SessionManager class (as shown earlier)
class SessionManager:
    def __init__(self):
        self.sessions = {}

    def create_session(self, key, data):
        expiration_time = datetime.datetime.now() + datetime.timedelta(hours=1)
        self.sessions[key] = {"data": data, "expiration_time": expiration_time}

    def get_session(self, key):
        session = self.sessions.get(key)
        if session and session["expiration_time"] > datetime.datetime.now():
            return session["data"]
        else:
            # Session has expired or doesn't exist
            return None

    def delete_session(self, key):
        if key in self.sessions:
            del self.sessions[key]
   

# Initialize the session manager
session_manager = SessionManager()

# Function to connect to FreeSWITCH
def connect_to_freeswitch():
    con = session_manager.get_session("freeswitch_connection")
    if con is None or not con.connected():
        con = ESL.ESLconnection("127.0.0.1", "8021", "ClueCon")
        if con.connected():
            print("Successfully connected to FreeSWITCH!")
            session_manager.create_session("freeswitch_connection", con)
        else:
            print("Failed to connect to FreeSWITCH")
    return con

# Function to connect to MongoDB
def connect_to_mongodb():
    con = session_manager.get_session("mongodb_connection")
    if con is None:
        try:
            myclient = pymongo.MongoClient(mongo_url)
            mydb = myclient["admin"]
            auto_originate_collection = mydb['auto_campaign_originate']
            session_manager.create_session("mongodb_connection", auto_originate_collection)
            return auto_originate_collection
        except Exception as e:
            print("Failed to connect to MongoDB:", str(e))
            return None
    return con

# ...

# Main loop
while True:
    # Get or establish FreeSWITCH connection
    freeswitch_connection = connect_to_freeswitch()

    # Get or establish MongoDB connection
    mongodb_connection = connect_to_mongodb()

    freeswitch_connection.events("plain", "ALL")
    while True:
        event = freeswitch_connection.recvEvent();
        #print(event.serialize())
        if event.getHeader("Event-Name") == "CHANNEL_ANSWER" and event.getHeader("variable_sip_h_P-auto_campaign") == 'true' and event.getHeader("variable_sip_h_P-amd_status") == 'true':
    	    auto_campaign_originate = event.getHeader('variable_sip_h_P-auto_campaign_originate')
    	    if auto_campaign_originate and auto_campaign_originate != "":
    		    print("CAMAPAING FIND RETURE")
    	    else:   
    		    print(event.serialize())
    		    createtimestr = event.getHeader("Caller-Channel-Created-Time")
    		    progressmediatime_str = event.getHeader("Caller-Channel-Progress-Media-Time")
    		    progresstime_str = event.getHeader("Caller-Channel-Progress-Time")
    		    answertime_str = event.getHeader('Caller-Channel-Answered-Time')
    		    createtime = datetime.fromtimestamp(int(createtimestr) / 1000000)
    		    answertime = datetime.fromtimestamp(int(answertime_str) / 1000000)
    		    progressmediatime = datetime.fromtimestamp(int(progressmediatime_str) / 1000000)
    		    progresstime = datetime.fromtimestamp(int(progresstime_str) / 1000000)
    		    print("create time",createtime)
    		    print("answer time",answertime)
    		    print("progrees time",progresstime)
    		    print("progress media time",progressmediatime)
    		    if int(progresstime_str) == 0 and int(progressmediatime_str) == 0:
    			    print("no media ringing time and ringing")
    			    seconds_difference = (answertime - createtime).total_seconds()
    			    print("create",seconds_difference)
    		    elif int(progresstime_str) != 0 and int(progressmediatime_str) == 0:
    			    print("no media ringing time")
    			    seconds_difference = (answertime - progresstime).total_seconds()
    			    print("ringing ", seconds_difference)
    		    else:
    			    print("media found")
    			    seconds_difference = (answertime - progressmediatime).total_seconds()
    			    print("progress seconds difference",seconds_difference)
    		    flag = False
    		    myArray = [
    				    [0.0,1.50],
		 	    ]
    		    for element in myArray:
    			    if (seconds_difference > element[0] and seconds_difference < element[1]) or (seconds_difference < 1):
    				    print("hangup element======================", element)
    				    flag = True
    				    break  # No need to continue checking once the condition is met    			
    		    if flag:
    			    print("flag is true")
    			    hangup_command = f'uuid_kill {event.getHeader("Unique-ID")} INCOMPATIBLE_DESTINATION'
    			    con.api(hangup_command)
    			    print("hangup commanf ========>",hangup_command)
    			    if event.getHeader('variable_sip_h_P-campaign_uuid') != "" and event.getHeader('variable_sip_h_P-user_uuid') != "":
    				    print("found the values")
    				    filter = {"campaign_uuid": event.getHeader("variable_sip_h_P-campaign_uuid"),"agent_uuid":event.getHeader("variable_sip_h_P-user_uuid")}
    				    update = {"$set": {"flag":'0'}}
    				    auto_originate_collection.update_one(filter,update)
    		    else:
    			    print("flag is false")
        if event.getHeader("Event-Name") == "CHANNEL_HOLD":
            print(event.serialize())


    # Sleep for one second before the next iteration
    time.sleep(1)

