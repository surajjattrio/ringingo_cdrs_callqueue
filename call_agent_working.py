from freeswitchESL import ESL
from datetime import datetime
import pymongo
import json,sys,uuid,time
import urllib.parse
import re
from config import mongo_url

def word_exists(text, word):
    return bool(re.search(r'\b' + re.escape(word) + r'\b', text))

def get_header(event, key):
    """Cache-friendly header getter"""
    return event.getHeader(key) or ""

# create an ESL connection to FreeSWITCH
con = ESL.ESLconnection("127.0.0.1", "8021", "ClueCon")

# authenticate with FreeSWITCH
if con.connected():
    print("Successfully connected to FreeSWITCH!")
else:
    print("Failed to connect to FreeSWITCH")
    
# Use connection pooling
myclient = pymongo.MongoClient(
    mongo_url,
    maxPoolSize=10,
    minPoolSize=2
)
mydb = myclient["db_pbxcc"]
auto_originate_collection = mydb['auto_campaign_originate']
realtime_report_collection = mydb['realtime_report_count_mgmt']
inbound_report_collection = mydb['inbound_realtime_count_mgmt']
manager_realtime_collection = mydb['manager_realtime_count_mgmt']
manager_inbound_realtime_collection = mydb['manager_inbound_realtime_count_mgmt']

#con.events('plain', 'CHANNEL_CREATE')
#con.events('plain', 'CHANNEL_CREATE CHANNEL_EXECUTE CHANNEL_DESTROY CHANNEL_ANSWER CHANNEL_HANGUP')
con.events("plain", "ALL")

while True:
    event = con.recvEvent()
    event_name = get_header(event, "Event-Name")
    
    # Remove: print(event.serialize()) - this wastes memory
    
    # CHANNEL_EXECUTE handler
    if event_name == "CHANNEL_EXECUTE" and get_header(event, "variable_sip_h_X-inbound_campaign_flag") == 'true':
        lead_uuid = get_header(event, "variable_sip_h_X-Leaduuid")
        tenant_uuid = get_header(event, "variable_sip_h_X-tenant_uuid")
        if lead_uuid and tenant_uuid and get_header(event, "Application") == 'callcenter':
            if not inbound_report_collection.find_one({"lead_uuid": lead_uuid}):
                inbound_report_collection.insert_one({
                    "uuid": str(uuid.uuid4()),
                    "lead_uuid": lead_uuid,
                    "tenant_uuid": tenant_uuid,
                    "type": 1
                })
    
    # CHANNEL_CREATE handler
    if event_name == "CHANNEL_CREATE" and get_header(event, "variable_sip_h_X-inbound_campaign_flag") == 'true':
        lead_uuid = get_header(event, "variable_sip_h_X-Leaduuid")
        if lead_uuid and get_header(event, "variable_sip_h_X-tenant_uuid"):
            existing = inbound_report_collection.find_one({"lead_uuid": lead_uuid})
            if existing:
                inbound_report_collection.update_one({"lead_uuid": lead_uuid}, {"$set": {"type": 0}})
    
    # Hangup handlers - cache UUIDs
    if event_name == "CUSTOM":
        event_subclass = get_header(event, "Event-Subclass")
        if event_subclass in ["X_FER_HANGUP", "A_LEG_HANGUP"]:
            conference_uuid = get_header(event, "conference_uuid")
            if conference_uuid:
                con.api(f'uuid_kill {conference_uuid}')
    
    # THREE WAY / attended_xfer handler - optimize with early exit
    if ((event_name == "CHANNEL_EXECUTE" and get_header(event, "variable_current_application") == 'three_way') or 
        (event_name == "CALL_UPDATE" and get_header(event, "Other-Leg-Destination-Number") == 'attended_xfer')):
        
        try:
            response = con.api('show registrations as json')
            parsed_data = json.loads(response.getBody())
            rows = parsed_data.get("rows", [])
            
            bridge_variable = urllib.parse.unquote(get_header(event, "Other-Leg-Callee-ID-Number"))
            check_var = get_header(event, "variable_original_destination_number")
            if get_header(event, "Other-Leg-Direction") == 'inbound':
                check_var = get_header(event, "Other-Leg-Username")
            
            register_user = None
            final_string = None
            
            for row in rows:
                if word_exists(row.get("reg_user", ""), check_var):
                    register_user = row.get("reg_user")
                    final_string = f"sip:{row.get('reg_user')}@{row.get('network_ip')}:{row.get('network_port')};transport=wss"
                    break  # Early exit once found
            
            if register_user and final_string:
                sip_from_uri = f"sip:{get_header(event, 'variable_sip_from_uri')}"
                x_fer_uuid = get_header(event, "Unique-ID")
                a_leg_call_uuid = get_header(event, "variable_sip_h_X-a_leg_call_uuid")
                
                evt = ESL.ESLevent("SWITCH_EVENT_NOTIFY")
                evt.addHeader("profile", 'external')
                evt.addHeader("from-uri", sip_from_uri)
                evt.addHeader("to-uri", final_string)
                evt.addHeader("user", "202")
                evt.addHeader("host", get_header(event, "variable_sip_from_host"))
                evt.addHeader("X-XFER-UUID", x_fer_uuid)
                evt.addHeader("extra-headers", f"X-XFER-UUID:{x_fer_uuid},X-A-LEG-UUID:{a_leg_call_uuid}")
                con.sendEvent(evt)
                del evt  # Explicit cleanup
        except (json.JSONDecodeError, KeyError):
            pass  # Silently skip on parse errors
        finally:
            del parsed_data, rows  # Release memory
    
    # CHANNEL_ORIGINATE handler
    if event_name == "CHANNEL_ORIGINATE" and get_header(event, "variable_sip_h_P-auto_campaign") == 'true':
        campaign_uuid = get_header(event, "variable_sip_h_P-campaign_uuid")
        tenant_uuid = get_header(event, "variable_sip_h_P-tenant_uuid")
        lead_uuid = get_header(event, "variable_sip_h_P-lead_uuid")
        callstart = get_header(event, "variable_sip_h_P-call_start")
        manager_uuid = get_header(event, "variable_sip_h_P-manager_uuid")
        
        if not realtime_report_collection.find_one({"lead_uuid": lead_uuid}):
            insert_doc = {
                "uuid": str(uuid.uuid4()),
                "lead_uuid": lead_uuid,
                "campaign_uuid": campaign_uuid,
                "tenant_uuid": tenant_uuid,
                "type": 3,
                "created_at": callstart
            }
            if manager_uuid:
                insert_doc["manager_uuid"] = manager_uuid
            realtime_report_collection.insert_one(insert_doc)
    
    # CHANNEL_PROGRESS handler
    if event_name == "CHANNEL_PROGRESS" and get_header(event, "variable_sip_h_P-auto_campaign") == 'true':
        lead_uuid = get_header(event, "variable_sip_h_P-lead_uuid")
        if lead_uuid and realtime_report_collection.find_one({"lead_uuid": lead_uuid}):
            realtime_report_collection.update_one({"lead_uuid": lead_uuid}, {"$set": {"type": 0}})
    
    # CHANNEL_ANSWER handler - complex logic optimized
    if event_name == "CHANNEL_ANSWER":
        auto_campaign = get_header(event, "variable_sip_h_P-auto_campaign")
        inbound_flag = get_header(event, "variable_sip_h_X-inbound_campaign_flag")
        
        if auto_campaign == 'true':
            # Start recording logic
            auto_campaign_originate = get_header(event, "variable_sip_h_P-auto_campaign_originate")
            auto_campaign_recording = get_header(event, "variable_custom_recording")
            destination_number = get_header(event, "Other-Leg-Destination-Number")
            call_uuid = get_header(event, "variable_uuid")
            recording_path = get_header(event, "variable_recording_directory")
            
            print(f"[CHANNEL_ANSWER] Recording check: auto_campaign_originate={auto_campaign_originate}, auto_campaign_recording={auto_campaign_recording}")
            
            # Start recording if conditions met
            if (auto_campaign_originate and auto_campaign_recording == '0' and 
                call_uuid and destination_number and recording_path):
                recording_file = f"{recording_path}/{call_uuid}.wav"
                # recording_file = f"{recording_path}/{destination_number}_{call_uuid}.wav"  # ::yaksh::
                recording_command = f'uuid_record {call_uuid} start {recording_file}'
                print(f"[RECORDING START] Command: {recording_command}")
                con.api(recording_command)
                print(f"[RECORDING] Started for call_uuid: {call_uuid}")
            
            # AMD detection logic
            amd_status = get_header(event, "variable_sip_h_P-amd_status")
            if amd_status == 'true' and not auto_campaign_originate:
                print("[AMD] Processing AMD detection logic")
                try:
                    progresstime_str = get_header(event, "Caller-Channel-Progress-Time")
                    progressmediatetime_str = get_header(event, "Caller-Channel-Progress-Media-Time")
                    answertime_str = get_header(event, "Caller-Channel-Answered-Time")
                    createtimestr = get_header(event, "Caller-Channel-Created-Time")
                    
                    if not all([progresstime_str, progressmediatime_str, answertime_str, createtimestr]):
                        print("[AMD] Missing timestamp data, skipping AMD detection")
                    else:
                        answertime = datetime.fromtimestamp(int(answertime_str) / 1000000)
                        createtime = datetime.fromtimestamp(int(createtimestr) / 1000000)
                        progresstime = datetime.fromtimestamp(int(progresstime_str) / 1000000)
                        progressmediatime = datetime.fromtimestamp(int(progressmediatetime_str) / 1000000)
                        
                        if int(progressmediatime_str) > 0:
                            seconds_difference = (answertime - progressmediatime).total_seconds()
                            print(f"[AMD] Using progress media time: {seconds_difference}s")
                        elif int(progresstime_str) > 0:
                            seconds_difference = (answertime - progresstime).total_seconds()
                            print(f"[AMD] Using progress time: {seconds_difference}s")
                        else:
                            seconds_difference = (answertime - createtime).total_seconds()
                            print(f"[AMD] Using create time: {seconds_difference}s")
                        
                        # Check AMD hangup thresholds
                        hangup_ranges = [[0.0, 1.50], [23.00, 24.00], [30.00, 31.00]]
                        should_hangup = any(start < seconds_difference < end for start, end in hangup_ranges)
                        
                        print(f"[AMD] Seconds difference: {seconds_difference}, Should hangup: {should_hangup}")
                        
                        if should_hangup:
                            campaign_uuid = get_header(event, "variable_sip_h_P-campaign_uuid")
                            user_uuid = get_header(event, "variable_sip_h_P-user_uuid")
                            if campaign_uuid and user_uuid:
                                print(f"[AMD] Setting flag=0 for campaign_uuid={campaign_uuid}, user_uuid={user_uuid}")
                                auto_originate_collection.update_one(
                                    {"campaign_uuid": campaign_uuid, "agent_uuid": user_uuid},
                                    {"$set": {"flag": '0'}}
                                )
                except (ValueError, TypeError) as e:
                    print(f"[AMD] Error in timestamp parsing: {e}")
            
            # Update realtime report
            lead_uuid = get_header(event, "variable_sip_h_P-lead_uuid")
            if lead_uuid:
                if realtime_report_collection.find_one({"lead_uuid": lead_uuid}):
                    realtime_report_collection.update_one({"lead_uuid": lead_uuid}, {"$set": {"type": 2}})
                    print(f"[REALTIME] Updated realtime_report type=2 for lead_uuid={lead_uuid}")
            
            # Update manager realtime if applicable
            manager_uuid = get_header(event, "variable_sip_h_P-manager_uuid")
            if manager_uuid and lead_uuid:
                if manager_realtime_collection.find_one({"lead_uuid": lead_uuid}):
                    manager_realtime_collection.update_one({"lead_uuid": lead_uuid}, {"$set": {"type": 2}})
                    print(f"[MANAGER_REALTIME] Updated manager_realtime type=2 for lead_uuid={lead_uuid}")
            
            # Set agent state
            username = get_header(event, "variable_sip_h_P-username")
            tenant_domain = get_header(event, "variable_sip_h_P-tenant_domain")
            if username and tenant_domain:
                api_command = f"callcenter_config agent set state {username}.{tenant_domain}@default 'In a queue call'"
                con.api(api_command)
                print(f"[AGENT_STATE] Set agent to 'In a queue call': {api_command}")
        
        if inbound_flag == 'true':
            lead_uuid = get_header(event, "variable_sip_h_X-Leaduuid")
            if lead_uuid and inbound_report_collection.find_one({"lead_uuid": lead_uuid}):
                inbound_report_collection.update_one({"lead_uuid": lead_uuid}, {"$set": {"type": 2}})
                print(f"[INBOUND] Updated inbound_report type=2 for lead_uuid={lead_uuid}")
    
    # CHANNEL_DESTROY / HANGUP handler
    if event_name in ["CHANNEL_DESTROY", "CHANNEL_HANGUP", "CHANNEL_HANGUP_COMPLETE"]:
        print(f"[HANGUP] Event: {event_name}")
       
        if get_header(event, "variable_sip_h_P-auto_campaign") == 'true':
            # Stop recording logic
            auto_campaign_originate = get_header(event, "variable_sip_h_P-auto_campaign_originate")
            auto_campaign_recording = get_header(event, "variable_custom_recording")
            destination_number = get_header(event, "Other-Leg-Destination-Number")
            recording_path = get_header(event, "variable_recording_directory")
            call_uuid = get_header(event, "variable_uuid")
            
            print(f"[HANGUP_RECORDING] Check: auto_campaign_originate={auto_campaign_originate}, auto_campaign_recording={auto_campaign_recording}")
            
            # Stop recording if conditions met
            if (auto_campaign_originate and auto_campaign_recording == '0' and 
                destination_number and call_uuid and recording_path):
                recording_file = f"{recording_path}/{call_uuid}.wav"
                # recording_file = f"{recording_path}/{destination_number}_{call_uuid}.wav"  # ::yaksh::
                recording_command = f'uuid_record {call_uuid} stop {recording_file}'
                print(f"[RECORDING STOP] Command: {recording_command}")
                con.api(recording_command)
                print(f"[RECORDING] Stopped for call_uuid: {call_uuid}")
            
            # Set agent state to Idle
            username = get_header(event, "variable_sip_h_P-username")
            tenant_domain = get_header(event, "variable_sip_h_P-tenant_domain")
            if username and tenant_domain:
                api_command = f"callcenter_config agent set state {username}.{tenant_domain}@default 'Idle'"
                con.api(api_command)
                print(f"[AGENT_STATE] Set agent to 'Idle': {api_command}")
            
            # Delete from realtime report
            lead_uuid = get_header(event, "variable_sip_h_P-lead_uuid")
            if lead_uuid:
                realtime_report_collection.delete_one({"lead_uuid": lead_uuid})
                print(f"[REALTIME] Deleted realtime_report for lead_uuid={lead_uuid}")
        
        if get_header(event, "variable_sip_h_X-inbound_campaign_flag") == 'true':
            lead_uuid = get_header(event, "variable_sip_h_X-Leaduuid")
            if lead_uuid:
                inbound_report_collection.delete_one({"lead_uuid": lead_uuid})
                print(f"[INBOUND] Deleted inbound_report for lead_uuid={lead_uuid}")

            # Set agent state to Idle (::yaksh::)
            username = get_header(event, "variable_sip_h_P-username")
            tenant_domain = get_header(event, "variable_sip_h_P-tenant_domain")
            print(f"[INBOUND] user anme and tenat fomainnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn {username} mmmmmmmmmmmmmmmmmm {tenant_domain}")
            if username and tenant_domain:
                api_command = f"callcenter_config agent set state {username}.{tenant_domain}@default 'Idle'"
                con.api(api_command)
                print(f"[AGENT_STATE] Set agent to 'Idle': {api_command}")
    
    # BEEP_DETECTED handler
    if event_name == "CUSTOM" and get_header(event, "Beep-Status") == "DETECTED":
        unique_id = get_header(event, "Unique-ID")
        if unique_id:
            hangup_command = f'uuid_kill {unique_id}'
            con.api(hangup_command)
            print(f"[BEEP] Hangup command sent: {hangup_command}")
