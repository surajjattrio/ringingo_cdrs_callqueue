import os, json, pymongo, uuid, time
import requests
from datetime import datetime, timedelta
import datetime
import simplejson as json
from config import mongo_url


# This is my path
path_to_json_files = "/usr/local/freeswitch/log/json_cdr/"
path_to_move_json_files = "/usr/local/freeswitch/log/json_cdr_archive/"

#Mongo Connection
myclient = pymongo.MongoClient(mongo_url)
mydb = myclient["admin"]
cdrs_collection = mydb["cdrs"]
inbound_cdrs_collection = mydb["inbound_cdrs"]
recordings_collection = mydb["recording"]
cdrs_summary_collection = mydb["cdrs_summary"]
extension_collection = mydb["extensions"]

#Json File List
json_file_names = [filename for filename in os.listdir(path_to_json_files) if filename.endswith('.json')]
json_file_names.sort(key=lambda x: os.path.getctime(os.path.join(path_to_json_files, x)))

def manage_drop_call_flag (lead_uuid,hangup_reason):
	print("hangup_reason",hangup_reason)
	lead_mgmt_collection = mydb['lead_management']
	disposition_collection = mydb['disposition']
	if hangup_reason == 'INCOMPATIBLE_DESTINATION':
		filter = {"tenant_uuid": '99fb3e0a-bf23-11ed-afa1-0242ac120002',"code":"AA"}
	elif hangup_reason == 'ALLOTTED_TIMEOUT':
		filter = {"tenant_uuid": '99fb3e0a-bf23-11ed-afa1-0242ac120002',"code":"NA"}
	elif hangup_reason == 'NORMAL_UNSPECIFIED':
		filter = {"tenant_uuid": '99fb3e0a-bf23-11ed-afa1-0242ac120002',"code":"Busy"}
	elif hangup_reason == 'NO_USER_RESPONSE':
		filter = {"tenant_uuid": '99fb3e0a-bf23-11ed-afa1-0242ac120002',"code":"NAVL"}
	else:
		filter = {"tenant_uuid": '99fb3e0a-bf23-11ed-afa1-0242ac120002',"code":"DROP"}
	print("manage_drop_lead_uuid",lead_uuid)
	disposition_collection_data = disposition_collection.find_one(filter)
	disposition_uuid = "";
	if disposition_collection_data != '' and disposition_collection_data != None and disposition_collection_data != 'None':
		disposition_uuid = disposition_collection_data['disposition_uuid']
		print("Update lead status as DROP call for disposition_uuid",disposition_uuid)
		print("Update lead status as DROP call for UUID",lead_uuid)
		filter = {"lead_management_uuid": lead_uuid}
		update = {"$set": {"lead_status": disposition_uuid}}
		lead_mgmt_collection.update_one(filter, update)
	return disposition_uuid
		
		
def manage_lead_originated(variables_value):
	auto_lead_uuid  = variables_value['sip_h_P-lead_uuid'] if 'sip_h_P-lead_uuid' in variables_value else "";
	lead_originate = mydb["lead_originate"]
	lead_originate_archive = mydb["lead_originate_archive"]
	filter = {"lead_management_uuid": auto_lead_uuid}
	print("auto_lead_uuid",auto_lead_uuid)	
	document_to_copy = lead_originate.find_one(filter)
	if document_to_copy != '' and document_to_copy != None and document_to_copy != 'None':
		print(document_to_copy)
		print(":::::document_to_copy")
		lead_originate_archive.insert_one(document_to_copy)
		lead_originate.delete_many(filter)

def manage_auto_campaign(variables_value,auto_campaign_originate_uuid):
#	for variables_key in variables_value:
#		print(variables_key,"::HARSH:",variables_value[variables_key])
	auto_campaign_originate = mydb["auto_campaign_originate"]
	filter = {"uuid": auto_campaign_originate_uuid}
	update = {"$set": {"flag": "0"}}
	auto_campaign_originate.update_one(filter, update)

def check_number_masking(campaign_uuid):
	outbound_campaign = mydb['outbound_campaign']
	filter = {'uuid':campaign_uuid}
	result_outbound_campaign = outbound_campaign.find_one(filter)
	print(result_outbound_campaign)
	print("result of outbound campaign")
	return result_outbound_campaign
	
def manage_lead_retries(variables_value,campaign_uuid):
	lead_uuid = variables_value['sip_h_X-Leaduuid'] if 'sip_h_X-Leaduuid' in variables_value else '';
	outbound_campaign_db = mydb['outbound_campaign']
	filter = {'uuid':campaign_uuid}
	outbound_campaign = outbound_campaign_db.find_one(filter)
	print(outbound_campaign)
	if outbound_campaign is not None:
		max_retries = outbound_campaign['max_retries']
		retries_time = outbound_campaign['retries_time']
		print(max_retries)
		print(retries_time)
		current_date = datetime.now()
		new_date = current_date + timedelta(minutes=int(retries_time))
		iso_date = f'ISODate({new_date.isoformat()}Z)'	
		leads_in_queue_db = mydb['leads_in_queue'] 
		filter = {'campaign_uuid':campaign_uuid,'lead_management_uuid':lead_uuid}
		lead_in_queue =  leads_in_queue_db.find_one(filter)
		print(lead_in_queue)
		if lead_in_queue is not None:
			if 'lead_resent_count' in lead_in_queue:
				if int(lead_in_queue['lead_resent_count']) < int(max_retries):
					lead_type = int(lead_in_queue['lead_resent_count']) + 1
					if int(lead_type) == int(max_retries):
						filter = {"campaign_uuid":campaign_uuid,"lead_management_uuid":lead_uuid}
						leads_in_queue_db.delete_one(filter)
					else:
						filter = {"campaign_uuid":campaign_uuid,"lead_management_uuid":lead_uuid}
						update = {"$set": {"lead_resent_count": lead_type,"lead_sent":1,'resend_lead_time':iso_date}}
						leads_in_queue_db.update_one(filter,update)
			else:
				filter = {"campaign_uuid":campaign_uuid,"lead_management_uuid":lead_uuid}
				update = {"$set": {"lead_resent_count": 1,"lead_sent":1,"resend_lead_time":iso_date}}
				leads_in_queue_db.update_one(filter,update)
				print("updated successfully")
			
def lead_entry_management(destination_number,user_uuid,tenant_uuid):
	print("\n","here4:::")
	if destination_number.startswith("+"):
		dest_number = destination_number[1:]
	else:
		dest_number = destination_number
	tenant_collection = mydb['tenant']
	filter = {"uuid":tenant_uuid}
	tenant_info = tenant_collection.find_one(filter)
	lead_mgmt_collection = mydb['lead_management']
	lead_uuid = uuid.uuid4()
	new_lead_uuid = str(lead_uuid)
	filter = {"phone_number": dest_number,"tenant_uuid":tenant_uuid}
	lead_mgmt = lead_mgmt_collection.find_one(filter)
	if not lead_mgmt:
		if str(tenant_info['add_anonymous_lead']) != '1':
			insert_lead_mgmt_string = {"address": '',"state": '',"province": '',"postal_code": '',"dob": '',"phone_code": '',"alternate_phone_number": '',"email": '',"status": '0',"gender": '0',"created_flag": '2',"country_uuid": '',"city": '',"first_name":"Anonymous","last_name":"","phone_number":dest_number,"user_uuid":user_uuid,"tenant_uuid":tenant_uuid,"lead_management_uuid":new_lead_uuid, "lead_status": '3809f32d-9096-406b-b2ff-1207b3fbfbc5'}
			lead_mgmt_collection.insert_one(insert_lead_mgmt_string)
			print(insert_lead_mgmt_string)
			return new_lead_uuid
		else:
			return ""
	else:
		return lead_mgmt['lead_management_uuid']

try:
	#For Loop for All File
	for json_file_name in json_file_names:
	    print("\n","Jsom File parsing Start for:",json_file_name)
	    file_pass_flag = 0
	    with open(os.path.join(path_to_json_files, json_file_name)) as json_file:
	    	loaded_json = json.load(json_file)
	    	for key in loaded_json:
	    		if key == 'variables':
	    			#print(key,":::",loaded_json[key])
	    			json_str = json.dumps(loaded_json[key])
	    			variables_value = json.loads(json_str)
	    			#Un Comment Below Line for Debug
	    			for variables_key in variables_value:
	    				print(variables_key,"adesh:::",variables_value[variables_key])
	    			auto_campaign = variables_value['sip_h_P-auto_campaign'] if 'sip_h_P-auto_campaign' in variables_value else "";
	    			if auto_campaign != '':
	    				manage_lead_originated(variables_value)
	    			auto_campaign_originate_uuid = variables_value['sip_h_P-auto_campaign_originate'] if 'sip_h_P-auto_campaign_originate' in variables_value else "";
	    			#if auto_campaign_originate_uuid != "":
	    				#manage_auto_campaign(variables_value,auto_campaign_originate_uuid)
				auto_agent_flag = variables_value['sip_h_P-auto_agent_flag'] if 'sip_h_P-auto_agent_flag' in variables_value else "";
				if(auto_agent_flag == "auto_agent"):
					#Remove un-use file.
					file_pass_flag = 1
					os.unlink(path_to_json_files+json_file_name)
					print("\n","Remove un-use file",path_to_json_files+json_file_name)
					continue
				callstart = variables_value['custom_callstart'] if 'custom_callstart' in variables_value else "";
				if callstart == "":
					callstart = variables_value['sip_h_P-callstart'] if 'sip_h_P-callstart' in variables_value else "";
				feature_code_flag = variables_value['feature_code_flag'] if 'feature_code_flag' in variables_value else "false";
				if auto_campaign != "":
					if callstart == "":
						epoch_time = variables_value['end_epoch'] if 'end_epoch' in variables_value else "";
						print(int(epoch_time))
						date_object = datetime.datetime.fromtimestamp(int(epoch_time))
						formatted_date = date_object.strftime('%Y-%m-%d %H:%M:%S')
						callstart = formatted_date
						print("adesh auto campaign callstart ",callstart)
				if feature_code_flag == "true":
					#Remove un-use file.
					file_pass_flag = 1
					os.unlink(path_to_json_files+json_file_name)
					print("\n","FEATURE CODE","Remove un-use file",path_to_json_files+json_file_name)
					continue
					
				if callstart == "":
					#Remove un-use file.
					file_pass_flag = 1
					os.unlink(path_to_json_files+json_file_name)
					print("\n","Remove un-use file",path_to_json_files+json_file_name)
					continue
				pbx_feature = variables_value['pbx_feature'] if 'pbx_feature' in variables_value else "";
				ip_map_uuid = variables_value['ip_map_uuid'] if 'ip_map_uuid' in variables_value else "";
				custom_callid = variables_value['custom_callid'] if 'custom_callid' in variables_value else "";
				if custom_callid == '':
					custom_callid = variables_value['sip_h_X-custom_callid'] if 'sip_h_X-custom_callid' in variables_value else "";
				tenant_uuid = variables_value['tenant_uuid'] if 'tenant_uuid' in variables_value else "";
				if tenant_uuid == '':
					tenant_uuid = variables_value['sip_h_P-tenant_uuid'] if 'sip_h_P-tenant_uuid' in variables_value else "";
				direction = variables_value['direction'] if 'direction' in variables_value else "outbound";
				bridge_uuid = variables_value['bridge_uuid'] if 'bridge_uuid' in variables_value else str(uuid.uuid4());
				user_uuid = variables_value['user_uuid'] if 'user_uuid' in variables_value else "";
				if user_uuid == '':
					user_uuid = variables_value['sip_h_P-user_uuid'] if 'sip_h_P-user_uuid' in variables_value else "";
				authentication_type = variables_value['authentication_type'] if 'authentication_type' in variables_value else "auth";
				extension_uuid = variables_value['extension_uuid'] if 'extension_uuid' in variables_value else "";
				trunk_uuid = variables_value['trunk_uuid'] if 'trunk_uuid' in variables_value else "";
				sip_user = variables_value['sip_user'] if 'sip_user' in variables_value else "";
				#avmd_detect = variables_value['avmd_detect'] if 'avmd_detect' in variables_value else "";
				ivr_group = variables_value['ivr_group_uuid'] if 'ivr_group_uuid' in variables_value else "";
				variables_value['caller_id_name'] = variables_value['caller_id_name'] if 'caller_id_name' in variables_value else "";
				caller_id_name = variables_value['effective_caller_id_name'] if 'effective_caller_id_name' in variables_value else variables_value['caller_id_name'];
				variables_value['caller_id_number'] = variables_value['caller_id_number'] if 'caller_id_number' in variables_value else "";
				caller_id_number = variables_value['effective_caller_id_number'] if 'effective_caller_id_number' in variables_value else variables_value['caller_id_number'];
				variables_value['dialed_user'] = variables_value['dialed_user'] if 'dialed_user' in variables_value else '';
				lead_uuid = variables_value['sip_h_X-Leaduuid'] if 'sip_h_X-Leaduuid' in variables_value else '';
				if lead_uuid == '':
					lead_uuid = variables_value['sip_h_X-lead_uuid'] if 'sip_h_X-lead_uuid' in variables_value else '';
				if lead_uuid == '':
					lead_uuid = variables_value['sip_h_P-lead_uuid'] if 'sip_h_P-lead_uuid' in variables_value else '';
				if lead_uuid == "null":
					lead_uuid = ""
				hangup_reason = variables_value['hangup_cause'] if 'hangup_cause' in variables_value else '';
				print("hangup1",hangup_reason)
				auto_call_flag = "0" # Not Auto call
				hangup_cause = ""
				if auto_campaign != '' and auto_campaign_originate_uuid == '' and lead_uuid != "":
					hangup_cause = manage_drop_call_flag(lead_uuid,hangup_reason)
					auto_call_flag = "2" # Auto call and Drop 
				else:
					auto_call_flag = "1" # Auto call and success
				
				campaign_uuid = variables_value['sip_h_X-selectedcampaignuuid'] if 'sip_h_X-selectedcampaignuuid' in variables_value else '';
				call_stick_status = variables_value['call_stick_status'] if 'call_stick_status' in variables_value else 1;
				#if campaign_uuid == '':
					#campaign_uuid = variables_value['sip_h_P-campaign_uuid'] if 'sip_h_P-campaign_uuid' in variables_value else '';
				if 'custom_destination_number' in variables_value and variables_value['custom_destination_number'] != '':
					destination_number = variables_value['custom_destination_number']
				else:
					destination_number = variables_value['effective_destination_number'] if 'effective_destination_number' in variables_value else variables_value['dialed_user'];
				billsecond = int(variables_value['billsec']) if 'billsec' in variables_value else '0';
				disposition = variables_value['hangup_cause'] if 'hangup_cause' in variables_value else 'NORMAL_CLEARING';
				call_state = 'normal'
				originate_failed_cause = variables_value['originate_failed_cause'] if 'originate_failed_cause' in variables_value else '';
				if originate_failed_cause != '' and originate_failed_cause == 'ORIGINATOR_CANCEL':
					disposition = originate_failed_cause
				if disposition == 'NO_ANSWER' or disposition == 'NO_USER_RESPONSE' or disposition == 'ORIGINATOR_CANCEL' or disposition == 'RECOVERY_ON_TIMER_EXPIRE':
					call_state = 'missed'
				ivr_menu_status = variables_value['ivr_menu_status'] if 'ivr_menu_status' in variables_value else '';
				if ivr_menu_status == 'failure':
					call_state = 'missed'
					disposition = 'IVR_TIMEOUT'
				if ivr_menu_status == 'timeout':
					call_state = 'missed'
					disposition = 'IVR_TIMEOUT'
				cc_cause = variables_value['cc_cause'] if 'cc_cause' in variables_value else '';
				if cc_cause and cc_cause != 'answered':
					call_state = 'missed'
					disposition = 'CALLQUEUE_TIMEOUT'
				if disposition == 'ORIGINATOR_CANCEL':
					call_state_main = 'normal'
				else:
					 call_state_main = call_state
				call_type = variables_value['call_type'] if 'call_type' in variables_value else 'standard';
				if call_type == 'standard' or call_type == 'did':
					direction = 'outbound'
				if 'custom_recording' in variables_value and  variables_value['custom_recording'] == '0':
					if 'current_application' in variables_value and variables_value['current_application'] == 'record_session':
						recording_path = variables_value['current_application_data'] if 'current_application_data' in variables_value else '';
					else:
						execute_on_answer = variables_value['execute_on_answer'] if 'execute_on_answer' in variables_value else '';
						if 'record_session' in execute_on_answer:
							recording_path = execute_on_answer.replace('record_session ', '')
							print('recording_path_in_execute_on_answer',recording_path)
						else:
							recording_path = ''
				else:
					recording_path = ''	
				receiver_extension_uuid = variables_value['receiver_extension_uuid'] if 'receiver_extension_uuid' in variables_value else '';
				originate_disposition = variables_value['originate_disposition'] if 'originate_disposition' in variables_value else '';
				if originate_disposition != '' and (originate_disposition =='NO_ANSWER' or originate_disposition =='USER_BUSY'):
					call_state = 'missed'
					disposition = originate_disposition
					billsecond = 0
				if receiver_extension_uuid != '':
					direction = 'outbound'
				incoming_extra_entry_flag = 0
				call_mode = '0'
				if campaign_uuid != '':
					call_mode = '1';
				number_masking = '1'
				dial_method = ""
				if campaign_uuid != '':
					result_outbound_campaign = check_number_masking(campaign_uuid)
					if result_outbound_campaign is not None:
						number_masking = result_outbound_campaign['number_masking'] if 'number_masking' in result_outbound_campaign else '1';
						dial_method = result_outbound_campaign['dial_method']
				if campaign_uuid != '':
					print(billsecond)
					print(disposition)
					if billsecond == '0' and disposition != 'ORIGINATOR_CANCEL':
						manage_lead_retries(variables_value,campaign_uuid)
				print("\n","here1:::")
				if destination_number != "" and lead_uuid == "":
					print("\n","here2:::")
					if user_uuid != "":
						lead_uuid = lead_entry_management(destination_number,user_uuid,tenant_uuid)
				#if avmd_detect != "":
					#filter = {"code":"AA"}
					#disposition = disposition_collection.find_one(filter)
					#hangup_cause = disposition['disposition_uuid']	        		
				auto_campaign = variables_value['sip_h_P-auto_campaign'] if 'sip_h_P-auto_campaign' in variables_value else "";
				predictive_campaign = variables_value['sip_h_P-predictive_campaign'] if 'sip_h_P-predictive_campaign' in variables_value else "";
				if(predictive_campaign == 'true'):
					if (auto_call_flag =='1'):
						predictive_flag = "0"
					else:
						predictive_flag = "1"
					insert_predictive_campaign = {"agent_uuid":user_uuid,"tenant_uuid":tenant_uuid,"flag":predictive_flag}
					predictive_calculation_collection = mydb["predictive_calculation"]
					predictive_calculation_collection.insert_one(insert_predictive_campaign)
					print("\n","insert_predictive_campaign::::",insert_predictive_campaign)
				if(auto_campaign == 'true'):
					print("\n","auto_campaign CHANGE Destination:::")
					destination_number = variables_value['Caller-Destination-Number'] if 'Caller-Destination-Number' in variables_value else destination_number;
					caller_id_name = variables_value['Hunt-Callee-ID-Name'] if 'Hunt-Callee-ID-Name' in variables_value else caller_id_name;
					caller_id_number = variables_value['Hunt-Orig-Caller-ID-Number'] if 'Hunt-Orig-Caller-ID-Number' in variables_value else caller_id_number;
					number_masking = variables_value['sip_h_P-number_masking'] if 'sip_h_P-number_masking' in variables_value else number_masking;
				if ip_map_uuid != '' or user_uuid == '':
					#direction = 'inbound'
					incoming_extra_entry_flag = 1
					if(variables_value['sip_from_user'] and variables_value['sip_from_user'] != ''):
						caller_id_name = variables_value['sip_from_user']
						caller_id_number = variables_value['sip_from_user']
					insert_inbound_cdr_json = {"uuid": bridge_uuid,"user_uuid":user_uuid,"extension_uuid":extension_uuid,"tenant_uuid":tenant_uuid,"trunk_uuid":trunk_uuid, "sip_user":sip_user,"caller_id_name":caller_id_name,"caller_id_number":caller_id_number,"destination_number":destination_number, "billsecond":billsecond,"disposition":disposition,"direction": direction,"call_type":call_type, "callstart":callstart,"recording_path":recording_path,"pbx_feature":pbx_feature,"authentication_type":authentication_type, "custom_callid":custom_callid,"ip_map_uuid":ip_map_uuid,"call_state":call_state_main,"lead_uuid":lead_uuid,"campaign_uuid":campaign_uuid,"call_mode":call_mode,"hangup_cause":hangup_cause,"auto_call_flag":auto_call_flag,"ivr_group":ivr_group }
					inbound_cdrs_collection.insert_one(insert_inbound_cdr_json)
					print("\n","Inbound CDR Insert::::",insert_inbound_cdr_json)
					if tenant_uuid == 'b4cebcbe-8ff6-4a8f-8ff4-9d99847f22bf':
						url = "https://hook.eu1.make.com/rrcunnlep3j7lfsukd6v41qi29p47axs"
						web_hook_data = {'description': 'incoming calls notification from Belsmart', 'caller_id_name': caller_id_name, 'caller_id_number': caller_id_number, 'did': destination_number, 'date': callstart, 'duration': billsecond,'call_status':call_state,'answered_call':variables_value['dialed_user']}
						print("\n","response_webhooks::::",web_hook_data)
						headers = {'Content-type': 'application/json'}
						response_webhooks = requests.post(url, data=json.dumps(web_hook_data), headers=headers)
						print("\n","response_webhooks::::",response_webhooks)
					if tenant_uuid == 'd615aa9c-5e2d-45d3-a721-a22b3b62a9ec' and destination_number == '00918069982776':
						url = "https://hook.eu1.make.com/93frwd3fh8kldc6x1kdm9ucmijeyo68x"
						web_hook_data = {'description': 'incoming calls notification from Belsmart', 'caller_id_name': caller_id_name, 'caller_id_number': caller_id_number, 'did': destination_number, 'date': callstart, 'duration': billsecond,'call_status':call_state,'answered_call':variables_value['dialed_user']}
						print("\n","response_webhooks::::",web_hook_data)
						headers = {'Content-type': 'application/json'}
						response_webhooks = requests.post(url, data=json.dumps(web_hook_data), headers=headers)
						print("\n","response_webhooks::::",response_webhooks)
						
				else:
					did_pstn = ''
					print("\n","CDRS")
					insert_cdr_json = {"uuid": bridge_uuid,"user_uuid":user_uuid,"extension_uuid":extension_uuid,"tenant_uuid":tenant_uuid,"trunk_uuid":trunk_uuid, "sip_user":sip_user,"caller_id_name":caller_id_name,"caller_id_number":caller_id_number,"destination_number":destination_number, "billsecond":billsecond,"disposition":disposition,"direction": direction,"call_type":call_type, "callstart":callstart,"recording_path":recording_path,"pbx_feature":pbx_feature,"authentication_type":authentication_type, "custom_callid":custom_callid,"ip_map_uuid":ip_map_uuid,"call_state":call_state_main,"lead_uuid":lead_uuid,"campaign_uuid":campaign_uuid,"call_mode":call_mode,"number_masking":number_masking,"dial_method":dial_method,"hangup_cause":hangup_cause,"auto_call_flag":auto_call_flag,"ivr_group":ivr_group }
					print("\n","4harsh CDR Insert::::",insert_cdr_json)
					cdrs_collection.insert_one(insert_cdr_json)
				did_pstn = variables_value['did_pstn'] if 'did_pstn' in variables_value else '';
				if (disposition == 'IVR_TIMEOUT' or disposition == 'CALLQUEUE_TIMEOUT' or did_pstn != '' or ivr_menu_status != ''):
					bridge_uuid = bridge_uuid+"incoming"
					direction = 'inbound'
					insert_cdr_json = {"uuid": bridge_uuid,"user_uuid":user_uuid,"extension_uuid":extension_uuid,"tenant_uuid":tenant_uuid,"trunk_uuid":trunk_uuid, "sip_user":sip_user,"caller_id_name":caller_id_name,"caller_id_number":caller_id_number,"destination_number":destination_number, "billsecond":billsecond,"disposition":disposition,"direction": direction,"call_type":call_type, "callstart":callstart,"recording_path":recording_path,"pbx_feature":pbx_feature,"authentication_type":authentication_type, "custom_callid":custom_callid,"ip_map_uuid":ip_map_uuid,"call_state":call_state_main,"lead_uuid":lead_uuid,"campaign_uuid":campaign_uuid,"call_mode":call_mode,"number_masking":number_masking,"dial_method":dial_method,"hangup_cause":hangup_cause,"auto_call_flag":auto_call_flag,"ivr_group":ivr_group }
					print("\n","3harsh CDR FOR INBOUND FAIL OR DIRECT OUTBOUND Insert::::",insert_cdr_json)
					cdrs_collection.insert_one(insert_cdr_json)

				#Insert Recording entry if have.
				if recording_path != '':
					insert_recoding_json = {"uuid": bridge_uuid,"user_uuid":user_uuid,"extension_uuid":extension_uuid,"tenant_uuid":tenant_uuid,"caller_id_name":caller_id_name,"caller_id_number":caller_id_number,"destination_number":destination_number, "billsecond":billsecond,"recording_path":recording_path,"direction": direction,"callstart":callstart,"call_state":call_state_main }
					recordings_collection.insert_one(insert_recoding_json)
					print("\n","Recording Insert::::",insert_recoding_json)
				##MANAGE CDRs SUMMARY
				today_date = time.strftime("%Y-%m-%d")
				# Define the filter to find the document
				summary_filter = {'date': today_date,'tenant_uuid': tenant_uuid,'user_uuid': user_uuid,'direction':direction}
				fail_count= 0 if int(billsecond) > 0 else 1;
				success_count = 1 if int(billsecond) > 0 else 0;
				total_count = 1
				# Define the update operation
				summary_update = {'$inc': {'fail': fail_count,'success': success_count,'total': total_count,'billsecond': billsecond}}
				# Find the document and perform the update
	#	        	if ip_map_uuid != '' or user_uuid == '':
	#	        		print("\n",'Inbound call skip in summary')
	#	        	else:
	#	        		summaray_result = cdrs_summary_collection.find_one_and_update(summary_filter, summary_update, upsert=True)
	#	        		if summaray_result:
						# The record exists, and the update was successful
	#	        			print("\n",'Summaray exists and was updated:', summaray_result)
	#		        	else:
						# The record doesn't exist
	#		        		print("\n",'Summaray Record does not exist so updated.',summary_update)
				summaray_result = cdrs_summary_collection.find_one_and_update(summary_filter, summary_update, upsert=True)
				if summaray_result:
					# The record exists, and the update was successful
					print("\n",'Summaray exists and was updated:', summaray_result)
				else:
					# The record doesn't exist
					print("\n",'Summaray Record does not exist so updated.',summary_update)
				##MANAGE CDRs SUMMARY END
				#Extra CDRs Entry.
				if receiver_extension_uuid != '':
					direction = 'inbound'
					extension_uuid = receiver_extension_uuid
					user_uuid = variables_value['receiver_user_uuid'] if 'receiver_user_uuid' in variables_value else "";
					sip_call_id = variables_value['sip_call_id'] if 'sip_call_id' in variables_value else custom_callid;
					#Insert CDRs Entry.
					if call_type == 'did':
						print("\n","here6:::")
						if user_uuid != "" and lead_uuid == "":
							print("\n","here7:::")
							if caller_id_number.startswith("+"):
								caller_id_number = caller_id_number[1:]
							if caller_id_name.startswith("+"):
								caller_id_name = caller_id_name[1:]
							lead_uuid = lead_entry_management(caller_id_number,user_uuid,tenant_uuid)
					insert_cdr_json = {"uuid": bridge_uuid+"-local","user_uuid":user_uuid,"extension_uuid":extension_uuid,"tenant_uuid":tenant_uuid,"trunk_uuid":trunk_uuid, "sip_user":sip_user,"caller_id_name":caller_id_name,"caller_id_number":caller_id_number,"destination_number":destination_number, "billsecond":billsecond,"disposition":disposition,"direction": direction,"call_type":call_type,"callstart":callstart,"recording_path":recording_path,"pbx_feature":pbx_feature,"authentication_type":authentication_type, "custom_callid":sip_call_id,"ip_map_uuid":ip_map_uuid,"call_state":call_state ,"lead_uuid":lead_uuid,"campaign_uuid":campaign_uuid,"call_mode":call_mode,"number_masking":number_masking,"dial_method":dial_method,"hangup_cause":hangup_cause,"auto_call_flag":auto_call_flag,"ivr_group":ivr_group }
					cdrs_collection.insert_one(insert_cdr_json)
					print("\n","1harsh Extra CDR Insert::::",insert_cdr_json)
					#Insert Recording entry if have.
					if recording_path != '':
						insert_recoding_json = {"uuid": bridge_uuid+"-local","user_uuid":user_uuid,"extension_uuid":extension_uuid,"tenant_uuid":tenant_uuid,"caller_id_name":caller_id_name,"caller_id_number":caller_id_number,"destination_number":destination_number, "billsecond":billsecond,"recording_path":recording_path,"direction": direction,"callstart":callstart,"call_state":call_state }
						recordings_collection.insert_one(insert_recoding_json)
						print("\n","Extra Recording Insert::::",insert_recoding_json)
					##MANAGE CDRs SUMMARY
					today_date_second = time.strftime("%Y-%m-%d")
					# Define the filter to find the document
					summary_filter = {'date': today_date_second,'tenant_uuid': tenant_uuid,'user_uuid': user_uuid,'direction':direction}
					fail_count = 0 if int(billsecond) > 0 else 1
					success_count = 1 if int(billsecond) > 0 else 0
					total_count = 1
					# Define the update operation
					summary_update = {'$inc': {'fail': fail_count,'success': success_count,'total': total_count,'billsecond': billsecond}}
					# Find the document and perform the update
					summaray_result = cdrs_summary_collection.find_one_and_update(summary_filter, summary_update, upsert=True)
					if summaray_result:
						# The record exists, and the update was successful
						print("\n",'Extra Summaray exists and was updated:', summaray_result)
					else:
						# The record doesn't exist
						print("\n",'Extra Summaray Record does not exist so updated.',summary_update)
					##MANAGE CDRs SUMMARY END
				##FOR RING GROUP AND OTHER FEATURES
				#if (pbx_feature == 'ring_group' or pbx_feature == 'call_queue') and authentication_type == 'acl':
				if pbx_feature == 'ring_group' or pbx_feature == 'call_queue':
					if pbx_feature == 'ring_group':
						dialed_user = variables_value['dialed_user']
						extension_user = dialed_user
					if pbx_feature == 'call_queue':
						dialed_user = variables_value['cc_agent'].replace("@default", "") if 'cc_agent' in variables_value else variables_value['dialed_user']
						dialed_user_explode = dialed_user.split('.')
						extension_user = dialed_user_explode[0]

						
					extension_search_query = {'username': extension_user, 'tenant_uuid':tenant_uuid}
					extensions_result = extension_collection.find_one(extension_search_query)
					print("\n","IN PBX FEATURE EXTRA ENTRY",pbx_feature,extension_search_query)
					if  extensions_result and extensions_result['uuid'] != '':
						extension_uuid = extensions_result['uuid']
						user_uuid = extensions_result['user_uuid']
						direction = 'inbound'
						sip_call_id = variables_value['sip_call_id'] if 'sip_call_id' in variables_value else custom_callid;
						#Insert CDRs Entry.
						print("\n","here5:::",call_type)
						if call_type == 'did':
							print("\n","here6:::")
							if user_uuid != "" and lead_uuid == "":
								print("\n","here7:::")
								if caller_id_number.startswith("+"):
									caller_id_number = caller_id_number[1:]
								if caller_id_name.startswith("+"):
									caller_id_name = caller_id_name[1:]
								lead_uuid = lead_entry_management(caller_id_number,user_uuid,tenant_uuid)

						insert_cdr_json = {"uuid": bridge_uuid+"-"+pbx_feature,"user_uuid":user_uuid,"extension_uuid":extension_uuid,"tenant_uuid":tenant_uuid,"trunk_uuid":trunk_uuid, "sip_user":sip_user,"caller_id_name":caller_id_name,"caller_id_number":caller_id_number,"destination_number":destination_number, "billsecond":billsecond,"disposition":disposition,"direction": direction,"call_type":call_type,"callstart":callstart,"recording_path":recording_path,"pbx_feature":pbx_feature,"authentication_type":authentication_type, "custom_callid":sip_call_id,"ip_map_uuid":ip_map_uuid,"call_state":call_state,"lead_uuid":lead_uuid,"campaign_uuid":campaign_uuid,"call_mode":call_mode,"number_masking":number_masking,"dial_method":dial_method,"call_stick_status":call_stick_status,"hangup_cause":hangup_cause,"auto_call_flag":auto_call_flag,"ivr_group":ivr_group }
						cdrs_collection.insert_one(insert_cdr_json)
						print("\n","0harsh Extra PBX CDR Insert::::",insert_cdr_json)
						#Insert Recording entry if have.
						if recording_path != '':
							insert_recoding_json = {"uuid": bridge_uuid+"-"+pbx_feature,"user_uuid":user_uuid,"extension_uuid":extension_uuid,"tenant_uuid":tenant_uuid,"caller_id_name":caller_id_name,"caller_id_number":caller_id_number,"destination_number":destination_number, "billsecond":billsecond,"recording_path":recording_path,"direction": direction,"callstart":callstart,"call_state":call_state }
							recordings_collection.insert_one(insert_recoding_json)
							print("\n","Extra PBX Recording Insert::::",insert_recoding_json)
						##MANAGE CDRs SUMMARY
						today_date_second = time.strftime("%Y-%m-%d")
						# Define the filter to find the document
						summary_filter = {'date': today_date_second,'tenant_uuid': tenant_uuid,'user_uuid': user_uuid,'direction':direction}
						fail_count = 0 if int(billsecond) > 0 else 1
						success_count = 1 if int(billsecond) > 0 else 0
						total_count = 1
						# Define the update operation
						summary_update = {'$inc': {'fail': fail_count,'success': success_count,'total': total_count,'billsecond': billsecond}}
						# Find the document and perform the update
						summaray_result = cdrs_summary_collection.find_one_and_update(summary_filter, summary_update, upsert=True)
						if summaray_result:
							# The record exists, and the update was successful
							print("\n",'Extra Summaray exists and was updated:', summaray_result)
						else:
							# The record doesn't exist
							print("\n",'Extra Summaray Record does not exist so updated.',summary_update)

	    #Mv file in other place one record pass
	    if file_pass_flag == 0:     		
		    os.system("mv "+path_to_json_files+json_file_name+" "+path_to_move_json_files+json_file_name)
	    print("\n","Jsom File parsing END for:",json_file_name)
except Exception as e:
    print(f"Caught an exception: {e}")
#Mongo Connection close.    
myclient.close()
