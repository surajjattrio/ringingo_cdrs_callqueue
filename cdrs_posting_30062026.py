import os
import json
import pymongo
import uuid
import time
import socket
from datetime import datetime, timezone, timedelta
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import subprocess
from flask import Flask, request, jsonify
from config import mongo_url

app = Flask(__name__)

# MongoDB Connection (Initialize once at startup)
try:
    myclient = pymongo.MongoClient(mongo_url)
    mydb = myclient["db_pbxcc"]
    cdrs_collection = mydb["cdrs"]
    inbound_cdrs_collection = mydb["inbound_cdrs"]
    webhook_collection = mydb["web_hook"]
    lead_mgmt_collection = mydb['lead_management']
    customer_callerid_collection = mydb["customer_callerid_management"]
    ringgroup_collection = mydb["ring_group"]
    tmp_rg_collection = mydb["tmp_ringgroup_details"]
    recordings_collection = mydb["recording"]
    cdrs_summary_collection = mydb["cdrs_summary"]
    extension_collection = mydb["extensions"]
    campaign_statistics_collection = mydb["campaign_statistics"]
    voicebroadcast_details_collection = mydb["voicebroadcast_details"]
    did_collection = mydb["did"]
    ivr_collection = mydb["ivr"]

except Exception as e:
    print(f"Failed to connect to MongoDB: {e}")
    raise SystemExit("Exiting due to MongoDB connection failure")

# Ensure MongoDB connection is alive
try:
    myclient.admin.command('ping')
    print("MongoDB connection successful")
except Exception as e:
    print(f"MongoDB ping failed: {e}")
    raise SystemExit("Exiting due to MongoDB ping failure")

def manage_drop_call_flag(variables_value, lead_uuid, hangup_reason, originate_disposition):
    print("hangup_reason", hangup_reason)
    disposition_collection = mydb['disposition']
    lead_report_collection = mydb['lead_report']
    auto_hangup = variables_value.get('sip_h_X-amd_hangup', '')
    auto_call = variables_value.get('sip_h_X-Autocalltype','false')
    if auto_hangup == 'true':
        filter = {"code": "AA"}
    elif originate_disposition == "NO_USER_RESPONSE" and auto_call == "true":
        filter = {"code": "DROP"}
    elif hangup_reason == 'ALLOTTED_TIMEOUT' or originate_disposition == "NO_USER_RESPONSE" or hangup_reason == 'NO_USER_RESPONSE' or hangup_reason == 'NO_ANSWER':
        filter = {"code": "NA"}
    elif  hangup_reason == "USER_BUSY" or originate_disposition == "USER_BUSY":
        filter = {"code": "Busy", "parent_disposition":""}
    elif hangup_reason == 'NORMAL_UNSPECIFIED':
        filter = {"code": "Busy"}
    elif hangup_reason == 'INVALID_NUMBER_FORMAT':
        filter = {"code": "Auto_Originate"}
    elif hangup_reason == 'AGENT_NOT_AVAILABLE':
        filter = {"code": "ANA"}  
    elif hangup_reason == 'ORIGINATOR_CANCEL' or originate_disposition == 'ORIGINATOR_CANCEL':
        filter = {"code": "Auto_Originate"}
    #elif hangup_reason == 'NO_USER_RESPONSE':
    #    filter = {"code": "NAVL"}
    else:
        filter = {"code": "DROP"}
    print("manage_drop_lead_uuid", lead_uuid)
    disposition_collection_data = disposition_collection.find_one(filter)
    disposition_uuid = ""
    if disposition_collection_data:
        disposition_uuid = disposition_collection_data['disposition_uuid']
        print("Update lead status as DROP call for disposition_uuid", disposition_uuid)
        print("Update lead status as DROP call for UUID", lead_uuid)
        filter = {"lead_management_uuid": lead_uuid}
        update = {"$set": {"lead_status": disposition_uuid}}
        lead_mgmt_collection.update_one(filter, update)

        filter = {"lead_management_uuid": lead_uuid}
        lead_management_data = lead_mgmt_collection.find_one(filter)
        print("lead_management_data", lead_management_data)
        lead_management_data = lead_mgmt_collection.find_one(filter)
        print("lead_management_data", lead_management_data)

        if lead_management_data:
            current_call_count = lead_management_data.get('call_count', 0)
            new_call_count = current_call_count + 1
            #new_call_count = lead_management_data['call_count'] + 1
            filter = {"lead_group_uuid": lead_management_data['lead_group_uuid'], "disposition_uuid": disposition_uuid, "called_count": new_call_count}
            lead_report_data = lead_report_collection.find_one(filter)

            if lead_report_data:
                print("lead_report_data", lead_report_data)
                filter = {"lead_group_uuid": lead_management_data['lead_group_uuid'], "disposition_uuid": disposition_uuid, "called_count": new_call_count}
                update = {"$inc": {"count": 1}}
                lead_report_collection.update_one(filter, update)
                print("lead report updated successfully")
            else:
                print("lead_report_found_empty")
                lead_report_uuid = str(uuid.uuid4())
                insert_lead_report_string = {
                    "disposition_uuid": disposition_uuid,
                    "lead_group_uuid": lead_management_data['lead_group_uuid'],
                    "uuid": lead_report_uuid,
                    "called_count": 1,
                    "count": 1
                }
                lead_report_collection.insert_one(insert_lead_report_string)
                print("lead_report_insert_string", insert_lead_report_string)

    return disposition_uuid

def manage_lead_originated(variables_value):
    auto_lead_uuid = variables_value.get('sip_h_P-lead_uuid', "")
    lead_originate = mydb["lead_originate"]
    filter = {"lead_management_uuid": auto_lead_uuid}
    print("auto_lead_uuid", auto_lead_uuid)
    if auto_lead_uuid:
        print("auto_lead_uuid DELETE", auto_lead_uuid)
        lead_originate.delete_many(filter)

def manage_auto_campaign(variables_value, auto_campaign_originate_uuid):
    auto_campaign_originate = mydb["auto_campaign_originate"]
    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    print("yaksh formatted date", formatted_time)
    filter = {"auto_campaign_originate_uuid": auto_campaign_originate_uuid, "flag": '3'}
    if int(variables_value.get('billsec', 0)) > 0:
        print("manage_auto_campaign duration > 0")
        update = {"$set": {"flag": "0", "last_calldate": formatted_time}}
    else:
        print("manage_auto_campaign duration == 0")
        update = {"$set": {"flag": "0"}}
    auto_campaign_originate.update_one(filter, update)

def manage_auto_campaign_count(variables_value, auto_campaign_originate_uuid):
    auto_campaign_originate = mydb["auto_campaign_originate"]
    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
    filter = {"auto_campaign_originate_uuid": auto_campaign_originate_uuid}
    if int(variables_value.get('billsec', 0)) > 0:
        print("manage_auto_campaign_count duration > 0", auto_campaign_originate_uuid)
        update = {"$set": {"flag": "0", "last_calldate": formatted_time}}
    else:
        print("manage_auto_campaign_count duration == 0", auto_campaign_originate_uuid)
        update = {"$set": {"flag": "0"}}
    auto_campaign_originate.update_one(filter, update)

def check_number_masking(campaign_uuid, campaign_flag):
    outbound_campaign = mydb['outbound_campaign']
    blended_campaign = mydb['blended_campaign']
    filter = {'uuid': campaign_uuid}
    o_result_campaign = outbound_campaign.find_one(filter)
    b_result_campaign = blended_campaign.find_one(filter)
    #if campaign_flag == 'blended':
    #    result_campaign = blended_campaign.find_one(filter)
    #else:
    #    result_campaign = outbound_campaign.find_one(filter)
    #print("result of campaign", result_campaign)
    return o_result_campaign or b_result_campaign

def manage_lead_retries(variables_value, campaign_uuid, campaign_flag, hangup_cause):
    lead_uuid = variables_value.get('sip_h_P-lead_uuid', '')
    outbound_campaign_db = mydb['outbound_campaign']
    blended_campaign_db = mydb['blended_campaign']
    filter = {'uuid': campaign_uuid}
    o_campaign = outbound_campaign_db.find_one(filter)
    b_campaign = blended_campaign_db.find_one(filter)
    if o_campaign:
        campaign = o_campaign
    else:
        campaign = b_campaign
    #if campaign_flag == 'blended':
    #    campaign = blended_campaign_db.find_one(filter)
    #else:
    #    campaign = outbound_campaign_db.find_one(filter)
    print(campaign)
    if campaign:
        max_retries = campaign['max_retries']
        retries_time = campaign['retries_time']
        max_retries_disposition = campaign.get('max_retries_disposition', [])
        current_date = datetime.now()
        new_date = current_date + timedelta(minutes=int(retries_time))
        #iso_date = f'ISODate({new_date.isoformat()}Z)'
        leads_in_queue_db = mydb['leads_in_queue']
        filter = {'campaign_uuid': campaign_uuid, 'lead_management_uuid': lead_uuid}
        print(filter)
        lead_in_queue = leads_in_queue_db.find_one(filter)
        print(lead_in_queue)
        #yaksh
        if lead_in_queue:
            if 'lead_resent_count' in lead_in_queue:
                if int(lead_in_queue['lead_resent_count']) < int(max_retries):
                    lead_type = int(lead_in_queue['lead_resent_count']) + 1
                    if int(lead_type) == int(max_retries):
                        filter = {"campaign_uuid": campaign_uuid, "lead_management_uuid": lead_uuid}
                        leads_in_queue_db.delete_one(filter)
                    elif hangup_cause in campaign['max_retries_disposition']:
                        filter = {"campaign_uuid": campaign_uuid, "lead_management_uuid": lead_uuid}
                        update = {"$set": {"lead_resent_count": lead_type, "lead_sent": '1', 'resend_lead_time': new_date}}
                        leads_in_queue_db.update_one(filter, update)
            elif int(max_retries) > 0 and hangup_cause in campaign['max_retries_disposition']:
                filter = {"campaign_uuid": campaign_uuid, "lead_management_uuid": lead_uuid}
                update = {"$set": {"lead_resent_count": '0', "lead_sent": '1', "resend_lead_time": new_date}}
                leads_in_queue_db.update_one(filter, update)
                print("updated successfully")
          
def lead_entry_management(destination_number, user_uuid, tenant_uuid):
    try:
        print("\n", "here4:::")

        # Validate tenant_uuid
        if not tenant_uuid or not str(tenant_uuid).strip():
            print("Invalid tenant_uuid")
            return ""

        dest_number = str(destination_number).lstrip('+')

        tenant_collection = mydb['tenant']
        tenant_info = tenant_collection.find_one({"uuid": tenant_uuid})

        if not tenant_info:
            print(f"Tenant not found: {tenant_uuid}")
            return ""

        lead_mgmt_collection = mydb['lead_management']
        lead_uuid = str(uuid.uuid4())

        lead_mgmt = lead_mgmt_collection.find_one({
            "phone_number": dest_number,
            "tenant_uuid": tenant_uuid
        })

        if not lead_mgmt and tenant_info.get('add_anonymous_lead', '0') != '1':
            insert_lead_mgmt_string = {
                "address": "",
                "state": "",
                "province": "",
                "postal_code": "",
                "dob": "",
                "phone_code": "",
                "alternate_phone_number": "",
                "email": "",
                "status": "0",
                "gender": "0",
                "created_flag": "2",
                "country_uuid": "",
                "city": "",
                "first_name": "Anonymous",
                "last_name": "",
                "phone_number": dest_number,
                "user_uuid": user_uuid,
                "tenant_uuid": tenant_uuid,
                "lead_management_uuid": lead_uuid,
                "lead_status": "3809f32d-9096-406b-b2ff-1207b3fbfbc5"
            }

            lead_mgmt_collection.insert_one(insert_lead_mgmt_string)
            print(insert_lead_mgmt_string)
            return lead_uuid

        return lead_mgmt.get('lead_management_uuid', "") if lead_mgmt else ""

    except Exception as e:
        print(f"Error in lead_entry_management: {e}")
        return ""

def get_voicebroadcast_disposition(vb_type, lead_uuid, voicebroadcast_uuid, duration):
    disposition_collection = mydb['disposition']
    lead_mgmt_collection = mydb['lead_management']
    voicebroadcast_collection = mydb['voice_broadcast']
    lead_broadcast_queue_collection = mydb['lead_broadcast_queue']
    filter_codes = {
        '0': 'VBOBD', '1': 'VBOBD', '2': 'NI', '3': 'DNC', 'CANCEL': 'NA',
        #'0': 'SURVEY', '1': 'IVR', '2': 'NI', '3': 'DNC', 'CANCEL': 'NA',
        'Busy here': 'Busy', 'Temporarily unavailable': 'NAVL', 'send_bye': 'NAVL'
    }
    #filter = {"code": filter_codes.get(vb_type, 'NDP' if duration != '0' else 'NA')}
    filter = {"code": filter_codes.get(vb_type)}
    print("filter for vb", filter)
    disposition = disposition_collection.find_one(filter)
    print("o/p dispooooor for vb", disposition)
    if disposition:
        disposition_uuid = str(disposition['disposition_uuid'])
        print("Update lead status as DROP voicebroadcast call for disposition_uuid", disposition_uuid)
        print("Update lead status as DROP voicebroadcast call for UUID", lead_uuid)
        filter = {"lead_management_uuid": lead_uuid}
        update = {"$set": {"lead_status": disposition_uuid}}
        lead_mgmt_collection.update_one(filter, update)
        voicebroadcast_data = voicebroadcast_collection.find_one({'uuid': voicebroadcast_uuid})
        print('yaksh this is your voicebroadcast data', voicebroadcast_data)
        lead_management_data = lead_mgmt_collection.find_one({'lead_management_uuid': lead_uuid})
        print(lead_management_data)
        if lead_management_data and lead_management_data['retries_count'] < int(voicebroadcast_data['max_retries']):
            if disposition_uuid in voicebroadcast_data['max_retries_disposition_uuid']:
                max_count_document = lead_broadcast_queue_collection.find_one(
                    {"lead_group_uuid": lead_management_data['lead_group_uuid']}, sort=[("count", -1)])
                max_count = max_count_document['count'] if max_count_document else 0
                new_count = max_count + 1
                lead_broadcast_uuid = str(uuid.uuid4())
                lead_broadcast_queue_insert_string = {
                    "lead_group_uuid": lead_management_data['lead_group_uuid'],
                    "tenant_uuid": lead_management_data['tenant_uuid'],
                    "lead_management_uuid": lead_management_data['lead_management_uuid'],
                    "count": new_count, "uuid": lead_broadcast_uuid
                }
                lead_broadcast_queue_collection.insert_one(lead_broadcast_queue_insert_string)
                print('leads_in_queue_insert_string', lead_broadcast_queue_insert_string)
                lead_mgmt_collection.update_one({"lead_management_uuid": lead_uuid}, {"$inc": {"retries_count": 1}})
            else:
                print('not going in disposition condition')
        else:
            print('not going in retries condition')
        return disposition_uuid
    return ""

def starts_with_number(s):
    try:
        int(s[0])
        return True
    except (ValueError, IndexError):
        return False

def send_notification(tenant_uuid, caller_id_name, caller_id_number, destination_number, callstart, call_type):
    print(tenant_uuid, caller_id_name, caller_id_number, destination_number, callstart, "first", call_type)
    if starts_with_number(caller_id_name):
        caller_id_name = "Anonymous"
    tenant_db = mydb['tenant']
    tenant_info = tenant_db.find_one({'uuid': tenant_uuid})
    print(tenant_info)
    sender_email = "7b26b2da1b254e6133d47b2cc3a2184a"
    sender_password = "1b7f00c11402cc3c26cacf592725ea87"
    from_email = "notifications@test.io"
    recipient_email = str(tenant_info['email'])
    call_type = 'sip' if call_type == "local" else call_type
    subject = "Missed call notification from COMPANY"
    message = f"""\
    <html>
    <body>
        <p>Hello,</p>
        <p>You have received a missed call.</p>
        <p>Caller name: {caller_id_name}<br>
        Caller number: {caller_id_number}<br>
        Destination number: {destination_number}<br>
        Time: {callstart}<br>
        Missed by: {call_type}</p>
        <p>Thanks for using COMPANY</p>
        <br>
    </body>
    </html>
    """
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = recipient_email
    msg["Subject"] = subject
    msg.attach(MIMEText(message, "html"))
    smtp_server = "in-v3.mailjet.com"
    smtp_port = 587
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(from_email, recipient_email, msg.as_string())
            print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

def set_caller_sticky(user_uuid, tenant_uuid, caller_id_number):
    user_collection = mydb['user']
    user_role_collection = mydb['user_role']
    sticky_agent_collection = mydb['sticky_agent']
    print("print the entry yaksh", user_uuid, tenant_uuid, caller_id_number)
    user_info = user_collection.find_one({'uuid': user_uuid})
    print("yaksh user_info", user_info['user_role_uuid'])
    user_role_info = user_role_collection.find_one({'uuid': user_info['user_role_uuid']})
    print("yaksh_user_role_info", user_role_info)
    if user_role_info.get('sticky_agent') == '0':
        sticky_new_uuid = str(uuid.uuid4())
        insert_sticky_string = {
            'uuid': sticky_new_uuid, 'user_uuid': user_uuid, 'tenant_uuid': tenant_uuid,
            'user_role_uuid': user_role_info['uuid'], 'phone_number': caller_id_number,
            'sticky_user_role_status': '0'
        }
        sticky_agent_collection.insert_one(insert_sticky_string)
        print("sticky data inserted successfully")

def get_telecom_circle(tenant_uuid, destination_number):
    telecom_circle_collection = mydb['telecom_circle']
    destination_number = str(destination_number)
    first_digit = destination_number[0] if destination_number else ''
    prefix_len = 0
    longest_prefix = ''
    longest_prefix_name = ''
    while destination_number:
        filter = {'tenant_uuid': tenant_uuid, 'prefix': {'$regex': '^' + first_digit}}
        result = telecom_circle_collection.find(filter)
        for prefix_details in result:
            if prefix_len == 0 or prefix_len < len(prefix_details['prefix']):
                prefix_len = len(prefix_details['prefix'])
                longest_prefix = prefix_details['prefix']
                longest_prefix_name = prefix_details['uuid']
                print("[common/custom.py]:: Prefix base caller id match:", longest_prefix, longest_prefix_name)
        destination_number = destination_number[:-1]
        if destination_number.startswith(longest_prefix):
            return longest_prefix_name
    return ''

def compute_call_count(tenant_uuid, lead_uuid, caller_id_number, destination_number, callstart_value):
    """Compute today's call count for this lead or caller/destination pair and return next count."""
    try:
        if callstart_value:
            call_date = str(callstart_value).split(' ')[0]
        else:
            call_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        if lead_uuid:
            filt = {'tenant_uuid': tenant_uuid, 'lead_uuid': lead_uuid, 'callstart': {'$regex': f'^{call_date}'}}
        else:
            filt = {'tenant_uuid': tenant_uuid, 'caller_id_number': caller_id_number, 'destination_number': destination_number, 'callstart': {'$regex': f'^{call_date}'}}

        existing = cdrs_collection.count_documents(filt)
        return existing + 1
    except Exception as e:
        print('compute_call_count error', e)
        return 1

first_leg_store = {}

# INBOUND CALL HOLD TIME LOGIC
hold_bleg_store = {}  # keyed by B-leg UUID → hold_accum_seconds; used for inbound call hold fix
hold_bleg_lock = threading.Lock()


def _parse_esl_headers(text):
    headers = {}
    for line in text.split('\n'):
        if ':' in line:
            k, v = line.split(':', 1)
            headers[k.strip()] = v.strip()
    return headers


def esl_listener_thread():
    """Daemon thread: subscribes to FreeSWITCH ESL and captures hold_accum_seconds
    from B-leg (direction=outbound) CHANNEL_HANGUP_COMPLETE events. Stores in
    hold_bleg_store so inbound A-leg CDR processing can use the real hold time."""
    while True:
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(('127.0.0.1', 8021))
            sock.settimeout(60)

            buf = b''
            while b'auth/request' not in buf:
                buf += sock.recv(1024)
            sock.send(b'auth ClueCon\r\n\r\n')
            buf = b''
            while b'+OK accepted' not in buf:
                buf += sock.recv(1024)

            sock.send(b'event plain CHANNEL_HANGUP_COMPLETE\r\n\r\n')
            buf = b''
            while b'Reply-Text' not in buf:
                buf += sock.recv(1024)

            buf = b''
            while True:
                try:
                    chunk = sock.recv(8192)
                    if not chunk:
                        break
                    buf += chunk

                    while True:
                        pos = buf.find(b'\n\n')
                        if pos == -1:
                            break
                        header_text = buf[:pos].decode('utf-8', errors='ignore')
                        hdrs = _parse_esl_headers(header_text)
                        content_len = int(hdrs.get('Content-Length', 0))
                        total = pos + 2 + content_len
                        if len(buf) < total:
                            break
                        body = buf[pos + 2:total].decode('utf-8', errors='ignore')
                        buf = buf[total:]

                        evt = _parse_esl_headers(body)
                        if evt.get('Event-Name') != 'CHANNEL_HANGUP_COMPLETE':
                            continue
                        if evt.get('Call-Direction') != 'outbound':
                            continue

                        b_uuid = evt.get('Unique-ID', '')
                        try:
                            hold_secs = int(evt.get('variable_hold_accum_seconds', 0))
                        except (ValueError, TypeError):
                            hold_secs = 0

                        print(f"[ESL_HOLD] B-leg hangup: uuid={b_uuid}, hold={hold_secs}")
                        if hold_secs > 0 and b_uuid:
                            with hold_bleg_lock:
                                hold_bleg_store[b_uuid] = hold_secs
                            try:
                                cdrs_collection.update_one(
                                    {'uuid': b_uuid, 'hold_time': 0},
                                    {'$set': {'hold_time': hold_secs}}
                                )
                                print(f"[ESL_HOLD] Retroactive CDR update: uuid={b_uuid}, hold={hold_secs}")
                            except Exception as db_err:
                                print(f"[ESL_HOLD] DB update error: {db_err}")

                except socket.timeout:
                    try:
                        sock.send(b'api status\r\n\r\n')
                        sock.recv(1024)
                    except Exception:
                        break

        except Exception as e:
            print(f"[ESL_HOLD] Listener error: {e}, reconnecting in 5s...")
        finally:
            if sock:
                try:
                    sock.close()
                except Exception:
                    pass
        time.sleep(5)


_esl_thread = threading.Thread(target=esl_listener_thread, daemon=True)
_esl_thread.start()


@app.route('/cdrs', methods=['POST'])
def process_cdr():
    #global variables_value
    #global first_leg_store

    try:
        data = request.get_json()
        if not data or 'variables' not in data:
            return jsonify({"error": "Invalid or missing variables data"}), 400

        first_leg_store = {}  # if needed, also make it per-request or thread-safe
        variables_value = data['variables']
        print("variableeee_valueeeeeeeeeeeeeeeeeeee",variables_value)

        # B-leg early handling: FreeSWITCH sets direction='outbound' on legs it originated.
        # For inbound calls the B-leg (agent's WebRTC) carries the real hold_accum_seconds;
        # for outbound calls the B-leg (customer SIP) always has hold_accum_seconds=0.
        if variables_value.get('direction') == 'outbound':
            global hold_bleg_store
            b_hold = int(variables_value.get('hold_accum_seconds', 0))
            b_uuid = variables_value.get('uuid', '')
            if b_hold > 0 and b_uuid:
                hold_bleg_store[b_uuid] = b_hold
                cdrs_collection.update_one({'uuid': b_uuid, 'hold_time': 0}, {'$set': {'hold_time': b_hold}})
                print(f"[HOLD_FIX] B-leg hold stored and retroactive CDR update: uuid={b_uuid}, hold={b_hold}")
            else:
                print(f"[HOLD_FIX] B-leg skipped (no hold): uuid={b_uuid}")
            #return jsonify({"status": "skipped b-leg"}), 200

        file_pass_flag = 0
        inbound_answered_user_uuid = ""
        inbound_answered_extension_uuid = ""
        profile = variables_value.get("sip_profile_name")
        if profile == "internal":
            first_leg_store['holdsecond'] = int(variables_value.get('hold_accum_seconds', 0))
        print("first leg store::::::::::",first_leg_store)

        auto_campaign = variables_value.get('sip_h_P-auto_campaign', "")
        threeway_agent_extension_uuid = variables_value.get('threeway_agent_extension_uuid', "")
        if threeway_agent_extension_uuid:
            customer_callerid_collection.delete_many({'user_uuid': threeway_agent_extension_uuid})
        print("harsh auto_campaign", auto_campaign)
        voice_broadcast = variables_value.get('sip_h_P-voicebroadcast', "")
        if voice_broadcast == "false":
            voice_broadcast = ''
        sticky_agent_flag = variables_value.get('sticky_agent_flag', "")
        inbound_hangup_cause = variables_value.get('hangup_cause', "")
        success_answered_user_uuid = variables_value.get('user_uuid', "")
        success_answered_extension_uuid = variables_value.get('extension_uuid', "")

        # Feedabck field addition:
        feedbackScore = variables_value.get('ivr_feedback_score', "");
       
        if inbound_hangup_cause == "NORMAL_CLEARING" and success_answered_user_uuid and success_answered_extension_uuid:
            tmp_call_uuid = variables_value.get('uuid', "")
            inbound_answered_user_uuid = success_answered_user_uuid
            inbound_answered_extension_uuid = success_answered_extension_uuid
            tmp_uuid = str(uuid.uuid4())
            insert_rg_string = {
                "uuid": tmp_uuid,
                "call_uuid": tmp_call_uuid,
                "user_uuid": inbound_answered_user_uuid,
                "extension_uuid": inbound_answered_extension_uuid
            }
            tmp_rg_collection.insert_one(insert_rg_string)
            print("yaksh this is the answered user uuid number", inbound_answered_user_uuid)
            print("yaksh answered extension uuid", inbound_answered_extension_uuid)
        
        manage_lead_originated(variables_value)
        auto_campaign_originate_uuid = variables_value.get('sip_h_P-auto_campaign_originate', "")
        if auto_campaign_originate_uuid:
            manage_auto_campaign_count(variables_value, auto_campaign_originate_uuid)
        auto_agent_flag = variables_value.get('sip_h_P-auto_agent_flag', "")
        if auto_campaign_originate_uuid and auto_agent_flag == '':
            manage_auto_campaign(variables_value, auto_campaign_originate_uuid)
        if auto_agent_flag == "auto_agent":
            file_pass_flag = 1
            return jsonify({"status": "skipped auto_agent"}), 200
        
        callstart = variables_value.get('custom_callstart', variables_value.get('sip_h_P-callstart', ""))
        #if callstart and callstart.strip():
        #   dt_utc = datetime.strptime(callstart, "%Y-%m-%d %H:%M:%S")
        #    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
        #    ist_offset = timezone(timedelta(hours=5, minutes=30))
        #    dt_ist = dt_utc.astimezone(ist_offset)
        #    callstart = dt_ist.strftime("%Y-%m-%d %H:%M:%S")
        print("call strttttttttttttttttttttt",callstart)
        created_at = datetime.now(timezone.utc)
        if not callstart:
            epoch_time = variables_value.get('start_epoch', "")
            #epoch_time = variables_value.get('end_epoch', "")
            if epoch_time:
                dt_utc = datetime.fromtimestamp(int(epoch_time), tz=timezone.utc)
                callstart = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
                #utc_now = datetime.utcnow()
                #callstart = utc_now.strftime('%Y-%m-%d %H:%M:%S')
                print("yaksh auto campaign callstart ", callstart)
        
        feature_code_flag = variables_value.get('feature_code_flag', "false")
        if feature_code_flag == "true":
            file_pass_flag = 1
            return jsonify({"status": "skipped feature code"}), 200
        
        if not callstart:
            file_pass_flag = 1
            return jsonify({"status": "skipped no callstart"}), 200
        
        pbx_feature = variables_value.get('pbx_feature', "")
        pbx_feature_uuid = ""
        if pbx_feature == 'call_queue':
            pbx_feature_uuid = variables_value.get('call_queue_id', "")
        elif pbx_feature == 'ivr':
            pbx_feature_uuid = variables_value.get('ivr_group_uuid', "")
            if not feedbackScore: 
                feedbackScore = "N/A"
        elif pbx_feature == 'ring_group':
            pbx_feature_uuid = variables_value.get('ringgroup_id', "")
        elif pbx_feature == 'time_condition':
            pbx_feature_uuid = variables_value.get('time_condition_uuid', "")
        
        ip_map_uuid = variables_value.get('ip_map_uuid', "")
        custom_callid = variables_value.get('custom_callid', variables_value.get('sip_h_X-custom_callid', variables_value.get('sip_h_X-Custom-Callid', "")))
        tenant_uuid = variables_value.get('tenant_uuid', variables_value.get('sip_h_P-tenant_uuid',variables_value.get('sip_h_X-tenant_uuid', "")))
        direction = variables_value.get('direction', "outbound")
        is_inbound_call = variables_value.get('sip_h_X-inbound_campaign_flag',"false")
        bridge_uuid = variables_value.get('bridge_uuid', str(uuid.uuid4()))
        user_uuid = variables_value.get('user_uuid', "") or variables_value.get('sip_h_P-user_uuid', "")
        authentication_type = variables_value.get('authentication_type', "auth")
        extension_uuid = variables_value.get('extension_uuid', "")
        trunk_uuid = variables_value.get('trunk_uuid', "")
        sip_user = variables_value.get('sip_user', "")
        ivr_group = variables_value.get('ivr_group_uuid', "")
        caller_id_name = variables_value.get('effective_caller_id_name', variables_value.get('caller_id_name', variables_value.get('origination_caller_id_name', variables_value.get('pre_transfer_caller_id_name', ""))))
        caller_id_number = variables_value.get('effective_caller_id_number', variables_value.get('caller_id_number', variables_value.get('original_caller_id_number', variables_value.get('origination_caller_id_number', variables_value.get('pre_transfer_caller_id_number', "")))))
        dialed_user = variables_value.get('dialed_user', "")
        lead_uuid = variables_value.get('sip_h_X-Leaduuid', variables_value.get('sip_h_X-lead_uuid', variables_value.get('sip_h_P-lead_uuid', "")))
        destination_number = variables_value.get('custom_destination_number', variables_value.get('effective_destination_number', variables_value.get('sip_h_P-cb_destination',variables_value.get('Caller-Destination-Number',variables_value.get('sip_h_X-did_number', dialed_user)))))

        if lead_uuid == "null":
            lead_uuid = ""
        
        if not tenant_uuid or tenant_uuid == "":
            if "_open" in str(destination_number):
                destination_number = str(destination_number).split("_")[0]
            did_info = did_collection.find_one({"number": f"+{destination_number}"})
            tenant_uuid = did_info['tenant_uuid'] if did_info else ""

        # Validate tenant ownership before trusting header
        if lead_uuid and tenant_uuid:
            lead_owner = lead_mgmt_collection.find_one(
                {"lead_management_uuid": lead_uuid, "tenant_uuid": tenant_uuid},
                {"_id": 1}
            )
            if not lead_owner:
                print(f"[TENANT_MISMATCH] Rejected lead_uuid={lead_uuid} for tenant={tenant_uuid}")
                lead_uuid = ""
        hangup_reason = variables_value.get('hangup_cause', "")
        print("hangup1", hangup_reason)
        auto_call_flag = "0"
        hangup_cause = ""
        campaign_uuid = variables_value.get('sip_h_X-selectedcampaignuuid', variables_value.get('sip_h_P-campaign_uuid', ""))
        campaign_flag = variables_value.get('sip_h_P-campaign_flag', "")
        #campaign_flag = variables_value.get('sip_h_X-campaign_flag', "")
        print("campaign flaggggggggggggggggg",campaign_flag)
        call_stick_status = variables_value.get('call_stick_status', 1)
        billsecond = int(variables_value.get('billsec', 0))

        # new fields
        waitsecond = int(variables_value.get('waitsec', 0)) # for inbound - call queue
        if hangup_party_disposition == 'send_cancel' and direction == 'outbound':
            disconnected_by = '2'
        elif hangup_party_disposition == 'send_bye' and direction == 'outbound':
            disconnected_by = '0'
        elif hangup_party_disposition == 'recv_cancel' and direction == 'outbound':
            disconnected_by = '1'
        elif hangup_party_disposition == 'recv_refuse' and direction == 'outbound' and disposition == "NO_USER_RESPONSE":
            disconnected_by = '2'
        elif hangup_party_disposition == 'recv_refuse' and direction == 'outbound':
            disconnected_by = '1'
        elif hangup_party_disposition == 'recv_bye' and direction == 'inbound' and pbx_feature != "":
            disconnected_by = '1'
        elif hangup_party_disposition == 'recv_bye' and direction == 'inbound' and is_inbound_call == "false":
            disconnected_by = '0'
            # direction = "outbound"
        elif hangup_party_disposition == 'send_bye' and direction == 'inbound' and is_inbound_call == "false":
            disconnected_by = '1'
            # direction = "outbound"
        elif hangup_party_disposition == 'recv_cancel' and direction == 'inbound' and is_inbound_call == "false":
            disconnected_by = '0'
            # direction = "outbound"
        elif hangup_party_disposition == 'send_refuse' and direction == 'inbound' and is_inbound_call == "false":
            disconnected_by = '2' # auto hangup from agent side.
            # direction = "outbound"
        elif hangup_party_disposition == 'recv_bye' and direction == 'inbound':
            disconnected_by = '1'
        #elif hangup_party_disposition == 'send_bye' and direction == 'inbound' and pbx_feature != "":
        #    disconnected_by = '2'
        elif hangup_party_disposition == 'send_bye' and direction == 'inbound':
            disconnected_by = '0'
        elif hangup_party_disposition == 'recv_refuse' and direction == 'inbound':
            disconnected_by = '0'

        #if hangup_party_disposition == 'send_bye':
        #    disconnected_by = '0' if direction == 'inbound' else '1'
        #elif hangup_party_disposition == 'recv_bye':
        #    disconnected_by = '1' if direction == 'inbound' else '0'
        #elif hangup_party_disposition == 'recv_cancel':
        #    disconnected_by = '1' if direction == 'inbound' else '0'
        #elif hangup_party_disposition == 'send_refuse':
        #    disconnected_by = '0' if direction == 'inbound' else '1'
        print("disconnected by", disconnected_by)

        if "webphonetransfer" in variables_value.get('sip_refer_to', '') or variables_value.get('webphonetransfer', '0') == '1':
            destination_number = variables_value.get('sip_to_user', destination_number)
            caller_id_number = variables_value.get('sip_user', variables_value.get('pre_transfer_caller_id_number', caller_id_number))

        if "_open" in destination_number:
            caller_id_number = variables_value.get('sip_user', caller_id_number)

        is_csat = "1"
        if pbx_feature == "ivr":
            ivr_details = ivr_collection.find_one({"uuid": ivr_group, "tenant_uuid": tenant_uuid})
            if ivr_details:
                is_csat = ivr_details["is_csat"] or is_csat

        normalized = str(destination_number).split("_")[0]
        did_filter = { "$or": [ {"number": str(destination_number).split("_")[0]}, {"number": f"+{normalized}"}, {"number": destination_number}]}
        did_value = did_collection.find_one(did_filter)
        if did_value:
            direction = "inbound"
        else:
            direction = "outbound"
        normalized = ''
        
        # set user uuid to nil
        if auto_campaign == "true" and billsecond == 0:
            user_uuid = ""

                'tenant_uuid': tenant_uuid,
                'campaign_uuid': campaign_uuid,
                'campaign_date': campaign_date,
                'manager_uuid': manager_uuid
            }
            campaign_duration_check = 1 if 0 < billsecond < 30 else 0
            campaign_statistics_drop_count = 0
            campaign_statistics_fail_count = 0
            campaign_statistics_amd_count = 0
            campaign_statistics_success_count = 0
            
            if auto_campaign:
                print("checking lead originate issue")
                print("yaksh auto campaign", auto_campaign)
                print("yaksh auto campaign originate uuid", auto_campaign_originate_uuid)
                print("yaksh lead_uuid", lead_uuid)
                if auto_campaign and not auto_campaign_originate_uuid and lead_uuid:
                    print("yaksh_ outbound")
                    if billsecond > 0:
                        campaign_statistics_drop_count = 1
                    print("hangup_reasonhangup_reasonhangup_reason",tenant_uuid, hangup_reason)
                    hangup_cause = manage_drop_call_flag(variables_value, lead_uuid, hangup_reason, originate_disposition)
                    print("hangup cause auto dial ----------------------------------------", hangup_cause)
                    auto_call_flag = "2"
                elif auto_campaign and auto_campaign_originate_uuid and lead_uuid:
                    print("this is success but fail")
                    if hangup_reason in ["NO_USER_RESPONSE", "ORIGINATOR_CANCEL"] or originate_disposition in ["USER_BUSY", "NO_USER_RESPONSE", "ORIGINATOR_CANCEL"]:  # ::yaksh::
                        hangup_cause = manage_drop_call_flag(variables_value, lead_uuid, hangup_reason, originate_disposition)
                        if billsecond > 0:
                            campaign_statistics_drop_count = 1
                        auto_call_flag = "2"
                    else:
                        campaign_statistics_success_count = 1
                        auto_call_flag = "1"
                if billsecond > 0:
                    campaign_statistics_success_count = 1
                else:
                    campaign_statistics_fail_count = 1
            else:
                campaign_statistics_success_count = 1 if billsecond > 0 else 0
                campaign_statistics_fail_count = 0 if billsecond > 0 else 1
            
            auto_hangup = variables_value.get('sip_h_X-amd_hangup', "")
            if auto_hangup == 'true':
                campaign_statistics_amd_count = 1
                campaign_statistics_drop_count = 0
            
            campaign_statistics_update = {
                '$inc': {
                    'drop': campaign_statistics_drop_count,
                    'success': campaign_statistics_success_count,
                    'fail': campaign_statistics_fail_count,
                    'short_call': campaign_duration_check,
                    'amd': campaign_statistics_amd_count,
                    'billsecond': billsecond
                }
            }
            print("campaign_statistics_update", campaign_statistics_update)
            campaign_statistics_collection.find_one_and_update(campaign_statistics_filter, campaign_statistics_update, upsert=True)
        
        # destination_number = variables_value.get('custom_destination_number', variables_value.get('effective_destination_number', variables_value.get('sip_h_P-cb_destination',variables_value.get('Caller-Destination-Number',variables_value.get('sip_h_X-did_number', dialed_user)))))
        disposition = variables_value.get('hangup_cause', 'NORMAL_CLEARING')
        call_state = 'normal'
        originate_failed_cause = variables_value.get('originate_failed_cause', "")
        if originate_failed_cause == 'ORIGINATOR_CANCEL':
            disposition = originate_failed_cause
        if disposition in ['NO_ANSWER', 'NO_USER_RESPONSE', 'ORIGINATOR_CANCEL', 'RECOVERY_ON_TIMER_EXPIRE']:
            call_state = 'missed'
        ivr_menu_status = variables_value.get('ivr_menu_status', "")
        if ivr_menu_status in ['failure', 'timeout']:
            call_state = 'missed'
            disposition = 'IVR_TIMEOUT'
            pbx_feature_uuid = ivr_group
        cc_cause = variables_value.get('cc_cause', "")
        if cc_cause and cc_cause != 'answered':
            call_state = 'missed'
            disposition = 'CALLQUEUE_TIMEOUT'
        call_state_main = 'normal' if disposition == 'ORIGINATOR_CANCEL' else call_state
        
        call_type = variables_value.get('call_type', 'standard')
        #if call_type in ['standard', 'did']:
        #    direction = 'outbound'
        if is_inbound_call == "true":
            direction = "inbound"

        recording_path = ""
        is_recording_inserted = "false"  # ::yaksh::
        print("variables_value.get",variables_value.get('bridge_uuid', ""))
        print("variables_value.get",variables_value.get('sip_h_X-Autocalltype', ""))
        print("variables_value.get",variables_value.get('recording_directory', ""))
        if variables_value.get('custom_recording', '0') == '0':
            print('yaksh this is recording')
            if variables_value.get('current_application') == 'record_session':
                recording_path = variables_value.get('current_application_data', "")
            elif 'record_session' in variables_value.get('execute_on_answer', ""):
                recording_path = variables_value['execute_on_answer'].replace('record_session ', '')
                print('recording_path_in_execute_on_answer', recording_path)
            
            if variables_value.get('sip_h_X-Autocalltype') == 'true':
                dialed_user = variables_value.get('sip_to_user', "")
                recording_uuid = variables_value.get('bridge_uuid', "")
                recording_directory = variables_value.get('recording_directory', "")
                if recording_uuid:
                    recording_path = f"{recording_directory}/{recording_uuid}.wav"
                    # recording_path = f"{recording_directory}/{dialed_user}_{recording_uuid}.wav"   # ::yaksh::
            elif voice_broadcast and variables_value.get('recording_follow_transfer', '') == 'true':
                recording_directory = variables_value.get('recording_directory', "")
                if recording_directory:
                    recording_path = recording_directory
            elif variables_value.get('sip_h_X-auto_campaign_recording', '') == '0':   # ::yaksh::
                hangup_cause = manage_drop_call_flag(variables_value, lead_uuid, "AGENT_NOT_AVAILABLE","")
        
        #if recording_path:
            #print("this is the recording url", recording_path)
            #new_path = os.path.splitext(recording_path)[0] + '.wav'
            #result = subprocess.run(['ffmpeg', '-y', '-i', recording_path, '-ar', '44100', '-ac', '2', '-ab', '320k', new_path], 
                                 #stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            #print("ffmpeg Output:", result.stdout)
            #print("ffmpeg Errors:", result.stderr)
            #print("this is the new recording path", new_path)
            #if os.path.exists(recording_path):
                #os.remove(recording_path)
            #recording_path = new_path
        
        receiver_extension_uuid = variables_value.get('receiver_extension_uuid', "")
        # originate_disposition = variables_value.get('originate_disposition', "")
        if originate_disposition in ['NO_ANSWER', 'USER_BUSY']:
            call_state = 'missed'
            disposition = originate_disposition
            billsecond = 0

        #if receiver_extension_uuid:
        #    direction = 'outbound'
        incoming_extra_entry_flag = 0
        call_mode = '1' if campaign_uuid else '0'
        number_masking = '1'
        dial_method = ""
        print("campaiasdajsbjaksbdasdasdaaa",campaign_uuid , campaign_flag)
        #if campaign_uuid and campaign_flag:
        if campaign_uuid:
            result_outbound_campaign = check_number_masking(campaign_uuid, campaign_flag)
            print("reault outbounf campaijg ggg",result_outbound_campaign)
            if result_outbound_campaign:
                number_masking = result_outbound_campaign.get('number_masking', '1')
                dial_method = result_outbound_campaign.get('dial_method', "")
            print("dial method of OC", dial_method)
        #if campaign_uuid and campaign_flag and billsecond == 0 and disposition != 'ORIGINATOR_CANCEL':
        #if campaign_uuid and billsecond == 0 and disposition != 'ORIGINATOR_CANCEL':
        if campaign_uuid and disposition != 'ORIGINATOR_CANCEL':
            manage_lead_retries(variables_value, campaign_uuid, campaign_flag, hangup_cause)
        
        if destination_number and not lead_uuid and call_type != 'local':
        #if destination_number and not lead_uuid and user_uuid and call_type != 'local':
            lead_uuid = lead_entry_management(destination_number, user_uuid, tenant_uuid)
        
        auto_campaign = variables_value.get('sip_h_P-auto_campaign', "")
        predictive_campaign = variables_value.get('sip_h_P-predictive_campaign', "")
        if predictive_campaign == 'true':
            #destination_number = variables_value.get('sip_h_P-original_number', destination_number)
            predictive_flag = "0" if auto_call_flag == '1' else "1"
            predictive_uuid = str(uuid.uuid4())
            insert_predictive_campaign = {
                "uuid": predictive_uuid, "campaign_uuid": campaign_uuid,
                "agent_uuid": user_uuid, "tenant_uuid": tenant_uuid, "flag": predictive_flag
            }
            mydb["predictive_calculation"].insert_one(insert_predictive_campaign)
            print("\n", "insert_predictive_campaign::::", insert_predictive_campaign)
        
        if auto_campaign == 'true':
            user_uuid = variables_value.get("sip_h_P-agent_uuid", '') or user_uuid
            print("\n", "auto_campaign CHANGE Destination:::", destination_number)
            #destination_number = variables_value.get('sip_h_P-original_number', "") or destination_number;
            print("\n", "auto_campaign CHANGE Destination::: afterrrr", destination_number)
            caller_id_name = variables_value.get('Hunt-Callee-ID-Name', caller_id_name)
            caller_id_number = variables_value.get('Hunt-Orig-Caller-ID-Number', caller_id_number)
            number_masking = variables_value.get('sip_h_P-number_masking', number_masking)
        
        hangup_party_disposition = variables_value.get('sip_hangup_disposition', "")
        print("yaksh hangup party disposition", hangup_party_disposition)
        print("hangup direction", direction)

        disconnected_by = ''

        if hangup_party_disposition == 'recv_bye' and direction == 'outbound':
            disconnected_by = '1'
        if hangup_party_disposition == 'send_cancel' and direction == 'outbound':
            disconnected_by = '2'
        elif hangup_party_disposition == 'send_bye' and direction == 'outbound':
            disconnected_by = '0'
        elif hangup_party_disposition == 'recv_cancel' and direction == 'outbound':
            disconnected_by = '1'
        elif hangup_party_disposition == 'recv_refuse' and direction == 'outbound' and disposition == "NO_USER_RESPONSE":
            disconnected_by = '2'
        elif hangup_party_disposition == 'recv_refuse' and direction == 'outbound':
            disconnected_by = '1'
        elif hangup_party_disposition == 'recv_bye' and direction == 'inbound' and pbx_feature != "":
            disconnected_by = '1'
        # elif hangup_party_disposition == 'send_bye' and direction == 'inbound' and pbx_feature != "":
        #     disconnected_by = '2'
        elif hangup_party_disposition == 'send_bye' and direction == 'inbound' and is_inbound_call == "true":
            disconnected_by = '2'
        elif hangup_party_disposition == 'recv_bye' and direction == 'inbound' and is_inbound_call == "false":
            disconnected_by = '0'
            # direction = "outbound"
        elif hangup_party_disposition == 'send_bye' and direction == 'inbound' and is_inbound_call == "false":
            disconnected_by = '1'
            # direction = "outbound"
        elif hangup_party_disposition == 'recv_cancel' and direction == 'inbound' and is_inbound_call == "false":
            disconnected_by = '0'
            # direction = "outbound"
        elif hangup_party_disposition == 'send_refuse' and direction == 'inbound' and is_inbound_call == "false":
            disconnected_by = '2' # auto hangup from agent side.
            # direction = "outbound"
        elif hangup_party_disposition == 'recv_bye' and direction == 'inbound':
            disconnected_by = '1'
        #elif hangup_party_disposition == 'send_bye' and direction == 'inbound' and pbx_feature != "":
        #    disconnected_by = '2'
        elif hangup_party_disposition == 'send_bye' and direction == 'inbound':
            disconnected_by = '0'
        elif hangup_party_disposition == 'recv_refuse' and direction == 'inbound' and is_inbound_call == "true":
            disconnected_by = '0'

        #if hangup_party_disposition == 'send_bye':
        #    disconnected_by = '0' if direction == 'inbound' else '1'
        #elif hangup_party_disposition == 'recv_bye':
        #    disconnected_by = '1' if direction == 'inbound' else '0'
        #elif hangup_party_disposition == 'recv_cancel':
        #    disconnected_by = '1' if direction == 'inbound' else '0'
        #elif hangup_party_disposition == 'send_refuse':
        #    disconnected_by = '0' if direction == 'inbound' else '1'
        print("disconnected by", disconnected_by)

        if "webphonetransfer" in variables_value.get('sip_refer_to', '') or variables_value.get('webphonetransfer', '0') == '1':
            destination_number = variables_value.get('sip_to_user', destination_number)
            caller_id_number = variables_value.get('sip_user', variables_value.get('pre_transfer_caller_id_number', caller_id_number))
            
        if "_open" in destination_number:
            caller_id_number = variables_value.get('sip_user', caller_id_number)

        if "_open#PBX#" in destination_number:
            print("insideif  conddtion remobing #pbx#")
            destination_number = str(destination_number).split("_")[0]
        elif "#PBX#" in destination_number:
            print("insiderlifff conddtion remobing #pbx#")
            destination_number = str(destination_number).split("#")[0]

        # strip + from DID and Destination number
        destination_number = destination_number.lstrip('+')
        caller_id_number = caller_id_number.lstrip('+')

        is_csat = "1"
        if pbx_feature == "ivr":
            ivr_details = ivr_collection.find_one({"uuid": ivr_group, "tenant_uuid": tenant_uuid})
            if ivr_details:
                is_csat = ivr_details["is_csat"] or is_csat

        normalized = str(destination_number).split("_")[0]
        did_filter = { "$or": [ {"number": str(destination_number).split("_")[0]}, {"number": f"+{normalized}"}, {"number": destination_number}]}
        did_value = did_collection.find_one(did_filter)
        if did_value:
            direction = "inbound"
        else:
            direction = "outbound"
        normalized = ''
        
        # set user uuid to nil
        if auto_campaign == "true" and billsecond == 0:
            user_uuid = ""
            
        if variables_value.get('cc_agent', ""):
            cc_agent = variables_value.get('cc_agent', "").replace("@default", "").split(".")[0]
            ext_value = extension_collection.find_one({"tenant_uuid": tenant_uuid, "username": cc_agent})
            if ext_value:
                user_uuid = ext_value["user_uuid"]
        
        #telecom_circle = get_telecom_circle(tenant_uuid, destination_number if direction == 'outbound' else caller_id_number)
        telecom_circle = ''
        if ip_map_uuid or user_uuid != "" and voice_broadcast != "":
        #if ip_map_uuid or user_uuid != "" and voice_broadcast != "" or disposition in ['IVR_TIMEOUT', 'CALLQUEUE_TIMEOUT']:
            incoming_extra_entry_flag = 1
            if variables_value.get('sip_from_user'):
                caller_id_name = caller_id_number = variables_value['sip_from_user'].lstrip('+')
            insert_inbound_cdr_json = {
                "uuid": bridge_uuid, "user_uuid": user_uuid, "extension_uuid": extension_uuid,
                "tenant_uuid": tenant_uuid, "trunk_uuid": trunk_uuid, "sip_user": sip_user,
                "caller_id_name": caller_id_name, "caller_id_number": caller_id_number,
                "destination_number": destination_number, "billsecond": billsecond,
                "disposition": disposition, "direction": direction, "call_type": call_type,
                "callstart": callstart, "recording_path": recording_path, "pbx_feature": pbx_feature,
                "pbx_feature_uuid": pbx_feature_uuid, "authentication_type": authentication_type,
                "custom_callid": custom_callid, "ip_map_uuid": ip_map_uuid, "call_state": call_state_main,
                "lead_uuid": lead_uuid, "campaign_uuid": campaign_uuid, "call_mode": call_mode,
                "hangup_cause": hangup_cause, "auto_call_flag": auto_call_flag, "ivr_group": ivr_group,
                "disconnected_by": disconnected_by, "createdAt": created_at, "circle_uuid": telecom_circle,
                "callend": callend, "hold_time": holdsecond, "waiting_time": waitsecond, "total_duration": totalduration, 
                "ringing_time": ringingsecond, "ivr_feedback_score": feedbackScore, "isSend": 0, "is_csat": is_csat
            }
            # Add actual_talktime for inbound calls only
            if direction == "inbound":
                insert_inbound_cdr_json["actual_talktime"] = actual_talktime
            # compute call_count for today
            insert_inbound_cdr_json["call_count"] = compute_call_count(tenant_uuid, lead_uuid, caller_id_number, destination_number, callstart)
            inbound_cdrs_collection.insert_one(insert_inbound_cdr_json)
            # cdrs_collection.update_one({"uuid": insert_inbound_cdr_json["uuid"]}, {"$set": insert_inbound_cdr_json}, upsert= True)

            # change done by yaksh
            if custom_callid:
                cdrs_one_data = cdrs_collection.find_one({"custom_callid": custom_callid, "tenant_uuid": tenant_uuid})
                if cdrs_one_data:
                    insert_cdr_one_json = {
                        "user_uuid": user_uuid, "extension_uuid": extension_uuid,
                        "tenant_uuid": tenant_uuid, "trunk_uuid": trunk_uuid, "sip_user": sip_user,
                        "caller_id_name": caller_id_name, "caller_id_number": caller_id_number,
                        "destination_number": destination_number, "billsecond": billsecond,
                        "disposition": disposition, "direction": direction, "call_type": call_type,
                        "callstart": callstart, "recording_path": recording_path, "pbx_feature": pbx_feature,
                        "pbx_feature_uuid": pbx_feature_uuid, "authentication_type": authentication_type,
                        "custom_callid": custom_callid, "ip_map_uuid": ip_map_uuid, "call_state": call_state_main,
                        "lead_uuid": lead_uuid, "campaign_uuid": campaign_uuid, "call_mode": call_mode,
                        "number_masking": number_masking, "dial_method": dial_method, "hangup_cause": hangup_cause,
                        "auto_call_flag": auto_call_flag, "ivr_group": ivr_group, "disconnected_by": disconnected_by,
                        "createdAt": created_at, "circle_uuid": telecom_circle,
                        "callend": callend, "hold_time": holdsecond, "waiting_time": waitsecond, "total_duration": totalduration,
                        "ringing_time": ringingsecond, "ivr_feedback_score": feedbackScore, "isSend": 0, "is_csat": is_csat
                    }
                    print("\n", "firstttt  harsh CDR FOR INBOUND FAIL OR DIRECT OUTBOUND Insert iffff uodappadatettetete:::", insert_cdr_one_json)
                    if direction == "inbound":
                        insert_cdr_one_json["actual_talktime"] = actual_talktime
                    insert_cdr_one_json["call_count"] = compute_call_count(tenant_uuid, lead_uuid, caller_id_number, destination_number, callstart)
                    cdrs_collection.update_one({"custom_callid": insert_inbound_cdr_json["custom_callid"]}, {"$set": insert_cdr_one_json})
                else:
                    cdrs_collection.update_one({"uuid": insert_inbound_cdr_json["uuid"]}, {"$set": insert_inbound_cdr_json}, upsert= True)
            else:
                cdrs_collection.update_one({"uuid": insert_inbound_cdr_json["uuid"]}, {"$set": insert_inbound_cdr_json}, upsert= True)


            
            print("\n", "Inbound CDR Insert::::", insert_inbound_cdr_json)
            
            if tenant_uuid:
                webhook_data = webhook_collection.find_one({"tenant_uuid": tenant_uuid})
                print("yaksh webhook data", webhook_data)
                if webhook_data:
                    url = str(webhook_data['webhook_url'])
                    #url = str(webhook_data['web_hook_url'])
                    web_hook_data = {
                        'description': 'incoming calls notification from Belsmart',
                        'caller_id_name': caller_id_name, 'caller_id_number': caller_id_number,
                        'did': destination_number, 'date': callstart, 'duration': billsecond,
                        'call_status': call_state, 'answered_call': dialed_user
                    }
                    #if webhook_data['notify'] != '0' and call_state == 'missed' else {
                    #    'description': 'incoming calls notification from Belsmart',
                    #    'caller_id_name': caller_id_name, 'caller_id_number': caller_id_number,
                    #    'did': destination_number, 'date': callstart, 'duration': billsecond,
                    #    'call_status': call_state, 'answered_call': dialed_user
                    #}
                    print("\n", "response_webhooks::::", web_hook_data)
                    headers = {'Content-type': 'application/json'}
                    #response_webhooks = requests.post(url, data=json.dumps(web_hook_data), headers=headers)
                    #print("\n", "response_webhooks::::", response_webhooks)
        
        else:
            #if auto_campaign:
                #print("Auto Campaign call")
                #if variables_value.get('sip_h_X-Autocalltype') != 'true':
                #    user_uuid = ""
                #    print("Remove user_uuid in ")
            did_pstn = variables_value.get('did_pstn', "")
            print("outbound call destination number log", destination_number)
            insert_cdr_json = {
                "uuid": bridge_uuid, "user_uuid": user_uuid, "extension_uuid": extension_uuid,
                "tenant_uuid": tenant_uuid, "trunk_uuid": trunk_uuid, "sip_user": sip_user,
                "caller_id_name": caller_id_name, "caller_id_number": caller_id_number,
                "destination_number": destination_number, "billsecond": billsecond,
                "disposition": disposition, "direction": direction, "call_type": call_type,
                "callstart": callstart, "recording_path": recording_path, "pbx_feature": pbx_feature,
                "pbx_feature_uuid": pbx_feature_uuid, "authentication_type": authentication_type,
                "custom_callid": custom_callid, "ip_map_uuid": ip_map_uuid, "call_state": call_state_main,
                "lead_uuid": lead_uuid, "campaign_uuid": campaign_uuid, "call_mode": call_mode,
                "number_masking": number_masking, "dial_method": dial_method, "hangup_cause": hangup_cause,
                "auto_call_flag": auto_call_flag, "ivr_group": ivr_group, "disconnected_by": disconnected_by,
                "createdAt": created_at, "circle_uuid": telecom_circle,
                "callend": callend, "hold_time": holdsecond, "waiting_time": waitsecond, "total_duration": totalduration,
                "ringing_time": ringingsecond, "ivr_feedback_score": feedbackScore, "isSend": 0, "is_csat": is_csat
            }
            # Add actual_talktime for inbound calls only
            if direction == "inbound":
                insert_cdr_json["actual_talktime"] = actual_talktime
            # compute call_count for today
            insert_cdr_json["call_count"] = compute_call_count(tenant_uuid, lead_uuid, caller_id_number, destination_number, callstart)
            print("\n", "first harsh CDR Insert::::", insert_cdr_json)
               # FIX: Skip inserting the base record if an 'Extra' queue/ring_group record will be generated
            # skip_base_insert = pbx_feature in ['ring_group', 'call_queue'] and disposition not in ['IVR_TIMEOUT', 'CALLQUEUE_TIMEOUT']
            # if not skip_base_insert:
            #     cdrs_collection.update_one({"uuid": insert_cdr_json["uuid"]}, {"$set": insert_cdr_json}, upsert=True)
            # else:
            #     print("Skipped base CDR insert to avoid duplicates in reporting.")
            #cdrs_collection.insert_one(insert_cdr_json)

            if custom_callid:
                cdrs_one_data = cdrs_collection.find_one({"custom_callid": custom_callid, "tenant_uuid": tenant_uuid})
                if cdrs_one_data:
                    insert_cdr_one_json = {
                        "user_uuid": user_uuid, "extension_uuid": extension_uuid,
                        "tenant_uuid": tenant_uuid, "trunk_uuid": trunk_uuid, "sip_user": sip_user,
                        "caller_id_name": caller_id_name, "caller_id_number": caller_id_number,
                        "destination_number": destination_number, "billsecond": billsecond,
                        "disposition": disposition, "direction": direction, "call_type": call_type,
                        "callstart": callstart, "recording_path": recording_path, "pbx_feature": pbx_feature,
                        "pbx_feature_uuid": pbx_feature_uuid, "authentication_type": authentication_type,
                        "custom_callid": custom_callid, "ip_map_uuid": ip_map_uuid, "call_state": call_state_main,
                        "lead_uuid": lead_uuid, "campaign_uuid": campaign_uuid, "call_mode": call_mode,
                        "number_masking": number_masking, "dial_method": dial_method, "hangup_cause": hangup_cause,
                        "auto_call_flag": auto_call_flag, "ivr_group": ivr_group, "disconnected_by": disconnected_by,
                    main_lead_disposition = ni_disposition
                    insert_broadcast_string = {"createdAt": created_at, "callstart":callstart,"destination_number":destination_number,"caller_id_number":caller_id_number,"billsecond":billsecond,"total_duration": totalduration, "ringing_time": ringingsecond, "type":voicebroadcast_type,"hangup_cause":ni_disposition,"voicebroadcast_uuid":campaign_uuid,"tenant_uuid":tenant_uuid,"lead_uuid":lead_uuid,"uuid":broadcast_uuid}
                    print(insert_broadcast_string)
            elif voicebroadcast_type == '3':
                    #broad cast type = DNC
                    dnc_disposition = get_voicebroadcast_disposition(voicebroadcast_type,lead_uuid,campaign_uuid,billsecond)
                    dnc_disposition = str(dnc_disposition)
                    main_lead_disposition = dnc_disposition
                    insert_broadcast_string = {"createdAt": created_at, "callstart":callstart,"destination_number":destination_number,"caller_id_number":caller_id_number,"billsecond":billsecond,"total_duration": totalduration, "ringing_time": ringingsecond,"type":voicebroadcast_type,"hangup_cause":dnc_disposition,"voicebroadcast_uuid":campaign_uuid,"tenant_uuid":tenant_uuid,"lead_uuid":lead_uuid,"uuid":broadcast_uuid}
                    print(insert_broadcast_string)
            elif voicebroadcast_type == '':
                    hangup_cause = variables_value['sip_invite_failure_phrase'] if 'sip_invite_failure_phrase' in variables_value else ''
                    print("hangupc ausee voicebroarsctatsss", hangup_cause)
                    if hangup_cause == "":
                            hangup_cause = variables_value['sip_hangup_disposition'] if 'sip_hangup_disposition' in variables_value else ''
                    if hangup_cause != '':
                            hangup_disposition = get_voicebroadcast_disposition(hangup_cause,lead_uuid,campaign_uuid,billsecond)
                            print("hangupc ausee voicebroarsctatsss", hangup_disposition)
                            hangup_disposition = str(hangup_disposition)
                            main_lead_disposition = hangup_disposition
                            insert_broadcast_string = {"createdAt": created_at, "callstart":callstart,"destination_number":destination_number,"caller_id_number":caller_id_number,"billsecond":billsecond,"type":'',"hangup_cause":hangup_disposition,"voicebroadcast_uuid":campaign_uuid,"tenant_uuid":tenant_uuid,"lead_uuid":lead_uuid,"uuid":broadcast_uuid}
                            print(insert_broadcast_string)
            if insert_broadcast_string != '':
                    voicebroadcast_details_collection.insert_one(insert_broadcast_string)
                    filter = {"lead_management_uuid": lead_uuid}
                    update = {"$set": {"lead_status": main_lead_disposition}}
                    lead_mgmt_collection.update_one(filter, update)

        return jsonify({"status": "success", "message": "CDR processed successfully"}), 200
    
    except Exception as e:
        print(f"Error processing CDR: {e}")
        return jsonify({"error": str(e)}), 500

# Optional: Graceful shutdown to close MongoDB connection
# @app.teardown_appcontext
# def cleanup(exception=None):
#     print("Closing MongoDB connection")
#     if myclient is not None:
#         myclient.close()

if __name__ == '__main__':
    try:
        #app.run(host='10.180.67.151', port=8735, debug=True)
        app.run(host='0.0.0.0', port=8735, debug=True)
    except KeyboardInterrupt:
        print("Shutting down Flask application")
    finally:
        print("Closing MongoDB connection")
        if myclient is not None:
            myclient.close()
