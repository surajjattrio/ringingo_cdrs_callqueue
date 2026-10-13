from freeswitchESL import ESL
from datetime import datetime, timedelta
import pymongo
import json, sys, uuid, time, os
import urllib.parse
import re
import traceback
import resource
from config import mongo_url

# ============================================
# RAM LEAK FIX (2026-08-13): the freeswitchESL package ships ESLevent.__del__
# as a no-op ("__del__ = lambda self: None" in ESL.py), which permanently
# disables the SWIG-generated cleanup (__swig_destroy__ / delete_ESLevent) for
# every event object. Since con.recvEvent() allocates a new event each call in
# the loop below, none of that C-side memory was ever being freed -> RSS grew
# in lockstep with event_count, forever, never flattening (see [STATUS] logs).
# Fix: restore a real destructor, but only free events this wrapper actually
# owns (mine==1) - verified against a live test connection - so we don't
# double-free events FreeSWITCH's C side still owns (mine==0, e.g. borrowed
# references), which would crash the process instead of just leaking it.
def _esl_event_del(self):
    try:
        if getattr(self, "mine", 0):
            self.__class__.__swig_destroy__(self)
    except Exception:
        pass
ESL.ESLevent.__del__ = _esl_event_del
# ============================================

def word_exists(text, word):
    return bool(re.search(r'\b' + re.escape(word) + r'\b', text))

def get_header(event, key):
    """Cache-friendly header getter"""
    return event.getHeader(key) or ""

FS_HOST     = "103.36.121.196"
FS_PORT     = "8021"
FS_PASSWORD = "ClueCon"
# Was "ALL" - that means every FreeSWITCH event (HEARTBEAT every ~20s, PRESENCE_*, RE_SCHEDULE,
# LOG, MODULE_LOAD, etc.) got delivered here and processed even with zero active calls, which
# explains RAM/CPU churn at idle. This script only ever acts on the event names below (see the
# event_name == / in [...] checks further down) - restrict the subscription to just those so
# FreeSWITCH itself filters out the noise instead of us discarding it after allocating an event
# object for it.
FS_EVENTS   = ("CHANNEL_CREATE CHANNEL_EXECUTE CHANNEL_ORIGINATE CHANNEL_PROGRESS "
               "CHANNEL_ANSWER CHANNEL_BRIDGE CHANNEL_DESTROY CHANNEL_HANGUP "
               "CHANNEL_HANGUP_COMPLETE CALL_UPDATE CUSTOM")

# Reconnect backoff schedule (seconds): 1,2,3,4,5,10,15,20,25,30, then stays at 30 forever.
RECONNECT_DELAYS = [1, 2, 3, 4, 5, 10, 15, 20, 25, 30]

def connect_esl():
    """Blocks until FreeSWITCH ESL connects, retrying forever on the backoff
    schedule above. Re-subscribes to events on every (re)connect since a
    fresh ESLconnection has no subscriptions yet."""
    attempt = 0
    while True:
        print("Connecting to FreeSWITCH ESL...")
        connection = ESL.ESLconnection(FS_HOST, FS_PORT, FS_PASSWORD)
        if connection.connected():
            print("Successfully connected to FreeSWITCH!")
            connection.events("plain", FS_EVENTS)
            return connection
        delay = RECONNECT_DELAYS[min(attempt, len(RECONNECT_DELAYS) - 1)]
        print(f"[ESL] Connect failed, retrying in {delay}s...")
        time.sleep(delay)
        attempt += 1

con = connect_esl()

def connect_mongo():
    """Blocks until MongoDB is reachable at startup, retrying on the same
    backoff schedule as connect_esl(). Once connected, pymongo's own client
    handles reconnection for any later mid-run drops on its own (connection
    pooling + background server monitoring) - no extra retry loop needed
    for that part, only for this initial connect."""
    attempt = 0
    while True:
        try:
            print("Connecting to MongoDB...")
            client = pymongo.MongoClient(
                mongo_url,
                maxPoolSize=10,
                minPoolSize=2
            )
            client.admin.command('ping')
            print("Successfully connected to MongoDB!")
            return client
        except Exception as e:
            delay = RECONNECT_DELAYS[min(attempt, len(RECONNECT_DELAYS) - 1)]
            print(f"[MONGO] Connect failed ({e}), retrying in {delay}s...")
            time.sleep(delay)
            attempt += 1

myclient = connect_mongo()
mydb = myclient["db_pbxcc"]

# Collections
auto_originate_collection           = mydb['auto_campaign_originate']
realtime_report_collection          = mydb['realtime_report_count_mgmt']
inbound_report_collection           = mydb['inbound_realtime_count_mgmt']
manager_realtime_collection         = mydb['manager_realtime_count_mgmt']
manager_inbound_realtime_collection = mydb['manager_inbound_realtime_count_mgmt']

print("Call Agent Service Started...")
print("-" * 50)

# ============================================
# HELPER FUNCTIONS
# ============================================

def cleanup_stale_realtime_records():
    """Cleanup stuck realtime records older than 2 hours"""
    try:
        stale_time = datetime.now() - timedelta(hours=2)
        result = realtime_report_collection.delete_many({"inserted_at": {"$lt": stale_time}})
        if result.deleted_count > 0:
            print(f"[CLEANUP] Removed {result.deleted_count} stale realtime records")
    except Exception as e:
        print(f"[CLEANUP ERROR] {e}")

def realtime_safe_delete(call_uuid, lead_uuid):
    """Delete realtime record on hangup. Primary: call_uuid. Fallback: lead_uuid."""
    try:
        if call_uuid:
            result = realtime_report_collection.delete_one({"call_uuid": call_uuid})
            if result.deleted_count > 0:
                print(f"[REALTIME] DELETE (call_uuid) - {call_uuid[:8]}")
                return
        if lead_uuid:
            result = realtime_report_collection.delete_one({"lead_uuid": lead_uuid})
            if result.deleted_count > 0:
                print(f"[REALTIME] DELETE (lead_uuid fallback) - {lead_uuid[:8]}")
    except Exception as e:
        print(f"[REALTIME ERROR] realtime_safe_delete: {e}")

# ============================================
# RECORDING (unconditional - every bridged call, no campaign/header filters)
# ============================================

RECORDING_BASE_DIR = "/usr/local/freeswitch/recordings"

# Tracks uuids we've started recording, so CHANNEL_BRIDGE firing more than
# once for the same leg (e.g. re-bridge on transfer) doesn't stack multiple
# uuid_record starts on top of each other.
active_recordings = set()

def resolve_recording_directory(event):
    """Directory to record into. Prefers the dialplan-provided directory;
    falls back to the same {base}/{tenant_domain}/{date} layout the dialplan
    already uses, so a missing channel var never blocks recording."""
    recording_dir = get_header(event, "variable_recording_directory")
    if recording_dir:
        return recording_dir.rstrip("/")
    tenant_domain = (get_header(event, "variable_sip_h_P-tenant_domain") or
                      get_header(event, "variable_domain_name") or "unknown_domain")
    date_str = datetime.now().strftime("%Y-%m-%d")
    return f"{RECORDING_BASE_DIR}/{tenant_domain}/{date_str}"

def recording_filename_uuid(event, call_uuid):
    """Filename must match what cdrs_posting.py's upload/CDR lookup expects,
    or the recording becomes orphaned (exists on disk but never found/uploaded).
    cdrs_posting.py, for a CDR with sip_h_X-Autocalltype=='true', looks for
    "{recording_directory}/{bridge_uuid}.wav" - reading bridge_uuid from THAT
    leg's own CDR (i.e. the uuid it bridged to, its partner).
      - If this event's own leg IS the autocalltype leg: name the file after
        its bridge partner (its own variable_bridge_uuid) - this is exactly
        the value cdrs_posting.py will read back later. Matches.
      - Otherwise (this leg is the non-autocalltype/customer leg, or a plain
        call with neither flag): name it after this leg's own uuid - this
        matches the historical convention (the old code recorded the
        auto_campaign leg under its own uuid, which is what the autocalltype
        leg's bridge_uuid resolves to)."""
    if get_header(event, "variable_sip_h_X-Autocalltype") == "true":
        return (get_header(event, "variable_bridge_uuid") or
                get_header(event, "Other-Leg-Unique-ID") or call_uuid)
    return call_uuid

def start_recording(event):
    """Start recording on CHANNEL_BRIDGE for every bridged call - no
    campaign/header filters, so nothing is missed.

    If the dialplan is already recording this call itself (record_session
    app, or custom_recording explicitly not '0'), we still record it here
    too (per request) but into a dual_recording_audit/ subfolder so it's
    easy to count/audit how many calls end up double-recorded, without
    that pre-existing recording being disturbed."""
    call_uuid = get_header(event, "Unique-ID")
    if not call_uuid or call_uuid in active_recordings:
        return

    file_uuid = recording_filename_uuid(event, call_uuid)

    dialplan_already_recording = (
        get_header(event, "variable_current_application") == "record_session" or
        "record_session" in get_header(event, "variable_execute_on_answer") or
        get_header(event, "variable_custom_recording") not in ("", "0")
    )

    try:
        recording_dir = resolve_recording_directory(event)
        if dialplan_already_recording:
            recording_dir = f"{recording_dir}/dual_recording_audit"
        os.makedirs(recording_dir, exist_ok=True)
        recording_file = f"{recording_dir}/{file_uuid}.wav"
        response = con.api(f"uuid_record {call_uuid} start {recording_file}")
        body = response.getBody().strip() if response else ""
        if body.startswith("-ERR"):
            print(f"[RECORDING][FAIL] {call_uuid[:8]} -> {recording_file} | {body}")
        else:
            active_recordings.add(call_uuid)
            tag = " [DUAL-AUDIT]" if dialplan_already_recording else ""
            print(f"[RECORDING][START]{tag} {call_uuid[:8]} -> {recording_file}")
    except Exception as e:
        print(f"[RECORDING][ERROR] start {call_uuid[:8]}: {e}")

def stop_recording(call_uuid):
    """Stop any recording on this uuid at hangup. Unconditional/idempotent -
    safe to call even if this uuid was never recording."""
    if not call_uuid:
        return
    try:
        response = con.api(f"uuid_record {call_uuid} stop all")
        body = response.getBody().strip() if response else ""
        was_tracked = call_uuid in active_recordings
        active_recordings.discard(call_uuid)
        if was_tracked or not body.startswith("-ERR"):
            print(f"[RECORDING][STOP] {call_uuid[:8]} | {body}")
    except Exception as e:
        print(f"[RECORDING][ERROR] stop {call_uuid[:8]}: {e}")

# ============================================
# MAIN EVENT LOOP
# ============================================

last_realtime_cleanup_time = time.time()
realtime_cleanup_interval  = 300  # cleanup stale records every 5 minutes
event_count = 0

while True:
    try:
        if not con.connected():
            print("[ESL] Connection lost, reconnecting...")
            con = connect_esl()
            continue

        event = con.recvEvent()
        if not event:
            if not con.connected():
                print("[ESL] Connection lost, reconnecting...")
                con = connect_esl()
            else:
                time.sleep(0.1)
            continue

        event_count += 1
        event_name = get_header(event, "Event-Name")

        if event_count % 100 == 0:
            # ru_maxrss is peak RSS in KB on Linux - logging it against event_count lets us
            # tell a genuine per-event leak (RSS keeps climbing in lockstep with events, never
            # flattens) apart from normal warm-up (Mongo pool fill, allocator arena growth,
            # which plateaus after the first few thousand events and then stays flat).
            rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
            print(f"[STATUS] Processed {event_count} events | RSS={rss_mb:.1f}MB")

        # Periodic stale records cleanup
        current_time = time.time()
        if current_time - last_realtime_cleanup_time > realtime_cleanup_interval:
            cleanup_stale_realtime_records()
            last_realtime_cleanup_time = current_time

        # ============================================
        # INBOUND CALL QUEUE HANDLERS (unchanged)
        # ============================================

        if event_name == "CHANNEL_EXECUTE" and get_header(event, "variable_sip_h_X-inbound_campaign_flag") == 'true':
            lead_uuid   = get_header(event, "variable_sip_h_X-Leaduuid")
            tenant_uuid = get_header(event, "variable_sip_h_X-tenant_uuid")
            if lead_uuid and tenant_uuid and get_header(event, "Application") == 'callcenter':
                if not inbound_report_collection.find_one({"lead_uuid": lead_uuid}):
                    inbound_report_collection.insert_one({
                        "uuid": str(uuid.uuid4()),
                        "lead_uuid": lead_uuid,
                        "tenant_uuid": tenant_uuid,
                        "type": 1
                    })

        if event_name == "CHANNEL_CREATE" and get_header(event, "variable_sip_h_X-inbound_campaign_flag") == 'true':
            lead_uuid = get_header(event, "variable_sip_h_X-Leaduuid")
            if lead_uuid and get_header(event, "variable_sip_h_X-tenant_uuid"):
                existing = inbound_report_collection.find_one({"lead_uuid": lead_uuid})
                if existing:
                    inbound_report_collection.update_one(
                        {"lead_uuid": lead_uuid},
                        {"$set": {"type": 0}}
                    )

        # ============================================
        # CUSTOM EVENT HANDLERS (unchanged)
        # ============================================

        if event_name == "CUSTOM":
            event_subclass = get_header(event, "Event-Subclass")
            if event_subclass in ["X_FER_HANGUP", "A_LEG_HANGUP"]:
                conference_uuid = get_header(event, "conference_uuid")
                if conference_uuid:
                    con.api(f'uuid_kill {conference_uuid}')

        if event_name == "CUSTOM" and get_header(event, "Beep-Status") == "DETECTED":
            unique_id = get_header(event, "Unique-ID")
            if unique_id:
                con.api(f'uuid_kill {unique_id}')
                print(f"[BEEP] Hangup command sent")

        # ============================================
        # THREE WAY / attended_xfer handler (unchanged)
        # ============================================

        if ((event_name == "CHANNEL_EXECUTE" and get_header(event, "variable_current_application") == 'three_way') or
                (event_name == "CALL_UPDATE" and get_header(event, "Other-Leg-Destination-Number") == 'attended_xfer')):
            try:
                response    = con.api('show registrations as json')
                parsed_data = json.loads(response.getBody())
                rows        = parsed_data.get("rows", [])
                check_var   = get_header(event, "variable_original_destination_number")
                if get_header(event, "Other-Leg-Direction") == 'inbound':
                    check_var = get_header(event, "Other-Leg-Username")
                register_user = None
                final_string  = None
                for row in rows:
                    if word_exists(row.get("reg_user", ""), check_var):
                        register_user = row.get("reg_user")
                        final_string  = f"sip:{row.get('reg_user')}@{row.get('network_ip')}:{row.get('network_port')};transport=wss"
                        break
                if register_user and final_string:
                    sip_from_uri    = f"sip:{get_header(event, 'variable_sip_from_uri')}"
                    x_fer_uuid      = get_header(event, "Unique-ID")
                    a_leg_call_uuid = get_header(event, "variable_sip_h_X-a_leg_call_uuid")
                    evt = ESL.ESLevent("SWITCH_EVENT_NOTIFY")
                    evt.addHeader("profile",       'external')
                    evt.addHeader("from-uri",      sip_from_uri)
                    evt.addHeader("to-uri",        final_string)
                    evt.addHeader("user",          "202")
                    evt.addHeader("host",          get_header(event, "variable_sip_from_host"))
                    evt.addHeader("X-XFER-UUID",   x_fer_uuid)
                    evt.addHeader("extra-headers", f"X-XFER-UUID:{x_fer_uuid},X-A-LEG-UUID:{a_leg_call_uuid}")
                    con.sendEvent(evt)
                    del evt
            except Exception as e:
                print(f"[THREE_WAY ERROR] {e}")

        # ============================================
        # REALTIME REPORT HANDLERS
        # ============================================

        # CHANNEL_ORIGINATE → INSERT type:3 (Dialing)
        # Tracks ALL call types (not just auto campaign)
        # Skips inbound direction and agent legs to prevent duplicate counting
        if event_name == "CHANNEL_ORIGINATE":
            call_uuid       = get_header(event, "Unique-ID")
            direction       = get_header(event, "Call-Direction")
            auto_campaign   = get_header(event, "variable_sip_h_P-auto_campaign")
            auto_agent_flag = get_header(event, "variable_sip_h_P-auto_agent_flag")
            tenant_uuid     = (get_header(event, "variable_sip_h_P-tenant_uuid") or
                               get_header(event, "variable_tenant_uuid") or "")
            lead_uuid       = (get_header(event, "variable_sip_h_P-lead_uuid") or
                               get_header(event, "variable_sip_h_X-Leaduuid") or "")
            campaign_uuid   = (get_header(event, "variable_sip_h_P-campaign_uuid") or
                               get_header(event, "variable_sip_h_X-selectedcampaignuuid") or "")
            callstart       = (get_header(event, "variable_sip_h_P-call_start") or
                               datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            manager_uuid    = get_header(event, "variable_sip_h_P-manager_uuid")

            # Skip inbound direction and agent legs
            if direction != "inbound" and auto_agent_flag != 'auto_agent':
                if call_uuid and tenant_uuid:
                    # Guard 1: skip if call_uuid already tracked
                    if not realtime_report_collection.find_one({"call_uuid": call_uuid}):
                        # Guard 2: skip if lead already has active record (prevents B-leg duplicate)
                        if not (lead_uuid and realtime_report_collection.find_one({"lead_uuid": lead_uuid})):
                            insert_doc = {
                                "uuid":          str(uuid.uuid4()),
                                "call_uuid":     call_uuid,
                                "lead_uuid":     lead_uuid,
                                "campaign_uuid": campaign_uuid,
                                "tenant_uuid":   tenant_uuid,
                                "auto_campaign": auto_campaign,
                                "direction":     direction,
                                "type":          3,           # Dialing
                                "created_at":    callstart,
                                "inserted_at":   datetime.now()
                            }
                            if manager_uuid:
                                insert_doc["manager_uuid"] = manager_uuid
                            realtime_report_collection.insert_one(insert_doc)
                            print(f"[REALTIME] INSERT type:3 Dialing - {call_uuid[:8]}")

        # CHANNEL_PROGRESS → UPDATE type:0 (Ringing)
        if event_name == "CHANNEL_PROGRESS":
            call_uuid = get_header(event, "Unique-ID")
            if call_uuid:
                result = realtime_report_collection.update_one(
                    {"call_uuid": call_uuid},
                    {"$set": {"type": 0}}
                )
                if result.modified_count > 0:
                    print(f"[REALTIME] UPDATE type:0 Ringing - {call_uuid[:8]}")

        # CHANNEL_ANSWER → UPDATE type:2 (On Call)
        # Skips agent leg using auto_agent_flag and autocall_type headers
        if event_name == "CHANNEL_ANSWER":
            auto_campaign     = get_header(event, "variable_sip_h_P-auto_campaign")
            inbound_flag      = get_header(event, "variable_sip_h_X-inbound_campaign_flag")
            call_uuid         = get_header(event, "Unique-ID")
            call_direction    = get_header(event, "Call-Direction")
            auto_agent_flag   = get_header(event, "variable_sip_h_P-auto_agent_flag")
            autocall_type     = get_header(event, "variable_sip_h_X-Autocalltype")
            lead_uuid_ans     = (get_header(event, "variable_sip_h_P-lead_uuid") or
                                 get_header(event, "variable_sip_h_X-Leaduuid") or "")
            tenant_uuid_ans   = (get_header(event, "variable_sip_h_P-tenant_uuid") or
                                 get_header(event, "variable_tenant_uuid") or "")
            campaign_uuid_ans = (get_header(event, "variable_sip_h_P-campaign_uuid") or
                                 get_header(event, "variable_sip_h_X-selectedcampaignuuid") or "")

            # Detect agent leg - skip realtime update for agent leg only
            is_agent = (
                auto_agent_flag == 'auto_agent' or
                autocall_type == 'true' or
                (call_direction == 'inbound' and auto_campaign != 'true')
            )

            if call_uuid and not is_agent:
                # Step 1: update by call_uuid (normal path)
                result = realtime_report_collection.update_one(
                    {"call_uuid": call_uuid},
                    {"$set": {"type": 2, "answered_at": datetime.now()}}
                )
                if result.matched_count > 0:
                    print(f"[REALTIME] UPDATE type:2 OnCall (call_uuid) - {call_uuid[:8]}")
                else:
                    # Step 2: fallback by lead_uuid
                    if lead_uuid_ans:
                        result = realtime_report_collection.update_one(
                            {"lead_uuid": lead_uuid_ans},
                            {"$set": {"type": 2, "answered_at": datetime.now()}}
                        )
                        if result.matched_count > 0:
                            print(f"[REALTIME] UPDATE type:2 OnCall (lead_uuid) - {call_uuid[:8]}")
                        else:
                            # Step 3: insert new record (pure inbound/direct call)
                            if tenant_uuid_ans:
                                realtime_report_collection.insert_one({
                                    "uuid":          str(uuid.uuid4()),
                                    "call_uuid":     call_uuid,
                                    "lead_uuid":     lead_uuid_ans,
                                    "campaign_uuid": campaign_uuid_ans,
                                    "tenant_uuid":   tenant_uuid_ans,
                                    "auto_campaign": auto_campaign,
                                    "type":          2,
                                    "answered_at":   datetime.now(),
                                    "inserted_at":   datetime.now()
                                })
                                print(f"[REALTIME] INSERT type:2 OnCall (new) - {call_uuid[:8]}")

            # Auto campaign specific logic (AMD, agent state) - unchanged.
            # Recording itself now happens unconditionally on CHANNEL_BRIDGE (below).
            if auto_campaign == 'true':
                auto_campaign_originate = get_header(event, "variable_sip_h_P-auto_campaign_originate")

                amd_status = get_header(event, "variable_sip_h_P-amd_status")
                if amd_status == 'true' and not auto_campaign_originate:
                    try:
                        progresstime_str      = get_header(event, "Caller-Channel-Progress-Time")
                        progressmediatime_str = get_header(event, "Caller-Channel-Progress-Media-Time")
                        answertime_str        = get_header(event, "Caller-Channel-Answered-Time")
                        createtimestr         = get_header(event, "Caller-Channel-Created-Time")
                        if all([progresstime_str, progressmediatime_str, answertime_str, createtimestr]):
                            answertime        = datetime.fromtimestamp(int(answertime_str) / 1000000)
                            createtime        = datetime.fromtimestamp(int(createtimestr) / 1000000)
                            progresstime      = datetime.fromtimestamp(int(progresstime_str) / 1000000)
                            progressmediatime = datetime.fromtimestamp(int(progressmediatime_str) / 1000000)
                            if int(progressmediatime_str) > 0:
                                seconds_difference = (answertime - progressmediatime).total_seconds()
                            elif int(progresstime_str) > 0:
                                seconds_difference = (answertime - progresstime).total_seconds()
                            else:
                                seconds_difference = (answertime - createtime).total_seconds()
                            hangup_ranges = [[0.0, 1.50], [23.00, 24.00], [30.00, 31.00]]
                            should_hangup = any(start < seconds_difference < end for start, end in hangup_ranges)
                            if should_hangup:
                                campaign_uuid_amd = get_header(event, "variable_sip_h_P-campaign_uuid")
                                user_uuid         = get_header(event, "variable_sip_h_P-user_uuid")
                                if campaign_uuid_amd and user_uuid:
                                    auto_originate_collection.update_one(
                                        {"campaign_uuid": campaign_uuid_amd, "agent_uuid": user_uuid},
                                        {"$set": {"flag": '0'}}
                                    )
                                    print(f"[AMD] Set flag=0 for campaign")
                    except Exception as e:
                        print(f"[AMD] Error: {e}")

                manager_uuid  = get_header(event, "variable_sip_h_P-manager_uuid")
                lead_uuid_mgr = get_header(event, "variable_sip_h_P-lead_uuid")
                if manager_uuid and lead_uuid_mgr:
                    if manager_realtime_collection.find_one({"lead_uuid": lead_uuid_mgr}):
                        manager_realtime_collection.update_one(
                            {"lead_uuid": lead_uuid_mgr},
                            {"$set": {"type": 2}}
                        )

                username      = get_header(event, "variable_sip_h_P-username")
                tenant_domain = get_header(event, "variable_sip_h_P-tenant_domain")
                if username and tenant_domain:
                    con.api(f"callcenter_config agent set state {username}.{tenant_domain}@default 'In a queue call'")

            if inbound_flag == 'true':
                lead_uuid_inb = get_header(event, "variable_sip_h_X-Leaduuid")
                if lead_uuid_inb and inbound_report_collection.find_one({"lead_uuid": lead_uuid_inb}):
                    inbound_report_collection.update_one(
                        {"lead_uuid": lead_uuid_inb},
                        {"$set": {"type": 2}}
                    )

        # CHANNEL_BRIDGE → start recording (unconditional, every bridge)
        if event_name == "CHANNEL_BRIDGE":
            start_recording(event)

        # CHANNEL_DESTROY / HANGUP → DELETE realtime record
        if event_name in ["CHANNEL_DESTROY", "CHANNEL_HANGUP", "CHANNEL_HANGUP_COMPLETE"]:
            hangup_cause  = get_header(event, "Hangup-Cause") or "UNKNOWN"
            cc_side       = get_header(event, "variable_cc_side")
            agent_uuid    = get_header(event, "Unique-ID")
            caller_uuid   = get_header(event, "variable_cc_member_session_uuid")
            call_uuid_hup = get_header(event, "Unique-ID")
            lead_uuid_hup = (get_header(event, "variable_sip_h_P-lead_uuid") or
                             get_header(event, "variable_sip_h_X-Leaduuid") or "")

            if hangup_cause == "NO_USER_RESPONSE" and cc_side == "agent":
                print("[NO_USER_RESPONSE] Agent did not answer → killing both legs")
                if agent_uuid:
                    con.api(f"uuid_kill {agent_uuid} NO_USER_RESPONSE")
                if caller_uuid:
                    con.api(f"uuid_kill {caller_uuid} NO_USER_RESPONSE")

            # Delete for ALL call types
            realtime_safe_delete(call_uuid_hup, lead_uuid_hup)

            # Stop recording (unconditional - matches the unconditional start on CHANNEL_BRIDGE)
            stop_recording(call_uuid_hup)

            if get_header(event, "variable_sip_h_P-auto_campaign") == 'true':
                username      = get_header(event, "variable_sip_h_P-username")
                tenant_domain = get_header(event, "variable_sip_h_P-tenant_domain")
                if username and tenant_domain:
                    con.api(f"callcenter_config agent set state {username}.{tenant_domain}@default 'Idle'")

            if get_header(event, "variable_sip_h_X-inbound_campaign_flag") == 'true':
                lead_uuid_inb = get_header(event, "variable_sip_h_X-Leaduuid")
                if lead_uuid_inb:
                    inbound_report_collection.delete_one({"lead_uuid": lead_uuid_inb})

    except KeyboardInterrupt:
        print("\nShutting down...")
        sys.exit(0)
    except Exception as e:
        print(f"[MAIN LOOP ERROR] {e}")
        traceback.print_exc()
        # recvEvent() raises (not just returns None) once the underlying socket
        # actually dies - reconnect immediately instead of retrying forever on
        # a dead connection object.
        if not con.connected():
            print("[ESL] Connection lost after error, reconnecting...")
            con = connect_esl()
        else:
            time.sleep(1)
