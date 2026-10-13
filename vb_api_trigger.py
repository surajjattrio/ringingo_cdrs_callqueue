"""
Voice broadcast API trigger.

"""
import re
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone

import pymongo
import requests

from config import mongo_url, database_name
from call_feedback import _render_template, _build_curl, _evaluate_success

_client = pymongo.MongoClient(mongo_url)
_db = _client[database_name]
vb_api_config_collection = _db["voicebroadcast_api_config"]
vb_api_logs_collection = _db["voicebroadcast_api_logs"]
voicebroadcast_details_collection = _db["voicebroadcast_details"]

# Hard caps regardless of what a tenant configures — a stalled provider or a huge retry
# count must never keep a thread alive indefinitely.
TIMEOUT_HARD_CAP_MS = 10000
DEFAULT_TIMEOUT_MS = 5000
MAX_RETRIES_HARD_CAP = 10
DEFAULT_RETRY_DELAY_SECONDS = 5
RETRY_DELAY_HARD_CAP_SECONDS = 300
DEFAULT_COUNTRY_CODE = "91"

try:
    vb_api_config_collection.create_index([("tenant_uuid", 1), ("voicebroadcast_uuid", 1), ("is_active", 1)])
    vb_api_logs_collection.create_index([("call_uuid", 1)])
    vb_api_logs_collection.create_index([("channel_uuid", 1)])
    vb_api_logs_collection.create_index([("config_uuid", 1), ("contact_number", 1), ("outcome", 1)])
    vb_api_logs_collection.create_index([("destination_number", 1)])
    vb_api_logs_collection.create_index([("tenant_uuid", 1), ("voicebroadcast_uuid", 1), ("createdAt", -1)])
except Exception as e:
    print(f"[VB_API] Failed to ensure indexes: {e}")


# ============================================================
# Helpers — number format, logging
# ============================================================

def normalize_contact_number(number, country_code=DEFAULT_COUNTRY_CODE):
    """Returns the number in +<countrycode><number> form, or "" if it can't be made valid.
    Already-valid numbers are returned unchanged; only a missing "+" / country code is added.
    """
    cleaned = re.sub(r"[\s\-().]", "", str(number or ""))
    cc = re.sub(r"\D", "", str(country_code or DEFAULT_COUNTRY_CODE))

    if cleaned.startswith("+"):
        rest = cleaned[1:]
        return cleaned if rest.isdigit() and 8 <= len(rest) <= 15 else ""
    if not cleaned.isdigit():
        return ""
    if cleaned.startswith(cc) and len(cleaned) == len(cc) + 10:
        return "+" + cleaned
    if len(cleaned) == 10:
        return "+" + cc + cleaned
    if cleaned.startswith("0") and len(cleaned) == 11:
        return "+" + cc + cleaned[1:]
    return ""


def _create_log(config, call, outcome, message="", contact_number="", max_retries=0, request=None, curl=""):
    """The one voicebroadcast_api_logs document for this config + call. Created before the
    first attempt (outcome "in_progress"), or directly as "skipped". Stored exactly as sent —
    nothing redacted. Never raises."""
    log_uuid = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    try:
        vb_api_logs_collection.insert_one({
            "uuid": log_uuid,
            "source": "cdr",
            "config_uuid": config.get("uuid", ""),
            "config_name": config.get("name", ""),
            "tenant_uuid": call["tenant_uuid"],
            "voicebroadcast_uuid": call["voicebroadcast_uuid"],
            "call_uuid": call["call_uuid"],
            "channel_uuid": call["channel_uuid"],
            "broadcast_uuid": call["broadcast_uuid"],
            "lead_uuid": call["lead_uuid"],
            "destination_number": call["destination_number"],
            "contact_number": contact_number,
            "max_retries": max_retries,
            "total_attempts": 0,
            "outcome": outcome,
            "message": message,
            "request": request,
            "curl": curl,
            "response": None,
            "duration_ms": 0,
            "attempts": [],
            "createdAt": now,
            "updatedAt": now,
        })
    except Exception as e:
        print(f"[VB_API] Failed to write voicebroadcast_api_logs document: {e}")
    return log_uuid


def _record_attempt(log_uuid, attempt, outcome, message, response, duration_ms, final):
    """Appends one try to the document's `attempts` array and mirrors it onto the top-level
    outcome/message/response, so the document always shows the latest state. While retries
    remain, a failed try leaves outcome "in_progress". Never raises."""
    now = datetime.now(timezone.utc)
    try:
        vb_api_logs_collection.update_one(
            {"uuid": log_uuid},
            {
                "$push": {"attempts": {
                    "attempt": attempt,
                    "outcome": outcome,
                    "message": message,
                    "response": response,
                    "duration_ms": duration_ms,
                    "createdAt": now,
                }},
                "$set": {
                    "outcome": outcome if final else "in_progress",
                    "message": message,
                    "response": response,
                    "total_attempts": attempt,
                    "updatedAt": now,
                },
                "$inc": {"duration_ms": duration_ms},
            },
        )
    except Exception as e:
        print(f"[VB_API] Failed to record attempt {attempt} on voicebroadcast_api_logs {log_uuid}: {e}")


def _stamp_broadcast_detail(broadcast_uuid, status, log_uuid):
    if not broadcast_uuid:
        return
    try:
        voicebroadcast_details_collection.update_one(
            {"uuid": broadcast_uuid},
            {"$set": {"api_trigger_status": status, "api_log_uuid": log_uuid}},
        )
    except Exception as e:
        print(f"[VB_API] Failed to stamp api_trigger_status on voicebroadcast_details {broadcast_uuid}: {e}")


def _clamp_int(value, default, low, high):
    try:
        value = int(value)
    except (TypeError, ValueError):
        value = default
    return max(low, min(value, high))


# ============================================================
# Sending — one config, with retries
# ============================================================

def _send_once(method, url, params, headers, body, timeout_ms):
    """Returns (status_code, response_headers, response_body, error_message)."""
    content_type = next((str(v).lower() for k, v in (headers or {}).items() if k.lower() == "content-type"), "")
    kwargs = {"params": params, "headers": headers, "timeout": timeout_ms / 1000}
    if body and method != "GET":
        if "x-www-form-urlencoded" in content_type:
            kwargs["data"] = body
        else:
            kwargs["json"] = body
    try:
        response = requests.request(method, url, **kwargs)
    except requests.exceptions.RequestException as e:
        return None, None, None, str(e)

    try:
        response_body = response.json()
    except Exception:
        response_body = response.text
    return response.status_code, dict(response.headers), response_body, None


# Per-config flow, printed as "step N/5" so a log line shows exactly how far a request got:
#   1 number_check  2 duplicate_check  3 build_request  4 send (one line per attempt)  5 finish
TOTAL_STEPS = 5


def _step(tag, step, name, status, detail=""):
    print(f"{tag} step {step}/{TOTAL_STEPS} {name} -> {status}" + (f" | {detail}" if detail else ""))


def _run_config(config, call):
    config_uuid = config.get("uuid", "")
    tag = f"[VB_API] config={config_uuid} call={call['call_uuid']} channel={call['channel_uuid'] or '-'}"
    step, step_name = 0, "start"
    try:
        print(f"{tag} started | name=\"{config.get('name', '')}\" thread={threading.current_thread().name}")

        step, step_name = 1, "number_check"
        country_code = config.get("default_country_code") or DEFAULT_COUNTRY_CODE
        contact_number = normalize_contact_number(call["destination_number"], country_code)
        if not contact_number:
            _step(tag, step, step_name, "SKIPPED",
                  f"reason=invalid_number | raw=\"{call['destination_number']}\" country_code={country_code} "
                  f"(expected +<8-15 digits>, {country_code}+10 digits, 10 digits, or 0+10 digits)")
            log_uuid = _create_log(config, call, "skipped", "invalid_number")
            _stamp_broadcast_detail(call["broadcast_uuid"], "skipped", log_uuid)
            return
        if contact_number == call["destination_number"]:
            _step(tag, step, step_name, "ok", f"number already valid: {contact_number}")
        else:
            _step(tag, step, step_name, "ok",
                  f"normalized \"{call['destination_number']}\" -> {contact_number} (country_code={country_code})")

        step, step_name = 2, "duplicate_check"
        # One successful send per customer per config: matched on config_uuid + the normalized
        # number only, so a retried / re-dialled lead or a re-posted CDR never gets it twice.
        already_sent = vb_api_logs_collection.find_one(
            {"config_uuid": config_uuid, "contact_number": contact_number, "outcome": "success"},
            {"uuid": 1, "call_uuid": 1, "createdAt": 1},
        )
        if already_sent:
            _step(tag, step, step_name, "SKIPPED",
                  f"reason=already_sent | {contact_number} already received this config's request "
                  f"(log={already_sent.get('uuid', '')} call={already_sent.get('call_uuid', '')} at={already_sent.get('createdAt', '')})")
            _create_log(config, call, "skipped", "already_sent", contact_number=contact_number)
            return
        _step(tag, step, step_name, "ok", f"{contact_number} has not received this config's request before")

        step, step_name = 3, "build_request"
        variables = {
            "contact_number": contact_number,
            "destination_number": call["destination_number"],
            "caller_id_number": call["caller_id_number"],
            "call_uuid": call["call_uuid"],
            "channel_uuid": call["channel_uuid"],
            "broadcast_uuid": call["broadcast_uuid"],
            "lead_uuid": call["lead_uuid"],
            "tenant_uuid": call["tenant_uuid"],
            "voicebroadcast_uuid": call["voicebroadcast_uuid"],
            "billsec": call["billsec"],
            "callstart": call["callstart"],
            "dtmf": call["dtmf"],
            "disposition": call["disposition"],
        }

        method = str(config.get("method") or "POST").upper()
        url = _render_template(str(config.get("url") or ""), variables)
        headers = _render_template(config.get("headers") or {}, variables)
        query_params = _render_template(config.get("query_params") or {}, variables)
        body = _render_template(config.get("body_template") or {}, variables)
        timeout_ms = _clamp_int(config.get("timeout_ms"), DEFAULT_TIMEOUT_MS, 1, TIMEOUT_HARD_CAP_MS)
        max_retries = _clamp_int(config.get("max_retries"), 0, 0, MAX_RETRIES_HARD_CAP)
        retry_delay = _clamp_int(config.get("retry_delay_seconds"), DEFAULT_RETRY_DELAY_SECONDS, 0, RETRY_DELAY_HARD_CAP_SECONDS)
        if not url:
            _step(tag, step, step_name, "WARNING", "config has an empty url - the send will fail")

        request_doc = {"method": method, "url": url, "params": query_params, "headers": headers, "body": body}
        curl_command = _build_curl(method, url, query_params, headers, body)
        log_uuid = _create_log(
            config, call, "in_progress", contact_number=contact_number, max_retries=max_retries,
            request=request_doc, curl=curl_command,
        )
        _step(tag, step, step_name, "ok",
              f"log={log_uuid} {method} {url} timeoutMs={timeout_ms} maxRetries={max_retries} "
              f"retryDelay={retry_delay}s totalAttemptsAllowed={max_retries + 1}")
        print(f"{tag} curl: {curl_command}")

        step, step_name = 4, "send"
        outcome, error_message, attempt, total_duration_ms = "failure", None, 0, 0
        for attempt in range(1, max_retries + 2):
            started_at = time.monotonic()
            status_code, response_headers, response_body, error_message = _send_once(
                method, url, query_params, headers, body, timeout_ms
            )
            duration_ms = int((time.monotonic() - started_at) * 1000)
            total_duration_ms += duration_ms

            success = status_code is not None and _evaluate_success(config, status_code, response_body)
            outcome = "success" if success else "failure"
            if success:
                reason = ""
            elif status_code is None:
                reason = " | reason=network_error_or_timeout"
            else:
                reason = " | reason=unsuccessful_response"
                if not error_message:
                    condition = config.get("success_condition") or {"statusMin": 200, "statusMax": 299}
                    error_message = f"HTTP {status_code} (success_condition={condition})"
            _step(tag, step, step_name, outcome.upper(),
                  f"attempt {attempt}/{max_retries + 1} status={status_code} durationMs={duration_ms}"
                  + reason + (f" error={error_message}" if error_message else "") + f" response={response_body}")

            # Any failure (network error, timeout, any non-success HTTP status, or a failed
            # success_condition) is retried, up to max_retries extra attempts.
            final = success or attempt > max_retries
            _record_attempt(
                log_uuid, attempt, outcome,
                message="sent" if success else error_message,
                response={"status_code": status_code, "headers": response_headers, "body": response_body},
                duration_ms=duration_ms, final=final,
            )
            if final:
                break
            _step(tag, step, step_name, "RETRY", f"waiting {retry_delay}s before attempt {attempt + 1}/{max_retries + 1}")
            time.sleep(retry_delay)

        step, step_name = 5, "finish"
        _stamp_broadcast_detail(call["broadcast_uuid"], outcome, log_uuid)
        if outcome == "success":
            summary = f"sent on attempt {attempt}/{max_retries + 1}"
        else:
            summary = f"all {attempt} attempt(s) failed, no retries left | lastError={error_message}"
        stamped = f"voicebroadcast_details={call['broadcast_uuid']} stamped" if call["broadcast_uuid"] else "no voicebroadcast_details row to stamp"
        _step(tag, step, step_name, outcome.upper(), f"{summary} | totalSendMs={total_duration_ms} log={log_uuid} {stamped}")
    except Exception:
        print(f"{tag} step {step}/{TOTAL_STEPS} {step_name} -> CRASHED | unexpected error, request abandoned:\n{traceback.format_exc()}")


# ============================================================
# Public entry point — called from cdrs_posting.py
# ============================================================

def trigger_vb_api(tenant_uuid, voicebroadcast_uuid, call_uuid, destination_number, caller_id_number="",
                   lead_uuid="", broadcast_uuid="", billsec=0, callstart="", dtmf="", disposition="",
                   channel_uuid=""):
    """Fires every active config for this tenant + voice broadcast campaign, each in its
    own background thread. Returns immediately. Never raises.
    call_uuid is the cdrs document's uuid; channel_uuid is the FreeSWITCH channel uuid (the
    ?uuid= on the CDR post)."""
    tag = f"[VB_API] call={call_uuid} channel={channel_uuid or '-'}"
    try:
        print(f"{tag} trigger received | tenant={tenant_uuid} voicebroadcast={voicebroadcast_uuid} "
              f"number={destination_number} broadcast_uuid={broadcast_uuid or '-'} billsec={billsec} "
              f"disposition={disposition or '-'}")
        missing = [name for name, value in (("tenant_uuid", tenant_uuid), ("voicebroadcast_uuid", voicebroadcast_uuid)) if not value]
        if missing:
            print(f"{tag} SKIPPED | reason=missing_ids | {', '.join(missing)} empty on the CDR, cannot look up a config")
            return

        config_filter = {"tenant_uuid": tenant_uuid, "voicebroadcast_uuid": voicebroadcast_uuid}
        configs = list(vb_api_config_collection.find({**config_filter, "is_active": True}))
        if not configs:
            inactive = vb_api_config_collection.count_documents({**config_filter, "is_active": {"$ne": True}})
            if inactive:
                print(f"{tag} SKIPPED | reason=config_inactive | {inactive} config(s) exist for this campaign but none is active")
            else:
                print(f"{tag} SKIPPED | reason=no_config | no voicebroadcast_api_config for this tenant + campaign")
            return

        call = {
            "tenant_uuid": tenant_uuid,
            "voicebroadcast_uuid": voicebroadcast_uuid,
            "call_uuid": str(call_uuid or ""),
            "channel_uuid": str(channel_uuid or ""),
            "destination_number": str(destination_number or ""),
            "caller_id_number": str(caller_id_number or ""),
            "lead_uuid": str(lead_uuid or ""),
            "broadcast_uuid": str(broadcast_uuid or ""),
            "billsec": str(billsec if billsec is not None else ""),
            "callstart": str(callstart or ""),
            "dtmf": str(dtmf or ""),
            "disposition": str(disposition or ""),
        }
        config_list = ", ".join(f"{c.get('uuid', '')} (\"{c.get('name', '')}\")" for c in configs)
        print(f"{tag} {len(configs)} active config(s) found, starting one background thread each: {config_list}")
        for config in configs:
            threading.Thread(target=_run_config, args=(config, call), daemon=True).start()
    except Exception:
        print(f"{tag} trigger_vb_api CRASHED | unexpected error, nothing sent:\n{traceback.format_exc()}")
