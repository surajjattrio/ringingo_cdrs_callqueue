"""
Post-call feedback (CSAT SMS).

Called from cdrs_posting.py right after a "cdrs" document is written, from every branch
that writes one. Creates a call_feedback record for a qualifying call, then immediately
sends the SMS itself — one process, one file, no RabbitMQ, no cron, no delay, no call
into another service. Never raises into the caller.

Eligibility/record-creation logging here is print-to-terminal only. The one exception is
the SMS send outcome itself: that gets one call_feedback_logs row (request/response,
credentials redacted) so a "my caller never got the SMS" question can be answered from
the DB later, not just from whatever's still in the terminal scrollback.
"""
import base64
import json
import re
import secrets
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode

import pymongo
import requests
from Crypto.Cipher import AES

from config import mongo_url, database_name, FEEDBACK_LINK_BASE_URL

_client = pymongo.MongoClient(mongo_url)
_db = _client[database_name]
call_feedback_collection = _db["call_feedback"]
call_feedback_logs_collection = _db["call_feedback_logs"]
sms_provider_configs_collection = _db["sms_provider_configs"]
cdrs_collection = _db["cdrs"]

MIN_BILLSEC = 3
# Ambiguous characters (0 O 1 l I) deliberately excluded — this is a link a human may read/type.
TOKEN_ALPHABET = "23456789abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ"
TOKEN_LENGTH = 10
TOKEN_MAX_ATTEMPTS = 3

# Hard cap regardless of what a tenant configures — a stalled provider call must never
# hang this process past this, no matter what timeoutMs was saved.
TIMEOUT_HARD_CAP_MS = 10000
DEFAULT_TIMEOUT_MS = 5000

# Same AES-256-CBC key/IV/algorithm as ringingo-api-fs-19's CommonFunctions.encode/decode
# (see its .env) — sms_provider_configs.credentials is encrypted there and must be
# decrypted here with the identical values. If ringingo-api-fs-19's ENCRYPTION_KEY/
# PASSWORD_LENGTH ever rotate, update these to match.
ALGORITHM = "aes-256-cbc"
ENCRYPTION_KEY = "1234567890abcdefghijklmnopqrstuv"
PASSWORD_LENGTH = "1234567890abcdef"

_WHOLE_PLACEHOLDER_RE = re.compile(r"^\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}$")
_PLACEHOLDER_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_.]+)\s*\}\}")
_SENSITIVE_KEY_RE = re.compile(r"auth|token|secret|password|credential|api[-_]?key|bearer", re.IGNORECASE)


# ============================================================
# Small helpers — logging, tokens
# ============================================================

def _generate_token():
    return ''.join(secrets.choice(TOKEN_ALPHABET) for _ in range(TOKEN_LENGTH))


def _print_feedback_link(token):
    if FEEDBACK_LINK_BASE_URL:
        print(f"[FEEDBACK] link: {FEEDBACK_LINK_BASE_URL.rstrip('/')}/r/{token}")
    else:
        print(f"[FEEDBACK] link: FEEDBACK_LINK_BASE_URL not configured, token={token}")


def _write_log(tenant_uuid, stage, outcome, message="", feedback_id="", call_uuid="", token="", context=None):
    try:
        parts = [f"[FEEDBACK] {stage} -> {outcome}"]
        if message:
            parts.append(message)
        if tenant_uuid:
            parts.append(f"tenantUuid={tenant_uuid}")
        if call_uuid:
            parts.append(f"callUuid={call_uuid}")
        if feedback_id:
            parts.append(f"feedbackId={feedback_id}")
        if token:
            parts.append(f"token={token}")
        if context:
            parts.append(f"context={context}")
        print(" | ".join(parts))
    except Exception as e:
        print(f"[FEEDBACK] Failed to print log line: {e}")


def _persist_sms_log(tenant_uuid, feedback_id, call_uuid, token, stage, outcome, message, request, response, duration_ms):
    """The one call_feedback_logs row per send attempt — the SMS provider's actual
    request/response, so a delivery failure has an exact, shareable reason later, not
    just whatever's still in the terminal scrollback. Credentials redacted. Never raises.
    """
    try:
        call_feedback_logs_collection.insert_one({
            "uuid": str(uuid.uuid4()),
            "tenantUuid": tenant_uuid,
            "feedbackId": feedback_id,
            "callUuid": call_uuid,
            "token": token,
            "stage": stage,
            "outcome": outcome,
            "message": message,
            "request": _redact(request) if request else request,
            "response": response,
            "durationMs": duration_ms,
            "createdAt": datetime.now(timezone.utc),
        })
    except Exception as e:
        print(f"[FEEDBACK] Failed to write call_feedback_logs row: {e}")


# ============================================================
# Record creation — eligibility + insert
# ============================================================

def _check_eligibility(tenant_uuid, call_uuid, cdr_doc):
    """Returns billsec if the call qualifies, else None (and logs the skip). Checks every
    gating condition here, before a record/token is ever created — no point minting a
    token for a tenant that has nothing configured to send it with.
    """
    billsec = int(cdr_doc.get("billsecond", 0) or 0)
    has_active_sms_config = sms_provider_configs_collection.find_one(
        {"tenantUuid": tenant_uuid, "isActive": True}
    ) is not None
    context = {"billsec": billsec, "minBillsec": MIN_BILLSEC, "hasActiveSmsConfig": has_active_sms_config}

    if billsec < MIN_BILLSEC:
        _write_log(
            tenant_uuid, "eligibility_checked", "skipped",
            f"billsec {billsec} is below the {MIN_BILLSEC}s minimum",
            call_uuid=call_uuid, context=context,
        )
        return None

    if not has_active_sms_config:
        _write_log(
            tenant_uuid, "eligibility_checked", "skipped",
            "No active SMS provider config for tenant",
            call_uuid=call_uuid, context=context,
        )
        return None

    _write_log(
        tenant_uuid, "eligibility_checked", "success",
        f"billsec {billsec} meets the {MIN_BILLSEC}s minimum",
        call_uuid=call_uuid, context=context,
    )
    return billsec


def _build_feedback_doc(tenant_uuid, call_uuid):
    # Deliberately minimal — everything about the call itself (caller number, agent,
    # queue, campaign, call end time) is looked up from `cdrs` via callUuid when
    # actually needed (see _resolve_caller_number), not duplicated onto this record.
    now = datetime.now(timezone.utc)
    return {
        "uuid": str(uuid.uuid4()),
        "tenantUuid": tenant_uuid,
        "callUuid": call_uuid,
        "status": "pending",
        "attempts": 0,
        "createdAt": now,
        "updatedAt": now,
    }


def _insert_with_token_retry(tenant_uuid, call_uuid, base_doc):
    """Inserts base_doc with a freshly generated token, retrying on token collision.
    A (tenantUuid, callUuid) collision means a feedback record already exists for this
    call — treated as success, not an error (this hook can fire more than once per call).
    Returns the feedback record's uuid (new or pre-existing) on success, else None.
    """
    last_error = None
    for _ in range(TOKEN_MAX_ATTEMPTS):
        token = _generate_token()
        doc = dict(base_doc)
        doc["token"] = token
        try:
            call_feedback_collection.insert_one(doc)
            _write_log(
                tenant_uuid, "feedback_created", "success", "Feedback record created",
                feedback_id=doc["uuid"], call_uuid=call_uuid, token=token,
            )
            _print_feedback_link(token)
            return doc["uuid"]
        except pymongo.errors.DuplicateKeyError as e:
            last_error = e
            if "callUuid" in str(e):
                _write_log(
                    tenant_uuid, "feedback_created", "skipped",
                    "Feedback record already exists for this call", call_uuid=call_uuid,
                )
                existing = call_feedback_collection.find_one({"tenantUuid": tenant_uuid, "callUuid": call_uuid})
                return existing["uuid"] if existing else None
            continue  # token collision — retry with a freshly generated token

    print(f"[FEEDBACK] Failed to create feedback record after token collisions: {last_error}")
    _write_log(
        tenant_uuid, "feedback_created", "failure",
        "Token generation collided too many times", call_uuid=call_uuid,
    )
    return None


def _link_cdr_to_feedback(tenant_uuid, call_uuid, custom_callid, feedback_uuid):
    """Stamps call_feedback_uuid onto the cdrs document so CDR reports can map to
    feedback without a second lookup. Tries the same two match paths cdrs_posting.py
    itself uses to write the document (uuid, then custom_callid as a fallback), since a
    custom_callid match may have updated a pre-existing document with a different uuid.
    """
    try:
        result = cdrs_collection.update_one(
            {"uuid": call_uuid, "tenant_uuid": tenant_uuid},
            {"$set": {"call_feedback_uuid": feedback_uuid}},
        )
        if result.matched_count == 0 and custom_callid:
            result = cdrs_collection.update_one(
                {"custom_callid": custom_callid, "tenant_uuid": tenant_uuid},
                {"$set": {"call_feedback_uuid": feedback_uuid}},
            )
        if result.matched_count == 0:
            print(f"[FEEDBACK] Could not find cdrs document to link call_feedback_uuid={feedback_uuid} (callUuid={call_uuid})")
    except Exception as e:
        print(f"[FEEDBACK] Failed to link call_feedback_uuid onto cdrs document: {e}")


# ============================================================
# SMS sending — resolve config, decrypt, render, send, evaluate
# ============================================================

def _decrypt(encrypted_hex):
    """Same AES-256-CBC scheme as ringingo-api-fs-19's CommonFunctions.decode: hex
    ciphertext, PKCS7-padded, raw UTF-8 key/IV bytes."""
    key = ENCRYPTION_KEY.encode("utf-8")
    iv = PASSWORD_LENGTH.encode("utf-8")
    ciphertext = bytes.fromhex(encrypted_hex)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    padded = cipher.decrypt(ciphertext)
    pad_len = padded[-1]
    return padded[:-pad_len].decode("utf-8")


def _render_value(value, variables):
    if not isinstance(value, str):
        return value
    whole = _WHOLE_PLACEHOLDER_RE.match(value)
    if whole:
        return variables.get(whole.group(1), "")
    return _PLACEHOLDER_RE.sub(lambda m: str(variables.get(m.group(1), "")), value)


def _render_template(template, variables):
    if isinstance(template, list):
        return [_render_template(v, variables) for v in template]
    if isinstance(template, dict):
        return {k: _render_template(v, variables) for k, v in template.items()}
    return _render_value(template, variables)


def _apply_auth(auth_type, credentials, headers, query_params, body):
    if auth_type == "bearer":
        token = credentials.get("token") or credentials.get("apiKey") or next(iter(credentials.values()), "")
        headers["Authorization"] = f"Bearer {token}"
    elif auth_type == "basic":
        raw = f"{credentials.get('username', '')}:{credentials.get('password', '')}".encode("utf-8")
        headers["Authorization"] = f"Basic {base64.b64encode(raw).decode('ascii')}"
    elif auth_type == "header":
        headers.update(credentials)
    elif auth_type == "query_param":
        query_params.update(credentials)
    elif auth_type == "body_field" and isinstance(body, dict):
        body.update(credentials)
    return headers, query_params, body


def _redact(value):
    """Recursively masks any key matching the sensitive pattern — used for logging only,
    never for what's actually sent to the provider. Covers headers, but also body/params
    for authType "body_field"/"query_param", where the credential lands there instead."""
    if isinstance(value, list):
        return [_redact(v) for v in value]
    if isinstance(value, dict):
        return {k: ("[REDACTED]" if _SENSITIVE_KEY_RE.search(str(k)) else _redact(v)) for k, v in value.items()}
    return value


def _shell_quote(value):
    return "'" + str(value).replace("'", "'\\''") + "'"


def _build_curl(method, url, params, headers, body):
    """Copy-pasteable curl for the outgoing SMS request."""
    full_url = url
    if params:
        separator = "&" if "?" in url else "?"
        full_url = f"{url}{separator}{urlencode(params)}"

    parts = [f"curl -X {method}", _shell_quote(full_url)]
    has_content_type = any(key.lower() == "content-type" for key in (headers or {}))
    for key, value in (headers or {}).items():
        parts.append(f"-H {_shell_quote(f'{key}: {value}')}")
    if body:
        if not has_content_type:
            parts.append(f"-H {_shell_quote('Content-Type: application/json')}")
        parts.append(f"-d {_shell_quote(json.dumps(body))}")
    return " ".join(parts)


def _get_by_path(obj, path):
    if not path or obj is None:
        return None
    cur = obj
    for key in path.split("."):
        if isinstance(cur, dict) and key in cur:
            cur = cur[key]
        else:
            return None
    return cur


def _resolve_caller_number(tenant_uuid, call_uuid):
    """call_feedback doesn't store call details — looked up from cdrs by callUuid
    only when actually needed, right here at send time."""
    cdr = cdrs_collection.find_one({"uuid": call_uuid, "tenant_uuid": tenant_uuid})
    if not cdr:
        return ""
    if cdr.get("direction") == "inbound":
        return str(cdr.get("caller_id_number") or "")
    return str(cdr.get("destination_number") or "")


def _evaluate_success(config, status_code, response_body):
    condition = config.get("successCondition") or {"statusMin": 200, "statusMax": 299}
    if not (condition.get("statusMin", 200) <= status_code <= condition.get("statusMax", 299)):
        return False
    json_path = condition.get("jsonPath")
    if json_path:
        return _get_by_path(response_body, json_path) == condition.get("expectedValue")
    return True


def _send_now(tenant_uuid, call_uuid):
    """Resolves the tenant's active SMS provider config, decrypts credentials, renders
    and sends, evaluates success, updates the call_feedback record. One attempt, no
    retry (no sweep). Idempotent — a no-op if the record isn't (still) "pending".
    Provider-neutral by configuration (authType/bodyTemplate/successCondition), not by
    class hierarchy — one implementation for every provider.
    """
    try:
        feedback = call_feedback_collection.find_one({"tenantUuid": tenant_uuid, "callUuid": call_uuid})
        if not feedback:
            print(f"[FEEDBACK] send_now: no feedback record for tenant={tenant_uuid} call={call_uuid}")
            return
        if feedback.get("status") != "pending":
            print(f"[FEEDBACK] send_now: feedback already in status \"{feedback.get('status')}\", skipping")
            return

        feedback_id = feedback["uuid"]
        token = feedback["token"]

        config = sms_provider_configs_collection.find_one({"tenantUuid": tenant_uuid, "isActive": True})
        if not config:
            print(f"[FEEDBACK] send_now: no active SMS provider config for tenant={tenant_uuid}")
            call_feedback_collection.update_one(
                {"uuid": feedback_id},
                {"$set": {"status": "skipped", "skipReason": "no_active_sms_config", "updatedAt": datetime.now(timezone.utc)}},
            )
            return

        print(f"[FEEDBACK] send_now: resolved SMS config \"{config.get('name')}\" ({config.get('uuid')}) for tenant={tenant_uuid}")

        credentials = {}
        try:
            decrypted = _decrypt(str(config.get("credentials") or ""))
            credentials = json.loads(decrypted) if decrypted else {}
        except Exception as e:
            print(f"[FEEDBACK] send_now: failed to decrypt credentials for config {config.get('uuid')}: {e}")

        link = f"{FEEDBACK_LINK_BASE_URL.rstrip('/')}/r/{token}" if FEEDBACK_LINK_BASE_URL else f"/r/{token}"
        variables = {
            "to": _resolve_caller_number(tenant_uuid, call_uuid),
            "senderId": str(config.get("senderId") or ""),
            "templateId": str(config.get("providerTemplateId") or ""),
            "link": link,
        }
        for name in (config.get("variables") or []):
            variables.setdefault(name, "")

        body = _render_template(config.get("bodyTemplate") or {}, variables)
        headers = _render_template(config.get("headers") or {}, variables)
        query_params = _render_template(config.get("queryParams") or {}, variables)
        headers, query_params, body = _apply_auth(str(config.get("authType") or ""), credentials, headers, query_params, body)

        url = str(config.get("baseUrl") or "")
        method = str(config.get("method") or "POST").upper()
        timeout_ms = min(int(config.get("timeoutMs") or DEFAULT_TIMEOUT_MS), TIMEOUT_HARD_CAP_MS)

        print(f"[FEEDBACK] send_now: sending SMS to {variables['to']} via {url} (config {config.get('uuid')})")
        # Unredacted on purpose — printed so it can be copy-pasted and run directly to
        # test against the real provider. This puts the real credential in the terminal
        # log; be mindful of who can read that log stream.
        curl_command = _build_curl(method, url, query_params, headers, body)
        print(f"[FEEDBACK] send_now: {curl_command}")

        started_at = datetime.now(timezone.utc)
        status_code = None
        response_body = None
        error_message = None
        permanent = False
        sent = False

        try:
            response = requests.request(
                method, url, params=query_params, json=body, headers=headers,
                timeout=timeout_ms / 1000,
            )
            status_code = response.status_code
            try:
                response_body = response.json()
            except Exception:
                response_body = response.text

            if _evaluate_success(config, status_code, response_body):
                sent = True
            else:
                permanent = 400 <= status_code < 500
                error_message = f"HTTP {status_code}"
        except Exception as e:
            error_message = str(e)

        duration_ms = int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000)
        print(
            f"[FEEDBACK] send_now: SMS send {'succeeded' if sent else 'failed'} "
            f"status={status_code} durationMs={duration_ms} response={response_body}"
            + (f" error={error_message}" if error_message else "")
        )

        _persist_sms_log(
            tenant_uuid, feedback_id, call_uuid, token,
            stage="sms_api_response" if sent else "sms_send_failed",
            outcome="success" if sent else "failure",
            message="SMS sent" if sent else (error_message or f"HTTP {status_code}"),
            request={"method": method, "url": url, "params": query_params, "headers": headers, "body": body},
            response={"statusCode": status_code, "body": response_body},
            duration_ms=duration_ms,
        )

        update = {"attempts": int(feedback.get("attempts") or 0) + 1, "updatedAt": datetime.now(timezone.utc)}
        if sent:
            update["status"] = "sent"
            update["smsConfigId"] = config.get("uuid")
            update["deliveryStatus"] = "sent"
        else:
            update["status"] = "failed_permanent" if permanent else "failed"
            update["lastError"] = error_message or f"HTTP {status_code}"
        call_feedback_collection.update_one({"uuid": feedback_id}, {"$set": update})
    except Exception as e:
        print(f"[FEEDBACK] send_now failed unexpectedly: {e}")


# ============================================================
# Public entry point — called from cdrs_posting.py
# ============================================================

def trigger_call_feedback(cdr_doc):
    """Evaluate eligibility, create a call_feedback record, send the SMS. Safe to call
    more than once for the same call (e.g. a request that touches more than one CDR
    write branch, or a transferred/re-routed leg) — the unique index on
    (tenantUuid, callUuid) makes every call after the first a no-op, treated as success.
    TODO: multi-leg dedupe is out of scope beyond that — a transferred call may still
    generate more than one feedback record if it produces more than one distinct callUuid.
    """
    if not cdr_doc:
        return
    tenant_uuid = cdr_doc.get("tenant_uuid", "")
    call_uuid = cdr_doc.get("uuid", "")
    try:
        if not tenant_uuid or not call_uuid:
            return
        if _check_eligibility(tenant_uuid, call_uuid, cdr_doc) is None:
            return

        base_doc = _build_feedback_doc(tenant_uuid, call_uuid)
        feedback_uuid = _insert_with_token_retry(tenant_uuid, call_uuid, base_doc)
        if feedback_uuid:
            _link_cdr_to_feedback(tenant_uuid, call_uuid, cdr_doc.get("custom_callid"), feedback_uuid)
            _send_now(tenant_uuid, call_uuid)
    except Exception as e:
        print(f"[FEEDBACK] CDR feedback hook failed: {e}")
