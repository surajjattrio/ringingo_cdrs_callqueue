import pika
import json
import sys
import uuid
import time
import re
import threading
from datetime import datetime
import pymongo
import urllib.parse
from freeswitchESL import ESL
import logging
from config import mongo_url

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== CONFIGURATION ====================
RABBITMQ_HOST = '40.81.228.224'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'admin'
RABBITMQ_PASSWORD = 'Trio@321'

EXCHANGE_NAME = 'freeswitch_events'
EXCHANGE_TYPE = 'direct'  # FIXED: was 'freeswitch_' (INVALID), now 'direct' (CORRECT)

# Event routing keys and queues
QUEUES = {
    'channel_execute': ('freeswitch_channel_execute', 'channel.execute'),
    'channel_create': ('freeswitch_channel_create', 'channel.create'),
    'channel_originate': ('freeswitch_channel_originate', 'channel.originate'),
    'channel_progress': ('freeswitch_channel_progress', 'channel.progress'),
    'channel_answer': ('freeswitch_channel_answer', 'channel.answer'),
    'channel_destroy': ('freeswitch_channel_destroy', 'channel.destroy'),
    'three_way': ('freeswitch_three_way', 'channel.three_way'),
    'custom': ('freeswitch_custom', 'custom.event'),
}

# ==================== HELPERS ====================
def word_exists(text, word):
    if not text or not word:
        return False
    return bool(re.search(r'\b' + re.escape(word) + r'\b', text))

def get_header(event, key):
    """Cache-friendly header getter"""
    try:
        val = event.getHeader(key)
        return val if val else ""
    except:
        return ""

# ==================== RABBITMQ CONNECTION MANAGER ====================
class RabbitMQConnectionManager:
    def __init__(self, host=RABBITMQ_HOST, port=RABBITMQ_PORT, 
                 username=RABBITMQ_USER, password=RABBITMQ_PASSWORD):
        self.host = host
        self.port = port
        self.credentials = pika.PlainCredentials(username, password)
        self.connection = None
        self.channel = None
        self._connect()
    
    def _connect(self):
        """Establish RabbitMQ connection"""
        try:
            connection_params = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=self.credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
                connection_attempts=3,
                retry_delay=2,
                socket_timeout=10
            )
            self.connection = pika.BlockingConnection(connection_params)
            self.channel = self.connection.channel()
            self.channel.basic_qos(prefetch_count=1)
            logger.info("[RABBITMQ] Connected successfully")
        except Exception as e:
            logger.error(f"[RABBITMQ] Connection failed: {e}")
            raise
    
    def setup_exchange_and_queues(self):
        """Declare exchange and all queues with proper bindings"""
        try:
            # Declare DIRECT exchange
            self.channel.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type=EXCHANGE_TYPE,
                durable=True,
                auto_delete=False
            )
            logger.info(f"[RABBITMQ] Exchange '{EXCHANGE_NAME}' declared")
            
            # Declare and bind queues
            for queue_key, (queue_name, routing_key) in QUEUES.items():
                self.channel.queue_declare(
                    queue=queue_name,
                    durable=True,
                    auto_delete=False
                )
                self.channel.queue_bind(
                    exchange=EXCHANGE_NAME,
                    queue=queue_name,
                    routing_key=routing_key
                )
                logger.info(f"[RABBITMQ] Queue '{queue_name}' bound to '{routing_key}'")
        
        except Exception as e:
            logger.error(f"[RABBITMQ] Setup failed: {e}")
            raise
    
    def publish_event(self, event_name, event_data):
        """Publish event to RabbitMQ with routing"""
        try:
            routing_key = self._get_routing_key(event_name)
            if not routing_key:
                return False
            
            self.channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=routing_key,
                body=json.dumps(event_data),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json',
                    timestamp=int(time.time() * 1000)
                )
            )
            return True
        except Exception as e:
            logger.error(f"[RABBITMQ] Publish failed: {e}")
            return False
    
    @staticmethod
    def _get_routing_key(event_name):
        """Map event name to routing key"""
        mapping = {
            'CHANNEL_EXECUTE': 'channel.execute',
            'CHANNEL_CREATE': 'channel.create',
            'CHANNEL_ORIGINATE': 'channel.originate',
            'CHANNEL_PROGRESS': 'channel.progress',
            'CHANNEL_ANSWER': 'channel.answer',
            'CHANNEL_DESTROY': 'channel.destroy',
            'CHANNEL_HANGUP': 'channel.destroy',
            'CHANNEL_HANGUP_COMPLETE': 'channel.destroy',
            'CUSTOM': 'custom.event',
            'CALL_UPDATE': 'channel.three_way',
        }
        return mapping.get(event_name)
    
    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

# ==================== MONGODB CONNECTION ====================
class MongoDBConnection:
    def __init__(self):
        self.client = pymongo.MongoClient(
            mongo_url,
            maxPoolSize=3,
            minPoolSize=1,
            maxIdleTimeMS=30000,
            serverSelectionTimeoutMS=5000
        )
        self.db = self.client["admin"]
        self.auto_originate = self.db['auto_campaign_originate']
        self.realtime_report = self.db['realtime_report_count_mgmt']
        self.inbound_report = self.db['inbound_realtime_count_mgmt']
        self.manager_realtime = self.db['manager_realtime_count_mgmt']
        logger.info("[MONGODB] Connected successfully")

mongo = MongoDBConnection()

# ==================== FREESWITCH CONNECTION ====================
class FreeSwitchConnection:
    def __init__(self):
        try:
            self.con = ESL.ESLconnection("127.0.0.1", "8021", "ClueCon")
            if self.con.connected():
                logger.info("[FREESWITCH] Connected successfully")
                self.con.events("plain", "ALL")
            else:
                logger.error("[FREESWITCH] Connection failed")
                sys.exit(1)
        except Exception as e:
            logger.error(f"[FREESWITCH] Connection error: {e}")
            sys.exit(1)
    
    def api(self, command):
        """Execute FreeSWITCH API command"""
        try:
            return self.con.api(command)
        except Exception as e:
            logger.error(f"[FREESWITCH] API error: {e}")
            return None
    
    def recv_event(self):
        """Receive event from FreeSWITCH"""
        try:
            return self.con.recvEvent()
        except Exception as e:
            logger.error(f"[FREESWITCH] Receive error: {e}")
            return None

fs = FreeSwitchConnection()

# ==================== EVENT PUBLISHER ====================
class EventPublisher:
    """Publishes FreeSWITCH events to RabbitMQ"""
    
    RELEVANT_EVENTS = {
        "CHANNEL_EXECUTE", "CHANNEL_CREATE", "CHANNEL_ORIGINATE",
        "CHANNEL_PROGRESS", "CHANNEL_ANSWER", "CHANNEL_DESTROY",
        "CHANNEL_HANGUP", "CHANNEL_HANGUP_COMPLETE", "CUSTOM", "CALL_UPDATE"
    }
    
    @staticmethod
    def publish_event(rabbitmq, event):
        """Convert FreeSWITCH event to JSON and publish"""
        event_name = get_header(event, "Event-Name")
        
        if event_name not in EventPublisher.RELEVANT_EVENTS:
            return
        
        headers = {
            "Unique-ID": get_header(event, "Unique-ID"),
            "Channel-Call-UUID": get_header(event, "Channel-Call-UUID"),
            "variable_uuid": get_header(event, "variable_uuid"),
            "Event-Subclass": get_header(event, "Event-Subclass"),
            "variable_sip_h_X-inbound_campaign_flag": get_header(event, "variable_sip_h_X-inbound_campaign_flag"),
            "variable_sip_h_X-Leaduuid": get_header(event, "variable_sip_h_X-Leaduuid"),
            "variable_sip_h_X-tenant_uuid": get_header(event, "variable_sip_h_X-tenant_uuid"),
            "variable_sip_h_P-auto_campaign": get_header(event, "variable_sip_h_P-auto_campaign"),
            "variable_sip_h_P-campaign_uuid": get_header(event, "variable_sip_h_P-campaign_uuid"),
            "variable_sip_h_P-tenant_uuid": get_header(event, "variable_sip_h_P-tenant_uuid"),
            "variable_sip_h_P-lead_uuid": get_header(event, "variable_sip_h_P-lead_uuid"),
            "variable_sip_h_P-manager_uuid": get_header(event, "variable_sip_h_P-manager_uuid"),
            "variable_sip_h_P-username": get_header(event, "variable_sip_h_P-username"),
            "variable_sip_h_P-tenant_domain": get_header(event, "variable_sip_h_P-tenant_domain"),
            "variable_sip_h_P-auto_campaign_originate": get_header(event, "variable_sip_h_P-auto_campaign_originate"),
            "variable_custom_recording": get_header(event, "variable_custom_recording"),
            "variable_recording_directory": get_header(event, "variable_recording_directory"),
            "variable_sip_h_P-amd_status": get_header(event, "variable_sip_h_P-amd_status"),
            "variable_sip_h_P-user_uuid": get_header(event, "variable_sip_h_P-user_uuid"),
            "variable_current_application": get_header(event, "variable_current_application"),
            "Other-Leg-Direction": get_header(event, "Other-Leg-Direction"),
            "Other-Leg-Username": get_header(event, "Other-Leg-Username"),
            "Other-Leg-Callee-ID-Number": get_header(event, "Other-Leg-Callee-ID-Number"),
            "Other-Leg-Destination-Number": get_header(event, "Other-Leg-Destination-Number"),
            "variable_original_destination_number": get_header(event, "variable_original_destination_number"),
            "Application": get_header(event, "Application"),
            "Caller-Channel-Created-Time": get_header(event, "Caller-Channel-Created-Time"),
            "Caller-Channel-Progress-Time": get_header(event, "Caller-Channel-Progress-Time"),
            "Caller-Channel-Progress-Media-Time": get_header(event, "Caller-Channel-Progress-Media-Time"),
            "Caller-Channel-Answered-Time": get_header(event, "Caller-Channel-Answered-Time"),
            "Beep-Status": get_header(event, "Beep-Status"),
            "variable_sip_from_uri": get_header(event, "variable_sip_from_uri"),
            "variable_sip_from_host": get_header(event, "variable_sip_from_host"),
            "variable_sip_h_X-a_leg_call_uuid": get_header(event, "variable_sip_h_X-a_leg_call_uuid"),
            "conference_uuid": get_header(event, "conference_uuid"),
            "variable_sip_h_P-call_start": get_header(event, "variable_sip_h_P-call_start"),
        }
        
        event_data = {
            "event_name": event_name,
            "timestamp": datetime.utcnow().isoformat(),
            "headers": headers
        }
        
        rabbitmq.publish_event(event_name, event_data)

# ==================== EVENT CONSUMERS ====================
class ConsumerBase:
    @staticmethod
    def safe_update(collection, query, update_doc):
        """Safe MongoDB update"""
        try:
            if query.get('lead_uuid'):
                result = collection.update_one(query, {"$set": update_doc})
                return result.matched_count > 0
        except Exception as e:
            logger.error(f"[DB] Update error: {e}")
        return False
    
    @staticmethod
    def safe_insert(collection, doc):
        """Safe MongoDB insert"""
        try:
            collection.insert_one(doc)
            return True
        except Exception as e:
            logger.error(f"[DB] Insert error: {e}")
        return False
    
    @staticmethod
    def safe_delete(collection, query):
        """Safe MongoDB delete"""
        try:
            collection.delete_one(query)
            return True
        except Exception as e:
            logger.error(f"[DB] Delete error: {e}")
        return False

class ChannelExecuteConsumer(ConsumerBase):
    @staticmethod
    def handle(msg):
        headers = msg['headers']
        lead_uuid = headers.get('variable_sip_h_X-Leaduuid')
        tenant_uuid = headers.get('variable_sip_h_X-tenant_uuid')
        
        if (headers.get('variable_sip_h_X-inbound_campaign_flag') == 'true' and 
            lead_uuid and tenant_uuid and headers.get('Application') == 'callcenter'):
            
            if not mongo.inbound_report.find_one({"lead_uuid": lead_uuid}):
                ChannelExecuteConsumer.safe_insert(mongo.inbound_report, {
                    "uuid": str(uuid.uuid4()),
                    "lead_uuid": lead_uuid,
                    "tenant_uuid": tenant_uuid,
                    "type": 1
                })

class ChannelCreateConsumer(ConsumerBase):
    @staticmethod
    def handle(msg):
        headers = msg['headers']
        lead_uuid = headers.get('variable_sip_h_X-Leaduuid')
        
        if (headers.get('variable_sip_h_X-inbound_campaign_flag') == 'true' and 
            lead_uuid and headers.get('variable_sip_h_X-tenant_uuid')):
            
            if mongo.inbound_report.find_one({"lead_uuid": lead_uuid}):
                ChannelCreateConsumer.safe_update(mongo.inbound_report, 
                    {"lead_uuid": lead_uuid}, {"type": 0})

class ChannelOriginateConsumer(ConsumerBase):
    @staticmethod
    def handle(msg):
        headers = msg['headers']
        
        if headers.get('variable_sip_h_P-auto_campaign') == 'true':
            lead_uuid = headers.get('variable_sip_h_P-lead_uuid')
            
            if not mongo.realtime_report.find_one({"lead_uuid": lead_uuid}):
                insert_doc = {
                    "uuid": str(uuid.uuid4()),
                    "lead_uuid": lead_uuid,
                    "campaign_uuid": headers.get('variable_sip_h_P-campaign_uuid'),
                    "tenant_uuid": headers.get('variable_sip_h_P-tenant_uuid'),
                    "type": 3,
                    "created_at": headers.get('variable_sip_h_P-call_start')
                }
                if headers.get('variable_sip_h_P-manager_uuid'):
                    insert_doc["manager_uuid"] = headers.get('variable_sip_h_P-manager_uuid')
                
                ChannelOriginateConsumer.safe_insert(mongo.realtime_report, insert_doc)

class ChannelProgressConsumer(ConsumerBase):
    @staticmethod
    def handle(msg):
        headers = msg['headers']
        
        if headers.get('variable_sip_h_P-auto_campaign') == 'true':
            lead_uuid = headers.get('variable_sip_h_P-lead_uuid')
            
            if mongo.realtime_report.find_one({"lead_uuid": lead_uuid}):
                ChannelProgressConsumer.safe_update(mongo.realtime_report,
                    {"lead_uuid": lead_uuid}, {"type": 0})

class ChannelAnswerConsumer(ConsumerBase):
    @staticmethod
    def handle(msg):
        headers = msg['headers']
        auto_campaign = headers.get('variable_sip_h_P-auto_campaign')
        inbound_flag = headers.get('variable_sip_h_X-inbound_campaign_flag')
        
        if auto_campaign == 'true':
            ChannelAnswerConsumer._handle_recording_start(headers)
            ChannelAnswerConsumer._handle_amd(headers)
            ChannelAnswerConsumer._handle_realtime_update(headers)
            ChannelAnswerConsumer._handle_agent_state(headers, 'In a queue call')
        
        if inbound_flag == 'true':
            lead_uuid = headers.get('variable_sip_h_X-Leaduuid')
            if lead_uuid and mongo.inbound_report.find_one({"lead_uuid": lead_uuid}):
                ChannelAnswerConsumer.safe_update(mongo.inbound_report,
                    {"lead_uuid": lead_uuid}, {"type": 2})
    
    @staticmethod
    def _handle_recording_start(headers):
        auto_campaign_originate = headers.get('variable_sip_h_P-auto_campaign_originate')
        auto_campaign_recording = headers.get('variable_custom_recording')
        call_uuid = headers.get('variable_uuid')
        destination_number = headers.get('Other-Leg-Destination-Number')
        recording_path = headers.get('variable_recording_directory')
        
        if (auto_campaign_originate and auto_campaign_recording == '0' and 
            call_uuid and destination_number and recording_path):
            recording_file = f"{recording_path}/{destination_number}_{call_uuid}.wav"
            fs.api(f'uuid_record {call_uuid} start {recording_file}')
    
    @staticmethod
    def _handle_amd(headers):
        amd_status = headers.get('variable_sip_h_P-amd_status')
        if amd_status != 'true':
            return
        
        auto_campaign_originate = headers.get('variable_sip_h_P-auto_campaign_originate')
        if auto_campaign_originate:
            return
        
        try:
            progresstime = int(headers.get('Caller-Channel-Progress-Time') or 0)
            progressmediatime = int(headers.get('Caller-Channel-Progress-Media-Time') or 0)
            answertime = int(headers.get('Caller-Channel-Answered-Time') or 0)
            createtime = int(headers.get('Caller-Channel-Created-Time') or 0)
            
            if not all([progresstime, progressmediatime, answertime, createtime]):
                return
            
            if progressmediatime > 0:
                seconds_diff = (answertime - progressmediatime) / 1000000.0
            elif progresstime > 0:
                seconds_diff = (answertime - progresstime) / 1000000.0
            else:
                seconds_diff = (answertime - createtime) / 1000000.0
            
            hangup_ranges = [[0.0, 1.50], [23.00, 24.00], [30.00, 31.00]]
            if any(start < seconds_diff < end for start, end in hangup_ranges):
                campaign_uuid = headers.get('variable_sip_h_P-campaign_uuid')
                user_uuid = headers.get('variable_sip_h_P-user_uuid')
                if campaign_uuid and user_uuid:
                    ChannelAnswerConsumer.safe_update(mongo.auto_originate,
                        {"campaign_uuid": campaign_uuid, "agent_uuid": user_uuid},
                        {"flag": '0'})
        except (ValueError, TypeError):
            pass
    
    @staticmethod
    def _handle_realtime_update(headers):
        lead_uuid = headers.get('variable_sip_h_P-lead_uuid')
        if lead_uuid and mongo.realtime_report.find_one({"lead_uuid": lead_uuid}):
            ChannelAnswerConsumer.safe_update(mongo.realtime_report,
                {"lead_uuid": lead_uuid}, {"type": 2})
        
        manager_uuid = headers.get('variable_sip_h_P-manager_uuid')
        if manager_uuid and lead_uuid and mongo.manager_realtime.find_one({"lead_uuid": lead_uuid}):
            ChannelAnswerConsumer.safe_update(mongo.manager_realtime,
                {"lead_uuid": lead_uuid}, {"type": 2})
    
    @staticmethod
    def _handle_agent_state(headers, state):
        username = headers.get('variable_sip_h_P-username')
        tenant_domain = headers.get('variable_sip_h_P-tenant_domain')
        if username and tenant_domain:
            fs.api(f"callcenter_config agent set state {username}.{tenant_domain}@default '{state}'")

class ChannelDestroyConsumer(ConsumerBase):
    @staticmethod
    def handle(msg):
        headers = msg['headers']
        
        if headers.get('variable_sip_h_P-auto_campaign') == 'true':
            ChannelDestroyConsumer._handle_recording_stop(headers)
            ChannelDestroyConsumer._handle_agent_state(headers)
            
            lead_uuid = headers.get('variable_sip_h_P-lead_uuid')
            if lead_uuid:
                ChannelDestroyConsumer.safe_delete(mongo.realtime_report, {"lead_uuid": lead_uuid})
        
        if headers.get('variable_sip_h_X-inbound_campaign_flag') == 'true':
            lead_uuid = headers.get('variable_sip_h_X-Leaduuid')
            if lead_uuid:
                ChannelDestroyConsumer.safe_delete(mongo.inbound_report, {"lead_uuid": lead_uuid})
    
    @staticmethod
    def _handle_recording_stop(headers):
        auto_campaign_originate = headers.get('variable_sip_h_P-auto_campaign_originate')
        auto_campaign_recording = headers.get('variable_custom_recording')
        call_uuid = headers.get('variable_uuid')
        destination_number = headers.get('Other-Leg-Destination-Number')
        recording_path = headers.get('variable_recording_directory')
        
        if (auto_campaign_originate and auto_campaign_recording == '0' and 
            call_uuid and destination_number and recording_path):
            recording_file = f"{recording_path}/{destination_number}_{call_uuid}.wav"
            fs.api(f'uuid_record {call_uuid} stop {recording_file}')
    
    @staticmethod
    def _handle_agent_state(headers):
        username = headers.get('variable_sip_h_P-username')
        tenant_domain = headers.get('variable_sip_h_P-tenant_domain')
        if username and tenant_domain:
            fs.api(f"callcenter_config agent set state {username}.{tenant_domain}@default 'Idle'")

class CustomEventConsumer(ConsumerBase):
    @staticmethod
    def handle(msg):
        headers = msg['headers']
        event_subclass = headers.get('Event-Subclass')
        
        if event_subclass in ["X_FER_HANGUP", "A_LEG_HANGUP"]:
            conference_uuid = headers.get('conference_uuid')
            if conference_uuid:
                fs.api(f'uuid_kill {conference_uuid}')
        elif headers.get('Beep-Status') == "DETECTED":
            unique_id = headers.get('Unique-ID')
            if unique_id:
                fs.api(f'uuid_kill {unique_id}')

class ThreeWayConsumer(ConsumerBase):
    @staticmethod
    def handle(msg):
        headers = msg['headers']
        
        try:
            response = fs.api('show registrations as json')
            if not response:
                return
            
            parsed_data = json.loads(response.getBody())
            rows = parsed_data.get("rows", [])
            
            check_var = headers.get('variable_original_destination_number')
            if headers.get('Other-Leg-Direction') == 'inbound':
                check_var = headers.get('Other-Leg-Username')
            
            if not check_var or not rows:
                return
            
            final_string = None
            for row in rows:
                if word_exists(row.get("reg_user", ""), check_var):
                    final_string = f"sip:{row.get('reg_user')}@{row.get('network_ip')}:{row.get('network_port')};transport=wss"
                    break
            
            if final_string:
                x_fer_uuid = headers.get('Unique-ID')
                a_leg_call_uuid = headers.get('variable_sip_h_X-a_leg_call_uuid')
                sip_from_uri = f"sip:{headers.get('variable_sip_from_uri')}"
                
                evt = ESL.ESLevent("SWITCH_EVENT_NOTIFY")
                evt.addHeader("profile", 'external')
                evt.addHeader("from-uri", sip_from_uri)
                evt.addHeader("to-uri", final_string)
                evt.addHeader("user", "202")
                evt.addHeader("host", headers.get('variable_sip_from_host'))
                evt.addHeader("X-XFER-UUID", x_fer_uuid)
                evt.addHeader("extra-headers", f"X-XFER-UUID:{x_fer_uuid},X-A-LEG-UUID:{a_leg_call_uuid}")
                fs.con.sendEvent(evt)
                del evt
        except Exception as e:
            logger.error(f"[THREE_WAY] Error: {e}")

# ==================== CONSUMER WORKERS ====================
class ConsumerWorker(threading.Thread):
    def __init__(self, queue_name, routing_key, consumer_class, worker_id):
        super().__init__(daemon=True)
        self.queue_name = queue_name
        self.routing_key = routing_key
        self.consumer_class = consumer_class
        self.worker_id = worker_id
        self.rmq = None
    
    def run(self):
        """Start consuming messages"""
        try:
            self.rmq = RabbitMQConnectionManager()
            self.rmq.setup_exchange_and_queues()
            
            def callback(ch, method, properties, body):
                try:
                    msg = json.loads(body)
                    self.consumer_class.handle(msg)
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    logger.error(f"[WORKER {self.worker_id}] Error: {e}")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            self.rmq.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False
            )
            
            logger.info(f"[WORKER {self.worker_id}] Started consuming {self.queue_name}")
            self.rmq.channel.start_consuming()
        
        except Exception as e:
            logger.error(f"[WORKER {self.worker_id}] Fatal error: {e}")
            time.sleep(5)

# ==================== MAIN ====================
def event_producer():
    """Main loop to receive events from FreeSWITCH and publish to RabbitMQ"""
    logger.info("[PRODUCER] Started")
    rabbitmq = RabbitMQConnectionManager()
    rabbitmq.setup_exchange_and_queues()
    
    event_count = 0
    
    while True:
        try:
            event = fs.recv_event()
            if event:
                EventPublisher.publish_event(rabbitmq, event)
                event_count += 1
                
                if event_count % 1000 == 0:
                    logger.info(f"[PRODUCER] Processed {event_count} events")
        except Exception as e:
            logger.error(f"[PRODUCER] Error: {e}")
            time.sleep(1)

def start_consumers():
    """Start consumer workers in separate threads"""
    consumers = [
        (QUEUES['channel_execute'][0], QUEUES['channel_execute'][1], ChannelExecuteConsumer, 'execute'),
        (QUEUES['channel_create'][0], QUEUES['channel_create'][1], ChannelCreateConsumer, 'create'),
        (QUEUES['channel_originate'][0], QUEUES['channel_originate'][1], ChannelOriginateConsumer, 'originate'),
        (QUEUES['channel_progress'][0], QUEUES['channel_progress'][1], ChannelProgressConsumer, 'progress'),
        (QUEUES['channel_answer'][0], QUEUES['channel_answer'][1], ChannelAnswerConsumer, 'answer'),
        (QUEUES['channel_destroy'][0], QUEUES['channel_destroy'][1], ChannelDestroyConsumer, 'destroy'),
        (QUEUES['custom'][0], QUEUES['custom'][1], CustomEventConsumer, 'custom'),
        (QUEUES['three_way'][0], QUEUES['three_way'][1], ThreeWayConsumer, 'three_way'),
    ]
    
    threads = []
    for queue_name, routing_key, consumer_class, name in consumers:
        for worker_id in range(2):
            worker = ConsumerWorker(queue_name, routing_key, consumer_class, f"{name}_{worker_id}")
            worker.start()
            threads.append(worker)
            logger.info(f"[STARTUP] Started consumer worker {name}_{worker_id}")
    
    return threads

if __name__ == "__main__":
    try:
        logger.info("[STARTUP] Initializing system...")
        
        logger.info("[STARTUP] Starting consumer workers...")
        consumer_threads = start_consumers()
        
        logger.info("[STARTUP] Starting event producer...")
        event_producer()
        
    except KeyboardInterrupt:
        logger.info("[SHUTDOWN] Shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"[STARTUP] Fatal error: {e}")
        sys.exit(1)

