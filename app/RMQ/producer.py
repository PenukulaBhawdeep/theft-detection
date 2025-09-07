import pika
import json
import ssl
from datetime import datetime,timezone
from loguru import logger 
from app import config

class TheftDetectionProducer:
    def __init__(self, host=config.RABBITMQ_HOST_PRODUCER, 
                 queue_name=config.RABBITMQ_QUEUE_NAME_PRODUCER, 
                 port=config.RABBITMQ_PORT_PRODUCER,
                 credentials=pika.PlainCredentials(config.RABBITMQ_USER_PRODUCER, config.RABBITMQ_PASS_PRODUCER)):
        # Create SSL context
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE  # Warning: This disables SSL certificate verification
        # Set up connection parameters
        self.params = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=credentials,
            ssl_options=pika.SSLOptions(context),
            connection_attempts=3,
            retry_delay=5,
            socket_timeout=10
        )
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue=self.queue_name, durable=True)
    def publish_detection(self,trace_id, cam_id, s3_uri,timestamp,theft_res,store_id):   # trace_id, camera_id, url,timestamp
        detection = {
            'trace_id': trace_id,
            'camera_id': cam_id,
            's3_url': s3_uri,
            'timestamp' : str(timestamp),
            'theft_probability' : float(theft_res),
            'store_id' : store_id
        }
        if config.STORE_MESSAGE_RMQ == "True": # by default True
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=json.dumps(detection),
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                )
            )
        # if config.DEFAULT_MESSAGE_RMQ == "True": # by default True
        #     self.channel.queue_declare(queue="theft_detections_multicam", durable=True)
        #     self.channel.basic_publish(
        #                 exchange='',
        #                 routing_key="theft_detections_multicam",
        #                 body=json.dumps(detection),
        #                 properties=pika.BasicProperties(
        #                     delivery_mode=pika.DeliveryMode.Persistent
        #                 )
                    # )
        print(f" [x] Sent {detection}")
    def close(self):
        self.connection.close()
