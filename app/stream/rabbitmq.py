# import cv2
# import time
# import pika
# import json
# import base64
# import numpy as np
# from typing import Iterator
# import os
# from app import config

# from app import logger
# from collections import deque



# # # RabbitMQ connection details
# # rabbitmq_host = 'localhost'
# # rabbitmq_port = 5672
# # rabbitmq_user = 'user'
# # rabbitmq_pass = 'password'

# # # Queue name
# # queue_name = 'video_frames'
# # # Connect to RabbitMQ
# # credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
# # connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port, credentials=credentials))
# # channel = connection.channel()


# class RabbitMQ:

#     def __init__(self,
#                  host=os.getenv("RABBITMQ_HOST","127.0.0"),
#                  port = os.getenv("RABBITMQ_PORT",5672),
#                  user = os.getenv("RABBITMQ_USER","user"),
#                  password = os.getenv("RABBITMQ_PASS","password"),
#                  queue_name=os.getenv("QUEUE_NAME",None),
#                  buffer_size=1,
#                  camera_id = config.RABBITMQ_CAMERAID,
#                  ):
#         self.host = host
#         self.queue_name = queue_name
#         self.connection = None
#         self.channel = None
#         self.camera_buffers = {}
#         self.rabbitmq_user = user
#         self.rabbitmq_pass = password
#         self.rabbitmq_port = port
#         self.buffer_size = buffer_size
#         self.camera_id = camera_id


#     def connect(self):
#         try:

#             # Connect to RabbitMQ
#             credentials = pika.PlainCredentials(self.rabbitmq_user, self.rabbitmq_pass)
#             connection = pika.BlockingConnection(pika.ConnectionParameters(
#                 host=self.host, port=self.rabbitmq_port, credentials=credentials))
#             self.channel = connection.channel()
#             self.channel.queue_declare(queue=self.queue_name, durable=False)

#             logger.info(f"---------------------------------------Connected to RabbitMQ ----------------------------on {self.host}")

#         except pika.exceptions.AMQPConnectionError as e:
#             logger.error(f"Failed to connect to RabbitMQ: {e}")
#             raise

#     def close(self):

#         if self.connection and not self.connection.is_closed:
#             self.connection.close()
#             logger.info("Connection to RabbitMQ closed")

#     def read(self):
#         try:
#             method_frame, _, body = self.channel.basic_get(queue=self.queue_name)
#             if method_frame:
#                 frame = self.process_message(body)
#                 if frame is not None:
#                     self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
#                     return True, frame
#                 else:
#                     return False,None
#                     logger.debug(f"Processed message didn't match camera")
#             else:
#                 # Instead of breaking, we'll sleep for a short time and continue polling
#                 time.sleep(0.0001)
#                 logger.debug(f"Method frame is False so returning none")
#                 return False, None

#         except Exception as e:
#             logger.error(f"error message {e}")
#             return False, None

#     # def process_message(self, body):
#     #     try:

#     #         nparr = np.frombuffer(body, np.uint8)
#     #         return cv2.imdecode(nparr, cv2.IMREAD_COLOR)

#     #     except Exception as e:
#     #         logger.error(f"Error processing message: {e}")
#     #     return None

#     def process_message(self, body):
#         try:
#             message = json.loads(body)
#             if message.get('cameraId') == self.camera_id:
#                 image_data = base64.b64decode(message['payload'])
#                 frame = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)
#                 frame = np.array(frame)
#                 frame = cv2.resize(frame, (224, 224))
#                 return frame
                
#             else:
#                 logger.debug(f"Message didn't match camera {self.camera_id}")
#                 return None
#         except json.JSONDecodeError as e:
#             logger.debug(f" error in process message rabbitmq  {e}")
#             return None




import cv2
import time
import pika
import json
import base64
import numpy as np
import os
import logging
import traceback
from typing import Tuple, Optional
class RabbitMQ:
    # def __init__(self,
    #              host="172.20.48.178",
    #              port= 5672,
    #              username="admin",
    #              password="YfHqxGBW8495YgiXnHBxKht7xaEo33wV",
    #              queue_name="g_Theft-1",
    #              camera_id="0rq8k4sjpmdf59a"):
    
    def __init__(self,
                 host=os.getenv("RABBITMQ_HOST","127.0.0"),
                 port = os.getenv("RABBITMQ_PORT",5672),
                 username = os.getenv("RABBITMQ_USER","user"),
                 password = os.getenv("RABBITMQ_PASS","password"),
                 queue_name=os.getenv("QUEUE_NAME",None),
                 buffer_size=1,
                 camera_id = os.getenv("RABBITMQ_CAMERAID",None),
                 ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.queue_name = queue_name
        self.camera_id = camera_id
        self.connection = None
        self.channel = None
        # print(camera_id,"------------------------------------")
        # print("iiiinnnnn ttthhheeee ccccliiieennnttt tttyyyyppppeeeee")
    def connect(self):
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                heartbeat=300,
                blocked_connection_timeout=300
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            # Declare the queue without lazy mode
            self.channel.queue_declare(
                queue=self.queue_name,
                durable=True,
                arguments={'x-queue-mode': 'lazy'}
            )
            print("-----------------------conected------to------------rabbitMQ-------------------------- inside-------------")
            logging.info(f"Connected to RabbitMQ on {self.host}")
        except Exception as e:
            print("conection failed---------------------------")
            logging.error(f"Failed to connect to RabbitMQ: {str(e)}")
            raise
    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logging.info("Connection to RabbitMQ closed")
    def reconnect(self):
        logging.info("Attempting to reconnect to RabbitMQ")
        self.close()
        time.sleep(5)
        self.connect()
    def read(self) -> Tuple[bool, Optional[np.ndarray]]:
        while True:
            try:
                if self.connection is None or self.connection.is_closed:
                    logging.info("Connection closed, reconnecting...")
                    self.reconnect()
                method_frame, _, body = self.channel.basic_get(queue=self.queue_name)
                if method_frame:
                    frame = self.process_message(body)
                    if frame is not None:
                        self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                        # logging.info(f"Message acknowledged for camera {self.camera_id}")
                        return True, frame
                    else:
                        # Changed to not requeue if message doesn't match camera_id
                        self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                        logging.debug(f"Processed message didn't match camera {self.camera_id}")
                else:
                    logging.debug(f"No message available for camera {self.camera_id}")
                time.sleep(0.1)
            except pika.exceptions.AMQPConnectionError as e:
                logging.error(f"AMQP Connection Error: {e}")
                self.reconnect()
            except Exception as e:
                logging.error(f"Error reading message for camera {self.camera_id}: {str(e)}")
                logging.error(f"Traceback: {traceback.format_exc()}")
                time.sleep(1)
    def process_message(self, body) -> Optional[np.ndarray]:
        try:
            message = json.loads(body)
            if message.get('camera_id') == self.camera_id:
                image_data = base64.b64decode(message['payload'])
                frame = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)
                if frame is not None:
                    frame = cv2.resize(frame, (640, 480))
                    return frame
                else:
                    logging.error("Failed to decode image data")
                    return None
            else:
                logging.debug(f"Message didn't match camera {self.camera_id}")
                return None
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON: {e}")
        except KeyError as e:
            logging.error(f"Missing key in message: {e}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")
        return None
