import os
import websocket
import logging
from threading import Event
import time
import csv
import datetime
from datetime import timezone
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class ChunkReceiver:
    def __init__(self, ws_url, aws_access_key_id, aws_secret_access_key, 
                 bucket_name, aws_region='us-east-1'):
        self.logger = logging.getLogger("ChunkReceiver")
        self.ws_url = ws_url
        self.temp_dir = "temp_dir"
        self.stop_event = Event()
        self.csv_file = "chunk_timestamps.csv"
        
        # AWS S3 configuration
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        
        # Create temp directory if it doesn't exist
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)
            self.logger.info(f"Created directory: {self.temp_dir}")
        else:
            self.logger.info(f"Using existing directory: {self.temp_dir}")
        
        # Initialize CSV file with headers if it doesn't exist
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['chunk_name', 'timestamp', 's3_url', 'folder_path'])
            self.logger.info(f"Created CSV file: {self.csv_file}")
            
        self.highest_chunk_number = self._get_current_highest_chunk()
        self.logger.info(f"Starting with highest chunk number: {self.highest_chunk_number}")

    def _get_date_folder(self):
        """Generate folder name based on current date."""
        current_date = datetime.datetime.now()
        return f"{current_date.day}_{current_date.strftime('%B')}"  # e.g., "24_February"

    def _get_s3_path(self, chunk_name):
        """Generate S3 path with date-based folder structure."""
        date_folder = self._get_date_folder()
        return f"chunks/{date_folder}/{os.getenv('RABBITMQ_CAMERAID')}/{chunk_name}"

    def _upload_to_s3(self, file_path, chunk_name):
        """Upload a file to S3 bucket in date-based folder structure and return the URL."""
        try:
            s3_path = self._get_s3_path(chunk_name)
            self.s3_client.upload_file(file_path, self.bucket_name, s3_path)
            url = f"https://{self.bucket_name}.s3.amazonaws.com/{s3_path}"
            self.logger.info(f"Successfully uploaded {s3_path} to S3")
            return url, s3_path
        except ClientError as e:
            self.logger.error(f"Error uploading to S3: {e}")
            return None, None

    def _save_timestamp(self, chunk_name, s3_url=None, folder_path=None):
        """Save chunk name, timestamp, S3 URL, and folder path to CSV file."""
        try:
            timestamp = datetime.datetime.now(timezone.utc)
            with open(self.csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([chunk_name, timestamp, s3_url or '', folder_path or ''])
        except Exception as e:
            self.logger.error(f"Error saving timestamp: {e}")

    def _get_current_highest_chunk(self):
        """Get the highest chunk number from existing files."""
        try:
            ts_files = [f for f in os.listdir(self.temp_dir) if f.endswith('.ts')]
            if not ts_files:
                return -1
            numbers = [int(f.split('.')[0]) for f in ts_files]
            return max(numbers) if numbers else -1
        except Exception as e:
            self.logger.error(f"Error getting highest chunk number: {e}")
            return -1

    def receive_chunks(self):
        """Connect to WebSocket and fetch video chunks."""
        chunk_index = self.highest_chunk_number + 1
        current_date = None
        
        while not self.stop_event.is_set():
            try:
                conn = websocket.create_connection(self.ws_url)
                self.logger.info(f"Connected to WebSocket server at {self.ws_url}")
                
                while not self.stop_event.is_set():
                    try:
                        data = conn.recv()
                        if isinstance(data, bytes):
                            # Check if date has changed
                            new_date = datetime.datetime.now().date()
                            if current_date != new_date:
                                current_date = new_date
                                self.logger.info(f"Starting new folder for date: {self._get_date_folder()}")
                            
                            chunk_name = f"{chunk_index}.ts"
                            temp_path = os.path.join(self.temp_dir, chunk_name)
                            self.logger.info(f"Received chunk {chunk_index}")
                            
                            # Save chunk file locally
                            with open(temp_path, 'wb') as f:
                                f.write(data)
                            
                            # # Upload to S3 with date-based folder structure
                            s3_url, folder_path = self._upload_to_s3(temp_path, chunk_name)
                            # s3_url, folder_path = "-","-"
                            
                            # Save timestamp and S3 URL
                            self._save_timestamp(chunk_name, s3_url, folder_path)
                            
                            self.highest_chunk_number = chunk_index
                            chunk_index += 1
                        else:
                            self.logger.error("Received non-binary data")
                    except websocket.WebSocketConnectionClosedException:
                        self.logger.info("WebSocket server disconnected. Attempting to reconnect...")
                        break
                    except Exception as e:
                        self.logger.error(f"Error processing chunk: {e}")
                        break
                        
            except Exception as e:
                self.logger.info("WebSocket server disconnected. Attempting to reconnect...")
                # time.sleep(5)  # Wait 5 seconds before retrying commenting for noiw
                continue

    def stop(self):
        """Stop receiving chunks."""
        self.stop_event.set()

def main():
    time.sleep(20)  # Initial delay
    
    # Get configuration from environment variables
    WS_URL = os.getenv('CAMERA_URL')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_BUCKET_NAME = os.getenv('AWS_BUCKET')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    
    # Validate required environment variables
    required_vars = {
        'CAMERA_URL': WS_URL,
        'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
        'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
        'AWS_BUCKET_NAME': AWS_BUCKET_NAME
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        logging.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return
    
    receiver = ChunkReceiver(
        WS_URL,
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_BUCKET_NAME,
        AWS_REGION
    )
    
    try:
        receiver.receive_chunks()
    except KeyboardInterrupt:
        print("\nStopping receiver...")
        receiver.stop()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        receiver.stop()

if __name__ == "__main__":
    main()














































# import os
# import websocket
# import logging
# from threading import Event
# import time
# import csv
# import datetime
# from datetime import timezone

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )

# class ChunkReceiver:
#     def __init__(self, ws_url):
#         self.logger = logging.getLogger("ChunkReceiver")
#         self.ws_url = ws_url
#         self.temp_dir = "temp_dir"
#         self.stop_event = Event()
#         self.csv_file = "chunk_timestamps.csv"
        
#         # Create temp directory if it doesn't exist
#         if not os.path.exists(self.temp_dir):
#             os.makedirs(self.temp_dir)
#             self.logger.info(f"Created directory: {self.temp_dir}")
#         else:
#             self.logger.info(f"Using existing directory: {self.temp_dir}")
        
#         # Initialize CSV file with headers if it doesn't exist
#         if not os.path.exists(self.csv_file):
#             with open(self.csv_file, 'w', newline='') as f:
#                 writer = csv.writer(f)
#                 writer.writerow(['chunk_name', 'timestamp'])
#             self.logger.info(f"Created CSV file: {self.csv_file}")
            
#         self.highest_chunk_number = self._get_current_highest_chunk()
#         self.logger.info(f"Starting with highest chunk number: {self.highest_chunk_number}")

#     def _get_current_highest_chunk(self):
#         """Get the highest chunk number from existing files."""
#         try:
#             ts_files = [f for f in os.listdir(self.temp_dir) if f.endswith('.ts')]
#             if not ts_files:
#                 return -1
#             numbers = [int(f.split('.')[0]) for f in ts_files]
#             return max(numbers) if numbers else -1
#         except Exception as e:
#             self.logger.error(f"Error getting highest chunk number: {e}")
#             return -1

#     def _save_timestamp(self, chunk_name):
#         """Save chunk name and timestamp to CSV file."""
#         try:
#             timestamp = datetime.datetime.now(timezone.utc)
#             with open(self.csv_file, 'a', newline='') as f:
#                 writer = csv.writer(f)
#                 writer.writerow([chunk_name, timestamp])
#         except Exception as e:
#             self.logger.error(f"Error saving timestamp: {e}")

#     def receive_chunks(self):
#         """Connect to WebSocket and fetch video chunks."""
#         chunk_index = self.highest_chunk_number + 1
        
#         while not self.stop_event.is_set():
#             try:
#                 conn = websocket.create_connection(self.ws_url)
#                 self.logger.info(f"Connected to WebSocket server at {self.ws_url}")
                
#                 while not self.stop_event.is_set():
#                     try:
#                         data = conn.recv()
#                         if isinstance(data, bytes):
#                             chunk_name = f"{chunk_index}.ts"
#                             temp_path = os.path.join(self.temp_dir, chunk_name)
#                             self.logger.info(f"Received chunk {chunk_index}")
                            
#                             # Save chunk file
#                             with open(temp_path, 'wb') as f:
#                                 f.write(data)
                                
#                             # Save timestamp
#                             self._save_timestamp(chunk_name)
                            
#                             self.highest_chunk_number = chunk_index
#                             chunk_index += 1
#                         else:
#                             self.logger.error("Received non-binary data")
#                     except websocket.WebSocketConnectionClosedException:
#                         self.logger.info("WebSocket server disconnected. Attempting to reconnect...")
#                         break
#                     except Exception as e:
#                         self.logger.error(f"Error processing chunk: {e}")
#                         break
                        
#             except Exception as e:
#                 self.logger.info("WebSocket server disconnected. Attempting to reconnect...")
#                 time.sleep(5)  # Wait 5 seconds before retrying
#                 continue

#     def stop(self):
#         """Stop receiving chunks."""
#         self.stop_event.set()

# def main():
#     time.sleep(20)  # Initial delay
#     # Replace with your WebSocket URL
#     WS_URL = os.getenv('CAMERA_URL', None)
    
#     if WS_URL is None:
#         logging.error("CAMERA_URL environment variable is not set")
#         return
    
#     receiver = ChunkReceiver(WS_URL)
#     try:
#         receiver.receive_chunks()
#     except KeyboardInterrupt:
#         print("\nStopping receiver...")
#         receiver.stop()
#     except Exception as e:
#         logging.error(f"Unexpected error: {e}")
#     finally:
#         receiver.stop()

# if __name__ == "__main__":
#     main()










































# import os
# import websocket
# import logging
# from threading import Event
# import time
# import csv
# import datetime
# from datetime import timezone

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )

# class ChunkReceiver:
#     def __init__(self, ws_url):
#         self.logger = logging.getLogger("ChunkReceiver")
#         self.ws_url = ws_url
#         self.temp_dir = "temp_dir"
#         self.stop_event = Event()
#         self.csv_file = "chunk_timestamps.csv"
        
#         # Create temp directory if it doesn't exist
#         if not os.path.exists(self.temp_dir):
#             os.makedirs(self.temp_dir)
#             self.logger.info(f"Created directory: {self.temp_dir}")
#         else:
#             self.logger.info(f"Using existing directory: {self.temp_dir}")
        
#         # Initialize CSV file with headers if it doesn't exist
#         if not os.path.exists(self.csv_file):
#             with open(self.csv_file, 'w', newline='') as f:
#                 writer = csv.writer(f)
#                 writer.writerow(['chunk_name', 'timestamp'])
#             self.logger.info(f"Created CSV file: {self.csv_file}")
            
#         self.highest_chunk_number = self._get_current_highest_chunk()
#         self.logger.info(f"Starting with highest chunk number: {self.highest_chunk_number}")

#     def _save_timestamp(self, chunk_name):
#         """Save chunk name and timestamp to CSV file."""
#         try:
#             timestamp = datetime.datetime.now(timezone.utc)
#             with open(self.csv_file, 'a', newline='') as f:
#                 writer = csv.writer(f)
#                 writer.writerow([chunk_name, timestamp])
#         except Exception as e:
#             self.logger.error(f"Error saving timestamp: {e}")

#     def receive_chunks(self):
#         """Connect to WebSocket and fetch video chunks."""
#         try:
#             conn = websocket.create_connection(self.ws_url)
#             self.logger.info(f"Connected to WebSocket server at {self.ws_url}")

#             chunk_index = self.highest_chunk_number + 1
            
#             while not self.stop_event.is_set():
#                 try:
#                     data = conn.recv()
#                     if isinstance(data, bytes):
#                         chunk_name = f"{chunk_index}.ts"
#                         temp_path = os.path.join(self.temp_dir, chunk_name)
#                         self.logger.info(f"Received chunk {chunk_index}")
                        
#                         # Save chunk file
#                         with open(temp_path, 'wb') as f:
#                             f.write(data)
                            
#                         # Save timestamp
#                         self._save_timestamp(chunk_name)
                        
#                         self.highest_chunk_number = chunk_index
#                         chunk_index += 1
#                     else:
#                         self.logger.error("Received non-binary data")
#                 except websocket.WebSocketConnectionClosedException as e:
#                     self.logger.error(f"Connection closed unexpectedly: {e}")
#                     try:
#                         conn = websocket.create_connection(self.ws_url)
#                         self.logger.info("Reconnected to WebSocket server")
#                     except Exception as re:
#                         self.logger.error(f"Failed to reconnect: {re}")
#                         break
#                 except Exception as e:
#                     self.logger.error(f"Error processing chunk: {e}")
#                     break
#             conn.close()
#         except Exception as e:
#             self.logger.error(f"Failed to connect to WebSocket server: {e}")

#     def stop(self):
#         """Stop receiving chunks."""
#         self.stop_event.set()

# if __name__ == "__main__":
#     time.sleep(20)
#     # Replace with your WebSocket URL
#     WS_URL = os.getenv('CAMERA_URL',None)
    
#     receiver = ChunkReceiver(WS_URL)
#     try:
#         receiver.receive_chunks()
#     except KeyboardInterrupt:
#         print("\nStopping receiver...")
#         receiver.stop()