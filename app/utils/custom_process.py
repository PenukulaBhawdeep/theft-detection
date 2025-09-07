import os
import datetime
import asyncio
import uuid
import logging
import boto3
from botocore.exceptions import ClientError
import subprocess
import time

from app.utils.common import CacheHelper
from app import config
from app.utils.message import TheftMessage
from app.kafka.asyncio.producer import CustomAIOKafkaProducer
from app.RMQ.producer import TheftDetectionProducer

logger = logging.getLogger("Custom Process")


class CustomProcess():
    def __init__(self, *args, **kwargs):
        super(CustomProcess, self).__init__(*args, **kwargs)
        self.create_folder()
    
    
    def create_folder(self):
        try:
            os.mkdir('theft_videos')
        except FileExistsError:
            pass

    def upload_file_and_get_direct_url(self, file_name, bucket, object_name=None):
        if object_name is None:
            object_name = file_name
        
        s3 = boto3.client(
            's3',
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            region_name='us-east-2'
        )

        try:
            s3.upload_file(file_name, bucket, object_name)
            url = f"https://{bucket}.s3.{s3.meta.region_name}.amazonaws.com/{object_name}"
            logger.info(f"File {file_name} uploaded successfully to {bucket}/{object_name}")
            return url
        except ClientError as e:
            logger.info(f"Error uploading file: {e}")
            return None
        
    def send_rabbitmq_message(self, camera_id, url, trace_id, timestamp, theft_res,store_id):
        try:
            self.rmq_producer.publish_detection(trace_id, camera_id, url, timestamp, theft_res,store_id)
            logger.info(f"Sent theft detection message to RabbitMQ for camera {camera_id} with trace_id {trace_id}")
        except Exception as e:
            logger.info(f"Error sending message to RabbitMQ for camera {camera_id}: {e}")
            while True:
                try:
                    self.rmq_producer = TheftDetectionProducer()
                    logger.info(":::connected to RABBITMQ:::")
                    break
                except:
                    continue
            self.rmq_producer.publish_detection(trace_id, camera_id, url, timestamp, theft_res,store_id)
            logger.info(f"Sent theft detection message to RabbitMQ for camera {camera_id} with trace_id {trace_id}")

    def write_video(self, frames, output_path, fps=15):
        if not frames:
            return
            
        command = [
            'ffmpeg',
            '-y',
            '-f', 'rawvideo',
            '-vcodec', 'rawvideo',
            '-s', f'{frames[0].shape[1]}x{frames[0].shape[0]}',
            '-pix_fmt', 'bgr24',
            '-r', str(fps),
            '-i', '-',
            '-an',
            '-vcodec', 'libx264',
            '-pix_fmt', 'yuv420p',
            output_path
        ]
        
        process = subprocess.Popen(command, stdin=subprocess.PIPE)
        
        for frame in frames:
            if frame is None:
                continue
            process.stdin.write(frame.tobytes())
        
        process.stdin.close()
        process.wait()

    async def run_process(self):
        rch = CacheHelper()
        
        # Try to establish connections
        while True:
            try:
                self.rmq_producer = TheftDetectionProducer()
                logger.info(":::connected to RABBITMQ:::")
                kafka_producer = CustomAIOKafkaProducer()
                await kafka_producer.start()
                logger.info(":::connected to KAFKA:::")
                break
            except Exception as e:
                logger.info(f"Error connecting to kafka or Rabbitmq: {str(e)}")
                time.sleep(1)
                continue

        while True:
            try:
                await asyncio.sleep(1)  # Non-blocking sleep
                logger.info("-----")
                frames = rch.get_json("theft_result")
                if not frames:
                    continue
                    
                logger.info(":::CUSTOM THREAD IS GETTING EXECUTED:::")
                
                frame_current_time = frames[3]
                frame_rate = frames[0]
                theft_res = frames[2]
                frames = list(frames[1])
                
                timestamp = datetime.datetime.fromisoformat(str(frame_current_time))
                timestamp = timestamp.replace(tzinfo=None) 
                timestamp = timestamp.isoformat()
                
                video_path = f'theft_videos/{timestamp}.mp4'
                self.write_video(frames, video_path, fps=frame_rate)

                url = self.upload_file_and_get_direct_url(
                    file_name=video_path,
                    bucket=config.AWS_BUCKET,
                    object_name=config.AWS_OBJECT_NAME + '/' + video_path
                )
                logger.info(url)
                
                trace_id = str(uuid.uuid4())
                camera_id = os.getenv("RABBITMQ_CAMERAID", "000")
                store_id = os.getenv("STORE_ID")
                
                message = TheftMessage(
                    camera_id=camera_id,
                    timestamp=frame_current_time,
                    s3_url=url,
                    trace_id=trace_id,
                    theft_probability=theft_res,
                    model_version="v1.0.0"
                )
                
                logger.info(":::::::::::BEFORE RABBITMQ PRODUCER:::::::::::")
                self.send_rabbitmq_message(camera_id, url, trace_id, timestamp, theft_res,store_id)
                logger.info(":::::::::::AFTER RABBITMQ PRODUCER:::::::::::")

                logger.info(":::::::::::BEFORE KAFKA PRODUCER:::::::::::")
                await kafka_producer.produce(topic=os.getenv("KAFKA_TOPIC", 'theft-detect-topic'), message=message)
                logger.info(":::::::::::AFTER KAFKA PRODUCER:::::::::::")
                
                os.remove(video_path)
                
            except Exception as e:
                logger.info(f"Error in Custom thread function: {str(e)}")
                


# Create an async main function to properly use await
async def main():
    process_obj = CustomProcess()
    time.sleep(30)
    await process_obj.run_process()

# Run the async main function with asyncio
if __name__ == "__main__":
    asyncio.run(main())
