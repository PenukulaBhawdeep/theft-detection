import os
import cv2
import logging
import time
from dotenv import load_dotenv

from app.stream.rabbitmq import RabbitMQ
from app.stream.chunks_process import FrameProcessor
from antmedia_ser.webrtc_sub import AntMediaCamera

logger = logging.getLogger("Camera Initialize :: ")

class CameraInit:
    def __init__(self) -> None:
        pass
    
    def camera_init(self, client_type=None, rtsp_url=None):
        if client_type == "rabitmq":
            logger.info(" ::: RABBITMQ INITIATED ::: ")
            video = RabbitMQ()
            video.connect()
        elif client_type == "chunks":
            video = FrameProcessor()
            
            
        # elif client_type == "webrtc":
        #         app_name = os.getenv("ANTMEDIA_APP_NAME", "live")
        #         stream_id = os.getenv("ANTMEDIA_STREAM_ID")
        #         server_url = os.getenv("ANTMEDIA_SERVER_URL", "test.antmedia.io")
        #         if not stream_id:
        #             raise ValueError("ANTMEDIA_STREAM_ID must be set in environment variables")
        
        #         video = create_antmedia_camera(app_name, stream_id, server_url)
        #         time.sleep(20)
        elif client_type == "webrtc":
            stream_id = os.getenv("ANTMEDIA_STREAM_ID")
            if not stream_id:
                logger.error("No stream ID found for WebRTC camera")
                return False
            
            websocket_url = os.getenv("WEBSOCKET_URL", "wss://ams-14883.antmedia.cloud:5443/WebRTCAppEE/websocket")
            if not websocket_url:
                logger.error("No WebSocket URL found for WebRTC camera")
                return False
            
            video = AntMediaCamera(
                websocket_url=websocket_url,
                stream_id=stream_id,
                buffer_size=50,
            )
        
        else:
            logger.info("RTSP INITIATED")
            video = cv2.VideoCapture(rtsp_url)
        
        return video
    
    def print_values(self):
        logger.info("::: Checking environment variables :::")
        load_dotenv()
        
        env_vars = dict(os.environ)
        logger.info("Environment Variables:")
        for key, value in env_vars.items():
            logger.info(f"{key}: {value}")
