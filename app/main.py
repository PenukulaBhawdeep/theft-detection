import cv2
import time
import asyncio
import logging
import numpy as np
from collections import deque
import datetime

from app import config
from app.utils.common import CacheHelper
from app.utils.preprocess import PreProcess
from app.utils.camera_intialize import CameraInit
from app.models.theft_inference import TheftInference

logger = logging.getLogger("main")

async def main() -> None:
    try:
        # Initialization
        camera = CameraInit()
        rch = CacheHelper()
        model = TheftInference()
        preprocess_obj = PreProcess()
        
        # Initialize variables
        camera.print_values()
        client_type = config.CLIENT_TYPE
        rtsp_link = config.RTSP_URL
        frame_count, batch_count, skip_predictions = 0, 0, 0
        clip, frames_original = deque(maxlen=config.FRAME_LENGTH), deque(maxlen=config.FRAME_LENGTH + 200)
        person_detection_history = deque(maxlen=100)
        
        # Video initialization
        video = camera.camera_init(client_type=client_type, rtsp_url=rtsp_link)
        frame_time = time.time()
        
        while True:
            success, frame = video.read()
            
            if not success and isinstance(frame, np.ndarray):
                continue
            if not success or not isinstance(frame, np.ndarray):
                logger.warning("Error loading frame")
                video.release()
                video = camera.camera_init(client_type=client_type, rtsp_url=rtsp_link)
                continue
            
            frame_count += 1
            batch_count += 1
            
            # Log FPS
            current_time = time.time()
            if current_time - frame_time >= 1:
                logger.info(f"Frames fetched in last second: {frame_count}")
                frame_count = 0
                frame_time = current_time
            
            # Process frame and get person count
            pre_frame, person_count = preprocess_obj.preprocess_image(frame)
            clip.append(pre_frame)
            frames_original.append(cv2.resize(frame, (480, 360)))
            
            person_detection_history.append(person_count > 0)
            persons_detected = any(person_detection_history)
            
            if len(clip) == config.FRAME_LENGTH and batch_count >= 5 and skip_predictions <= 0:
                if persons_detected:
                    frame_current_time = datetime.datetime.now(datetime.timezone.utc)
                    skip_predictions, theft_res = model.predict(list(clip), frame_current_time)
                    previous_theft_prob = theft_res
                else:
                    logger.info("Skipping prediction - no persons detected in recent frames")
                    model.counter = 0
                batch_count = 0
            else:
                if skip_predictions == 199:
                    rch.set_json({"theft_result": [15, frames_original, previous_theft_prob, frame_current_time]})
                skip_predictions -= 1
    
    except Exception as e:
        logger.error(f"Error in main function: {e}", exc_info=True)
        if 'video' in locals():
            video.release()

if __name__ == "__main__":
    asyncio.run(main())
