import os
import cv2
import numpy as np
import logging
from collections import deque
import csv
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class FrameProcessor:
    def __init__(self, max_frames=50, target_total_frames=45):
        self.logger = logging.getLogger("FrameProcessor")
        self.frame_queue = deque(maxlen=max_frames)
        self.temp_dir = "temp_dir"
        self.target_total_frames = target_total_frames
        self.current_time = None
        self.csv_file = "chunk_timestamps.csv"
        
        # Define a special frame to indicate no frames available
        self.NO_FRAMES_SIGNAL = "skip"
        self.dummy_frame = np.zeros((480, 640, 3), dtype=np.uint8)
        
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)
            self.logger.info(f"Created directory: {self.temp_dir}")

    def _get_chunk_timestamp(self, chunk_name):
        """
        Get timestamp for a specific chunk from CSV file.
        """
        try:
            with open(self.csv_file, 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                for row in reader:
                    if row[0] == chunk_name:
                        return datetime.fromisoformat(row[1])
            return None
        except Exception as e:
            self.logger.error(f"Error reading timestamp from CSV: {e}")
            return None

    def scale_frames(self, frames):
        """
        Scale the number of frames to match target_total_frames.
        Args:
            frames: List of frames to scale
        Returns:
            scaled_frames: List of frames scaled to target_total_frames
        """
        target_frame_count = self.target_total_frames
        current_frame_count = len(frames)

        if current_frame_count > target_frame_count:
            indices = np.linspace(0, current_frame_count - 1, target_frame_count)
            scaled_frames = [frames[int(round(i))] for i in indices]
        else:
            scaled_frames = frames

        return scaled_frames

    def read(self):
        """
        Get frames from the queue or process new TS files.
        Maintains chronological order and applies frame scaling.
        """
        try:
            if len(self.frame_queue) == 0:
                ts_files = [f for f in os.listdir(self.temp_dir) if f.endswith('.ts')]
                if not ts_files:
                    self.logger.debug("No TS files available")
                    return self.NO_FRAMES_SIGNAL, self.dummy_frame
                
                # Sort by actual chunk number
                ts_files_sorted = sorted(ts_files, key=lambda x: int(x.split('.')[0]))
                lowest_file = ts_files_sorted[0]
                lowest_number = int(lowest_file.split('.')[0])
                
                self.logger.info(f"Processing chunk ::::  {lowest_number}")
                
                # Update current_time from CSV
                self.current_time = self._get_chunk_timestamp(lowest_file)
                
                file_path = os.path.join(self.temp_dir, lowest_file)
                cap = cv2.VideoCapture(file_path)
                frames = []
                
                while cap.isOpened():
                    ret, frame = cap.read()
                    if not ret:
                        break
                    frames.append(frame)
                
                cap.release()
                os.remove(file_path)  # Remove processed chunk
                
                if not frames:
                    self.logger.warning(f"No frames could be read from {lowest_file}")
                    return self.NO_FRAMES_SIGNAL, self.dummy_frame
                
                # Scale frames and add to queue
                scaled_frames = self.scale_frames(frames)
                self.frame_queue.extend(scaled_frames)
                
                return True, self.frame_queue.popleft()
            else:
                return True, self.frame_queue.popleft()
                
        except Exception as e:
            self.logger.error(f"Error in processing ts file: {e}")
            return self.NO_FRAMES_SIGNAL, self.dummy_frame















































# import os
# import cv2
# import numpy as np
# import logging
# from collections import deque

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )

# class FrameProcessor:
#     def __init__(self, max_frames=50, target_total_frames=45):
#         self.logger = logging.getLogger("FrameProcessor")
#         self.frame_queue = deque(maxlen=max_frames)
#         self.temp_dir = "temp_dir"
#         self.target_total_frames = target_total_frames
        
#         # Define a special frame to indicate no frames available
#         self.NO_FRAMES_SIGNAL = "skip"
#         self.dummy_frame = np.zeros((480, 640, 3), dtype=np.uint8)
        
#         if not os.path.exists(self.temp_dir):
#             os.makedirs(self.temp_dir)
#             self.logger.info(f"Created directory: {self.temp_dir}")

#     def scale_frames(self, frames):
#         """
#         Scale the number of frames to match target_total_frames.
#         Args:
#             frames: List of frames to scale
#         Returns:
#             scaled_frames: List of frames scaled to target_total_frames
#         """
#         target_frame_count = self.target_total_frames
#         current_frame_count = len(frames)

#         if current_frame_count > target_frame_count:
#             indices = np.linspace(0, current_frame_count - 1, target_frame_count)
#             scaled_frames = [frames[int(round(i))] for i in indices]
#         else:
#             scaled_frames = frames

#         return scaled_frames

#     def read(self):
#         """
#         Get frames from the queue or process new TS files.
#         Maintains chronological order and applies frame scaling.
#         """
#         try:
#             if len(self.frame_queue) == 0:
#                 ts_files = [f for f in os.listdir(self.temp_dir) if f.endswith('.ts')]
#                 if not ts_files:
#                     self.logger.debug("No TS files available")
#                     return self.NO_FRAMES_SIGNAL,self.dummy_frame
                
#                 # Sort by actual chunk number
#                 ts_files_sorted = sorted(ts_files, key=lambda x: int(x.split('.')[0]))
#                 lowest_file = ts_files_sorted[0]
#                 lowest_number = int(lowest_file.split('.')[0])
                
#                 self.logger.debug(f"Processing chunk {lowest_number}")
                
#                 file_path = os.path.join(self.temp_dir, lowest_file)
#                 cap = cv2.VideoCapture(file_path)
#                 frames = []
                
#                 while cap.isOpened():
#                     ret, frame = cap.read()
#                     if not ret:
#                         break
#                     frames.append(frame)
                
#                 cap.release()
#                 os.remove(file_path)  # Remove processed chunk
                
#                 if not frames:
#                     self.logger.warning(f"No frames could be read from {lowest_file}")
#                     return self.NO_FRAMES_SIGNAL,self.dummy_frame
                
#                 # Scale frames and add to queue
#                 scaled_frames = self.scale_frames(frames)
#                 self.frame_queue.extend(scaled_frames)
                
#                 return True,self.frame_queue.popleft()
#             else:
#                 return True,self.frame_queue.popleft()
                
#         except Exception as e:
#             self.logger.error(f"Error in processing ts file: {e}")
#             return self.NO_FRAMES_SIGNAL,self.dummy_frame