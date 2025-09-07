import cv2
import numpy as np


class OpenCVCamera:
    def __init__(self, video_url: str) -> None:

        self.video = cv2.VideoCapture(video_url)

    @property
    def frame_width(self) -> int:
        return int(self.video.get(cv2.CAP_PROP_FRAME_WIDTH))

    @property
    def frame_height(self) -> int:
        return int(self.video.get(cv2.CAP_PROP_FRAME_HEIGHT))

    def read(self) -> tuple[bool, np.array]:

        success, frame = self.video.read()

        return success, frame
