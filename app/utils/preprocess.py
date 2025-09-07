import os
import cv2
import boto3
import logging
import numpy as np
from app import config
import tensorflow as tf
from ultralytics import YOLO

logger = logging.getLogger("PRE PROCESS")

class DownloadWeight:
    def __init__(self) -> None:
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
        )
        self.bucket_name = "prod.moksa.upload"
        self.object_name = "prod-heat-map"

    def download(self, file_path):
        local_file_path = file_path
        self.s3.download_file(self.bucket_name, f"{self.object_name}/{file_path}", local_file_path)
        return local_file_path

try:
    weight_obj = DownloadWeight()
    yolo_model_path = weight_obj.download("best.pt")
    logger.info("Downloaded YOLO Model from S3")
    yolo_model = "best.pt"
    person_class = 1
except Exception as e:
    logger.error(f"YOLO weights not found in S3: {e}")
    yolo_model = "yolov8m.pt"
    person_class = 0

class PreProcess:
    def __init__(self) -> None:
        self.tensorrt_yolo_model = YOLO(yolo_model, task="detect")
    
    def preprocess_image(self, image):
        image = cv2.resize(image, (640, 480))
        mask = np.zeros(image.shape[:2], dtype=np.uint8)
        results = self.tensorrt_yolo_model.predict(image, verbose=False, classes=person_class, conf=0.5)
        
        polygon_str = os.getenv("ROI", "")
        polygon_points = [list(map(int, point.split('-'))) for point in polygon_str.split(',') if '-' in point]
        polygon = np.array(polygon_points, np.int32)
        
        person_count = 0
        
        for result in results:
            boxes = result.boxes.xyxy.cpu().numpy().astype(int)
            for box in boxes:
                center_x, center_y = (box[0] + box[2]) // 2, (box[1] + box[3]) // 2
                if polygon_points:
                    point_inside = cv2.pointPolygonTest(polygon, (float(center_x), float(center_y)), False)
                    if point_inside >= 0:
                        person_count += 1
                        cv2.rectangle(mask, (box[0], box[1]), (box[2], box[3]), 255, -1)
                else:
                    person_count += 1
                    cv2.rectangle(mask, (box[0], box[1]), (box[2], box[3]), 255, -1)
        
        result = cv2.bitwise_and(image, image, mask=mask)
        result = cv2.resize(result, (224, 224))
        result = tf.cast(result, dtype=tf.float32)
        return result, person_count
