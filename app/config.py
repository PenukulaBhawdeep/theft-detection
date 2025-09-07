
import os


CONSECUTIVE_PRED = int(os.getenv("CONTINOUS_PREDICTION", 20))
THEFT_THRESHOLD =float(os.getenv('THEFT_THRESHOLD',0.7))
GPU_LIMIT = int(os.getenv('GPU_LIMIT'))
SKIP_FRAME = int(os.getenv('SKIP_FRAME', 300))


#Camera
STORE_ID = os.getenv("STORE_ID",'123')
CAMERA_IP = os.getenv("CAMERA_IP",'192.168.1.1')
CAMERA_NO = os.getenv('CAMERA_NO','102')
RTSP_URL = os.getenv('CAMERA_URL',None)
CLIENT_TYPE = os.getenv('CLIENT_TYPE',"rtsp")
RABBITMQ_CAMERAID = os.getenv("RABBITMQ_CAMERAID",None)
FRAME_LENGTH = int(os.getenv("FRAME_LENGTH"))


MODEL_PATH = os.getenv('MODEL_PATH',None)

