import tensorflow as tf
from loguru import logger
from dotenv import load_dotenv

from app import config

load_dotenv()

gpu_devices = tf.config.experimental.list_physical_devices('GPU')
for gpu in gpu_devices:
    tf.config.experimental.set_memory_growth(gpu, True)
    tf.config.experimental.set_virtual_device_configuration(
        gpu, [tf.config.experimental.VirtualDeviceConfiguration(config.GPU_LIMIT)]
    )

class TheftInference:
    def __init__(self):
        self.threshold_prob = config.THEFT_THRESHOLD
        self.model_path = config.MODEL_PATH
        self.skip_frame = config.SKIP_FRAME
        
        if not self.model_path:
            raise ValueError("MODEL_PATH is not set in the .env file")
        
        self.model = self.load_model()
        if self.model is None:
            raise ValueError(f"Failed to load the model from {self.model_path}.")
        
        self.counter = 0

    def load_model(self):
        try:
            # model = tf.keras.models.load_model(self.model_path)  #for raw model
            return tf.saved_model.load(self.model_path)
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            return None
    
    def predict(self, clip, frame_current_time):
        clip = tf.expand_dims(tf.stack(clip), axis=0)
        
        try:
            prediction = self.model(clip)
            theft_res = prediction[0][1]
            logger.info(f"Theft Predicted with confidence: {theft_res}, Time: {frame_current_time}")
            
            if theft_res > self.threshold_prob:
                self.counter += 1
                if self.counter >= config.CONSECUTIVE_PRED:
                    self.counter = 0
                    return self.skip_frame, float(theft_res)  
            else:
                self.counter = 0
        except Exception as e:
            logger.error(f"Prediction error: {e}")
        
        return 0, 0