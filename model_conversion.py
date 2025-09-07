import tensorflow as tf
import keras
from keras.models import load_model
import os
from tensorflow.python.compiler.tensorrt import trt_convert as trt
from tensorflow.python.saved_model import tag_constants
import cv2
import numpy as np
import time

model = load_model('parent_model_05_02.h5')
model.save('./parentmodel/')

print('Converting to TF-TRT FP32...')
conversion_params = trt.DEFAULT_TRT_CONVERSION_PARAMS._replace(precision_mode=trt.TrtPrecisionMode.FP32,max_workspace_size_bytes=1<<30)
                                                              #  max_workspace_size_bytes=8000000000)

converter = trt.TrtGraphConverterV2(input_saved_model_dir='./parentmodel/',
                                    conversion_params=conversion_params,use_dynamic_shape = True)
converter.convert()
converter.save(output_saved_model_dir='./app/models/14_02')
print('Done Converting to TF-TRT FP32')