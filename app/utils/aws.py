import os
import asyncio
from aiokafka.abc import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import boto3

AWS_REGION = os.getenv("KAFKA_AWS_REGION", "us-east-2")


def oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)

    return auth_token, expiry_ms / 1000


class AWSTokenProvider(AbstractTokenProvider):
    async def token(self):
        return await asyncio.get_running_loop().run_in_executor(None, self._token)

    def _token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        return token

class Download_weight:
    def __init__(self) -> None:
        self.s3 = boto3.client('s3',aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID',None),aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY',None))
        self.bucket_name = os.getenv('BUCKET_NAME',None)
        self.object_name = os.getenv('OBJECT_NAME',None)
        os.mkdir("weights")

    def download(self):
        local_file_path = r"weights/best.pt"
        self.s3.download_file(self.bucket_name, self.object_name, local_file_path)
        return local_file_path