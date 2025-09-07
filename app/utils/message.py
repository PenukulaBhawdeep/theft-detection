from datetime import datetime
from pydantic import BaseModel


class TheftMessage(BaseModel):

    # camera_ip: str
    camera_id: str
    s3_url: str
    timestamp: datetime
    trace_id: str
    model_version: str = 'v1.0.0'
    theft_probability: float

    def to_dict(self) -> dict:
        return self.model_dump(mode="json")