import cv2
import numpy as np

from app.stream.api.hikvisionapi import Client


class HikvisionCamera:
    def __init__(
        self,
        ip: str,
        username: str,
        password: str,
        channel_id: int,
        chunk_size: int = 2048,
    ) -> None:

        self.channel_id = channel_id
        self.chunk_size = chunk_size
        self.camera = Client(ip, username, password)

        self.streaming_channels = dict()
        self.set_streaming_channels()

        assert channel_id in self.streaming_channels

    def set_streaming_channels(self) -> None:

        res = self.camera.Streaming.channels(method="get")

        try:
            for channel in res["StreamingChannelList"]["StreamingChannel"]:
                self.streaming_channels[channel["id"]] = {
                    "id": channel["id"],
                    "channel_name": channel["channelName"],
                    "video": channel["Video"]["enabled"] == "true",
                    "width": channel["Video"]["videoResolutionWidth"],
                    "height": channel["Video"]["videoResolutionHeight"],
                }

        except Exception as error:
            print(f"error in retreiving streaming channels : {error}")

    @property
    def frame_width(self) -> int:
        return self.streaming_channels[self.channel_id]["width"]

    @property
    def frame_height(self) -> int:
        return self.streaming_channels[self.channel_id]["height"]

    def read(self) -> tuple[bool, np.array]:

        response = self.camera.Streaming.channels[self.channel_id].picture(
            method="get", type="opaque_data"
        )

        if response.content:
            return True, cv2.imdecode(
                np.frombuffer(response.content, dtype=np.uint8), cv2.IMREAD_COLOR
            )
        else:
            return False, np.array([], dtype=np.uint8)
