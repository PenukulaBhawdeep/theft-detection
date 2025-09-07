import asyncio
import threading
import time
import logging
import numpy as np
from typing import  Optional, Deque
from collections import deque
import ssl
import aiohttp
from aiortc import RTCPeerConnection, RTCSessionDescription, RTCIceServer, RTCConfiguration
from aiortc.rtcicetransport import RTCIceCandidate
from antmedia_ser.token_api import TokenManager
import json
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AntMediaCamera")
logging.getLogger("aiortc").setLevel(logging.DEBUG)
import cv2

# STUN server for WebRTC connections
STUN_SERVER_URL = "stun:stun1.l.google.com:19302"

class AntMediaCamera:
    """
    A camera-like interface for AntMedia WebRTC streams that mimics OpenCV's VideoCapture
    Implementation using aiortc library for improved WebRTC compatibility
    """
    token_manager = TokenManager()
    
    def __init__(self, websocket_url: str, stream_id: str, buffer_size: int = 60) -> None:
        """
        Initialize the AntMediaCamera.
        
        Args:
            websocket_url: WebSocket URL for the AntMedia server
            stream_id: Stream ID to connect to
            buffer_size: Maximum size of the frame buffer
        """
        self.websocket_url: str = websocket_url
        self.stream_id: str = stream_id
        
        # Use deque for better performance with maxlen to automatically drop oldest frames
        self.frame_buffer: Deque[np.ndarray] = deque(maxlen=buffer_size)
        self.buffer_lock: threading.Lock = threading.Lock()  # For thread safety
        
        self.latest_frame: Optional[np.ndarray] = None
        self.running: bool = True
        self.connected: bool = False
        
        # Create instance logger with stream ID for better tracking
        self.logger: logging.Logger = logging.getLogger(f"AntMediaCamera-{stream_id}")
        
        # Get tokens using shared TokenManager - this will use cached token if available
        self.play_token = self._get_play_token()
        
        # For async/sync bridging
        self.loop = None
        self.client = None
        self.websocket = None
        self.peer_connection = None
        self.stop_event = threading.Event()
        self.connection_established = threading.Event()
        
        # Start WebRTC connection in a background thread with event loop
        self.thread: threading.Thread = threading.Thread(target=self._connect_and_receive, daemon=True)
        self.thread.start()
        
        # Wait for the connection to be established (with timeout)
        timeout: int = 15  # seconds - increased timeout for aiortc which may take longer
        start_time: float = time.time()
        while not self.connected and time.time() - start_time < timeout:
            time.sleep(0.1)
        
        if not self.connected:
            self.logger.warning("Connection to AntMedia server timed out")
            
        self.frame_retry = time.time()
    
    def _get_play_token(self) -> str:
        """Get the play token for the stream"""
        try:
            # This will use the cached token if it exists
            token = AntMediaCamera.token_manager.get_play_token(self.stream_id)
            if token:
                self.logger.info(f"Got play token for stream ID: {self.stream_id}")
                return token
            else:
                self.logger.warning(f"Failed to get play token for stream ID: {self.stream_id}")
                return ""
        except Exception as e:
            self.logger.error(f"Error getting play token: {e}")
            return ""
    
    def _create_event_loop(self):
        """Create a new event loop for the current thread"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
    
    def _connect_and_receive(self) -> None:
        """Connect to AntMedia server and set up the WebRTC connection in a background thread"""
        try:
            self.logger.info(f"Starting aiortc WebRTC connection for stream ID: {self.stream_id}")
            
            # Create new event loop for this thread
            self.loop = self._create_event_loop()
            
            # Run the async connection method in the event loop
            self.loop.run_until_complete(self._async_connect_and_receive())
            
        except Exception as e:
            self.logger.error(f"WebRTC connection error: {e}", exc_info=True)
            self.running = False
    
    async def _connect_websocket(self):
        """Connect to the AntMedia server WebSocket endpoint"""
        # Extract the hostname from the URL
        if '://' in self.websocket_url:
            hostname = self.websocket_url.split('://')[-1]
        else:
            hostname = self.websocket_url
        
        # If hostname contains path, remove it
        if '/' in hostname:
            hostname = hostname.split('/')[0]
        
        # Default AntMedia WebRTC app name
        app_name = "WebRTCAppEE"
        
        # Websocket URL
        ws_url = f"wss://{hostname}/{app_name}/websocket"
        
        try:
            self.logger.info(f"Connecting to WebSocket: {ws_url}")
            session = aiohttp.ClientSession()
            
            # Create SSL context that doesn't verify certificate (for testing)
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            self.websocket = await session.ws_connect(
                ws_url, 
                ssl=ssl_context
            )
            
            self.logger.info("WebSocket connected")
            self.client = AntMediaClient(self.stream_id, self.play_token, self.websocket)
            return True
        except Exception as e:
            self.logger.error(f"WebSocket connection error: {e}")
            return False
    
    async def _async_connect_and_receive(self):
        """Asynchronous method to connect to the server and handle frames"""
        try:
            # Connect WebSocket
            websocket_connected = await self._connect_websocket()
            if not websocket_connected:
                return
            
            # Configure WebRTC
            config = RTCConfiguration([RTCIceServer(urls=STUN_SERVER_URL)])
            self.peer_connection = RTCPeerConnection(config)
            
            # Frame callback to store frames in our buffer
            @self.peer_connection.on("track")
            async def on_track(track):
                self.logger.info(f"Track received: {track.kind}")
                
                if track.kind == "video":
                    self.logger.info("Video track established")
                    self.connected = True
                    self.connection_established.set()
                    
                    # Process frames
                    while self.running and not self.stop_event.is_set():
                        try:
                            # Get frame from track
                            frame = await track.recv()
                            
                            # Convert frame to numpy array
                            if hasattr(frame, 'to_ndarray'):
                                # Convert frame to BGR (OpenCV format)
                                img = frame.to_ndarray(format='bgr24')
                                                                
                                # Store the frame
                                self.latest_frame = img
                                # Add to frame buffer (thread-safe)
                                with self.buffer_lock:
                                    self.frame_buffer.append(img)
                        except Exception as e:
                            if not self.stop_event.is_set():
                                self.logger.error(f"Error processing video frame: {e}")
                                await asyncio.sleep(0.1)  # Don't spin too fast on errors
            
            # Send play request
            await self.client.play(self.peer_connection)
            
            # Main loop to keep the connection alive
            ping_interval = 30  # seconds
            last_ping_time = time.time()
            
            # Send pings to keep the connection alive
            while self.running and not self.stop_event.is_set():
                # Check if it's time to send a ping
                current_time = time.time()
                if current_time - last_ping_time >= ping_interval:
                    if self.websocket and not self.websocket.closed:
                        try:
                            await self.websocket.send_str('{"command": "ping"}')
                            self.logger.debug("Ping sent")
                            last_ping_time = current_time
                        except Exception as e:
                            self.logger.error(f"Error sending ping: {e}")
                            break
                
                # Process messages from the WebSocket
                if self.websocket and not self.websocket.closed:
                    try:
                        message = await asyncio.wait_for(self.websocket.receive(), timeout=0.1)
                        if message.type == aiohttp.WSMsgType.TEXT:
                            await self.client.process_message(message.data, self.peer_connection)
                        elif message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            self.logger.error(f"WebSocket closed: {message}")
                            break
                    except asyncio.TimeoutError:
                        # This is expected, just continue
                        pass
                    except Exception as e:
                        if not self.stop_event.is_set():
                            self.logger.error(f"Error processing WebSocket message: {e}")
                
                await asyncio.sleep(0.01)  # Avoid spinning too fast
                
        except Exception as e:
            self.logger.error(f"Error in async connection handler: {e}", exc_info=True)
        finally:
            # Cleanup on exit
            if hasattr(self, 'websocket') and self.websocket:
                if not self.websocket.closed:
                    await self.websocket.close()
            
            if hasattr(self, 'peer_connection') and self.peer_connection:
                await self.peer_connection.close()
            
            if hasattr(self, 'client') and self.client:
                await self.client.close()
            
            self.logger.info("WebRTC connection closed")
        
    def read(self):
        """
        Read a frame from the WebRTC stream (mimics OpenCV's VideoCapture.read())

        Returns:
            tuple: (success, frame) where success is True if a frame was retrieved
        """
        if not self.connected:
            return False, None

        with self.buffer_lock:
            if len(self.frame_buffer) > 0:
                self.frame_retry = time.time()
                frame = self.frame_buffer.popleft().copy()  # Get and remove the oldest frame
                return True, frame

        if time.time() - self.frame_retry < 20:
            return False, np.zeros((64, 64, 3), dtype=np.uint8)
        else:
            return False, None
    
    def release(self) -> None:
        """
        Release resources (mimics OpenCV's VideoCapture.release())
        """
        self.logger.info("Releasing AntMediaCamera resources for stream: %s", self.stream_id)
        
        # Set running to False to stop the loop
        self.running = False
        self.stop_event.set()
        
        # If we have an event loop, schedule the cleanup
        if self.loop:
            try:
                # Stop the event loop
                self.loop.call_soon_threadsafe(self.loop.stop)
            except Exception as e:
                self.logger.error(f"Error stopping event loop: {e}")
        
        # Wait for the background thread to finish
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
            if self.thread.is_alive():
                self.logger.warning("Background thread did not terminate, may cause resource leaks")
        
        # Clear references
        self.latest_frame = None
        with self.buffer_lock:
            self.frame_buffer.clear()
        
        self.logger.info("AntMediaCamera resources released")
    
    def stop(self):
        """
        Stop method for compatibility with other camera interfaces
        Delegates to release() method
        """
        self.logger.info("Stop method called for AntMediaCamera - calling release()")
        self.release()


class AntMediaClient:
    """Client for Ant Media Server WebRTC communications"""
    
    def __init__(self, stream_id, token, websocket):
        """
        Initialize the client
        
        Args:
            stream_id: The ID of the stream to play
            token: Authentication token
            websocket: Connected websocket instance
        """
        self.stream_id = stream_id
        self.token = token
        self.websocket = websocket
        self.logger = logging.getLogger(f"AntMediaClient-{stream_id}")
        self.peer_connections = {}
        
    async def play(self, peer_connection):
        """
        Send play request and set up event handlers
        
        Args:
            peer_connection: RTCPeerConnection instance
        """
        # Store the peer connection in our dictionary

        self.peer_connections[self.stream_id] = peer_connection
        # Add event handlers for ICE candidates
        @peer_connection.on("icecandidate")
        async def on_icecandidate(event):
            if event.candidate:
                candidate = event.candidate.candidate
                sdpMLineIndex = event.candidate.sdpMLineIndex or 0
                
                message = {
                    "command": "takeCandidate",
                    "streamId": self.stream_id,
                    "candidate": candidate,
                    "label": sdpMLineIndex,
                    "id": sdpMLineIndex
                }
                
                await self.websocket.send_str(json.dumps(message))
        
        # Send play request
        play_request = {
            "command": "play",
            "streamId": self.stream_id,
        }
        
        # Add token if available
        if self.token:
            play_request["token"] = self.token
            
        self.logger.info(f"Sending play request for {self.stream_id}")
        await self.websocket.send_str(json.dumps(play_request))
    
    async def process_message(self, message_data, peer_connection):
        """
        Process a message from the server
        
        Args:
            message_data: Message data as string
            peer_connection: RTCPeerConnection instance
        """
        import json
        
        try:
            data = json.loads(message_data)
            command = data.get("command")
            
            if command == "start":
                self.logger.info(f"Stream started: {data.get('streamId')}")
                
            elif command == "takeConfiguration":
                await self._handle_sdp(data, peer_connection)
                
            elif command == "takeCandidate":
                await self._handle_ice_candidate(data)
                
            elif command == "notification":
                definition = data.get("definition", "")
                self.logger.info(f"Notification: {definition}")
                
            elif command == "error":
                self.logger.error(f"Error: {data.get('definition')}")
        
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON: {message_data}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
    
    async def _handle_sdp(self, data, peer_connection):
        """
        Handle SDP configuration from the server
        
        Args:
            data: SDP data from server
            peer_connection: RTCPeerConnection instance
        """
        stream_id = data.get("streamId")
        sdp_type = data.get("type")
        sdp = data.get("sdp")
        
        if stream_id != self.stream_id:
            return
        
        if sdp_type == "offer":
            self.logger.info(f"Setting remote description (offer)")
            offer = RTCSessionDescription(sdp=sdp, type=sdp_type)
            await peer_connection.setRemoteDescription(offer)
            
            # Create answer
            answer = await peer_connection.createAnswer()
            await peer_connection.setLocalDescription(answer)
            
            # Send answer
            response = {
                "command": "takeConfiguration",
                "streamId": self.stream_id,
                "type": "answer",
                "sdp": peer_connection.localDescription.sdp
            }
            await self.websocket.send_str(json.dumps(response))
    
    async def _handle_ice_candidate(self, data):
        """Handle ICE candidate from the server"""
        stream_id = data.get("streamId")
        candidate_str = data.get("candidate")
        label = data.get("label", 0)
        
        if not stream_id or not candidate_str:
            self.logger.error("Missing streamId or candidate")
            return
        
        if stream_id not in self.peer_connections:
            self.logger.error(f"No peer connection for stream: {stream_id}")
            return
        
        pc = self.peer_connections[stream_id]
        self.logger.info(f"[ICE] Received candidate from server: {candidate_str}")
        
        try:
            # Parse the candidate string to extract components
            # Format: candidate:foundation component protocol priority ip port typ type [...]
            parts = candidate_str.split()
            if len(parts) < 8:
                self.logger.error(f"Invalid ICE candidate format: {candidate_str}")
                return
                
            # Extract the basic required fields
            foundation = parts[0].split(':')[1]  # Remove 'candidate:' prefix
            component = int(parts[1])
            protocol = parts[2]
            priority = int(parts[3])
            ip = parts[4]
            port = int(parts[5])
            # parts[6] should be "typ"
            type = parts[7]
            
            # Create a proper RTCIceCandidate object with parsed values
            ice_candidate = RTCIceCandidate(
                component=component,
                foundation=foundation,
                ip=ip,
                port=port,
                priority=priority,
                protocol=protocol,
                type=type,
                sdpMid=str(label),
                sdpMLineIndex=int(label)
            )
            
            # Add the ICE candidate
            await pc.addIceCandidate(ice_candidate)
            self.logger.info(f"Successfully added ICE candidate for stream {stream_id}")
        except Exception as e:
            self.logger.error(f"Error adding ICE candidate: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
    
    async def close(self):
        """Close client resources"""
        # Nothing to do here as the WebSocket is managed by the caller
        pass