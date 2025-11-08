from fastapi import WebSocket

class ServerConnectionAdapter:
    """
    I built this class to keep the functionality of the existing OrderBroadcaster service I built.
    It expects a websocket object that has a method "send" instead of FastAPI's send_json or send_text.
    We are only sending jsons, so it makes sense to implement an adapter that wraps send_json with a universal "send function"
    """

    def __init__(self, fastapi_socket: WebSocket):
        self.socket = fastapi_socket

    async def send(self, message: str):
        """
        Expects a json formatted string built with something like json.dumps({})
        """
        await self.socket.send_json(message)