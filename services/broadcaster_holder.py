"""
Holds singleton for broadcaster
"""

from engine_server.broadcasters.engine_broadcaster import OrderBroadcaster


def get_broadcaster():
    global broadcaster
    if broadcaster:
        return broadcaster
    else:
        broadcaster = OrderBroadcaster()
