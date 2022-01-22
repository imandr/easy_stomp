import time
import sys
from pythreader import Primitive

from client import STOMPClient

class Listener(Primitive):
    
    def __init__(self):
        Primitive.__init__(self)
        self.Stop = False
        
    def on_error(self, client, frame):
        print('received an error "%s"' % frame)
        self.wakeup()

    def on_message(self, client, frame, **args):
        print('received a message "%s"' % frame)
        if frame.Body == b"stop":
            self.Stop = True
        self.wakeup()

    def on_disconnected(self, client, frame):
        print("on_disconnected")
        self.Stop = True
        self.wakeup()

ack_mode = "client"

listener = Listener()

client = STOMPClient()
client.connect(("127.0.0.1", 61613), 'admin', 'password')
client.subscribe('/queue/send', ack_mode)

for frame in client:
    print(frame)