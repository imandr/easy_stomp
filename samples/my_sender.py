import time
import sys

from stompy import STOMPClient

client = STOMPClient()
client.connect(("127.0.0.1", 61613), 'admin', 'password')
txn = client.transaction()
txn.message('/queue/send', "Message 1")
txn.message('/queue/send', "Message 2")
txn.commit(receipt=True)
client.disconnect()
