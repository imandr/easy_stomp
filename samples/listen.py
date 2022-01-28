import time, sys, getopt
from pythreader import Primitive
from stompy import connect

Usage = """
python listen.py <broker host> <port> <destination>
"""

opts, args = getopt.getopt(sys.argv[1:], "")

if not args:
    print(Usage)
    sys.exit(2)

host, port, dest = args
port = int(port)

client = connect((host, port))
client.subscribe(dest, "client")
for frame in client:
    sender = frame.get("sender", "???")
    print(f"received: {frame.text} from {sender}")
