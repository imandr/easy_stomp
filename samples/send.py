import time, sys, getopt
from pythreader import Primitive
from stompy import connect

Usage = """
python send.py [-n <nmessages>] <broker host> <port> <destination> 
"""

opts, args = getopt.getopt(sys.argv[1:], "n:")
opts = dict(opts)

if not args:
    print(Usage)
    sys.exit(2)

host, port, dest = args
port = int(port)
nmessages = int(opts.get("-n", 1))

client = connect((host, port))
for i in range(nmessages):
    client.message(dest, f"message #{i}")
