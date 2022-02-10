from socket import *
import sys, uuid
from stompy import STOMPStream, STOMPFrame, AckMode
from pythreader import PyThread, Primitive, DEQueue, synchronized, TaskQueue, Task

class Subscription(Primitive):
    
    def __init__(self, client, id, dest, ack_mode):
        Primitive.__init__(self, name=f"Subscription {dest}({id}, {ack_mode})")
        self.ID = id
        self.Destination = dest
        if isinstance(ack_mode, str):
            ack_mode = AckMode(ack_mode)
        self.AckMode = ack_mode
        self.Unacked = {}               # {ack_id -> original frame}
        self.Client = client
        
    def unacked(self):
        return self.Unacked.values()
        
    @synchronized
    def add_unacked(self, ack_id, frame):
        assert frame.Command == "SEND"      # make sure that the frame is the original frame
        self.Unacked[ack_id] = frame
        
    @synchronized
    def ack(self, ack_id):
        frame = self.Unacked.pop(ack_id)
        if self.AckMode == AckMode.Client:
            for aid in list(self.Unacked.keys()):
                if aid <= ack_id:
                    del self.Unacked[aid]
        return frame
                    
    @synchronized
    def nack(self, ack_id):
        return self.Unacked.pop(ack_id)            # will cause KeyError if not found
        
    def send_message(self, frame):
        headers = frame.headers       # this will make a copy, so we can modify headers here without affecting the original frame
        if "message-id" not in headers:
            headers["message-id"] = self.Client.next_id("m")
        headers["subscription"] = self.ID
        if "receipt" in headers:    del headers["receipt"]
        if self.AckMode != AckMode.Auto:
            ack_id = self.Client.next_id("a")
            headers["ack"] = ack_id
            self.add_unacked(ack_id, frame)
        msg = STOMPFrame("MESSAGE", frame.Body, headers)
        self.Client.send(msg)
        
class Transaction(Primitive):
    def __init__(self, id):
        Primitive.__init__(self, name=id)
        self.Frames = []
        print(self, "created")
        
    def add(self, f):
        del f.Headers["transaction"]
        try:    del f.Headers["receipt"]
        except KeyError:    pass
        self.Frames.append(f)
        print(self, ".add:", f)
        
    def abort(self):
        self.Frames = []
        
    def commit(self, client):
        for f in self.Frames:
            print(self, ".commit(): sending:", f)
            client.process(f)
        self.Frames = []

class ClientReader(PyThread):
    
    def __init__(self, client, stream):
        PyThread.__init__(self, name="Reader@{client.Addr}")
        self.Client = client
        self.Stream = stream
        self.Stop = False
    
    def run(self):
        for frame in self.Stream:
            self.Client.process(frame)
            if self.Stop:
                break
        self.Client.reader_ended()
        self.Client = None
        self.Stream = None
        
    def stop(self):
        self.Stop = True
        
    @synchronized
    def close(self):
        if self.Stream is not None:
            self.Stream.close()
            self.Stream = None
            
class ClientWriter(PyThread):
    
    def __init__(self, client, sock):
        PyThread.__init__(self, name="Writer@{client.Addr}")
        self.Client = client
        self.Sock = sock
        self.FrameQueue = DEQueue()
    
    def run(self):
        for frame in self.FrameQueue:
            try:    self.Sock.sendall(frame.to_bytes())
            except: break
        self.Client.writer_ended()
        self.Client = None
        self.Sock = None
            
    def send(self, frame):
        self.FrameQueue.append(frame)
        
    def close(self):
        self.FrameQueue.close()

class Client(Task):
    
    def __init__(self, broker, addr, sock):
        Task.__init__(self, name=str(addr))
        self.Addr = addr
        self.Sock = sock
        self.Broker = broker
        self.Subscriptions = {}                       # {subscription_id -> subscription}
        self.NextID = 1
        self.Reader = ClientReader(self, STOMPStream(sock))
        self.Writer = ClientWriter(self, sock)
        self.Connected = False
        self.Transactions = {}                      # {transaction_id: Transaction}
        
    def subscriptions(self):
        return self.Subscriptions.values()

    @synchronized
    def next_id(self, prefix=""):
        out = self.NextID
        self.NextID += 1
        if prefix: prefix = prefix + "."
        return f"{prefix}{out}"

    def run(self):
        self.Reader.start()
        self.Writer.start()
        self.Writer.join()
        self.Reader.close()
        self.Reader.join()
        try:    self.Sock.close()
        except: pass
        self.Broker.remove_client(self)
        self.Reader = self.Writer = None
        with self.Broker:
            for s in self.Subscriptions.values():
                self.Broker.remove_subscription
                for f in s.unacked():
                    self.Broker.nack(f)
        self.Subscriptions = None
        
    def disconnect(self):
        if self.Writer is not None:
            self.Writer.close()
            self.Writer = None
        self.Connected = False
    
    def reader_ended(self):
        self.disconnect()
        
    def writer_ended(self):
        pass
    
    def send(self, frame):
        print(self, ">> sending:", frame)
        self.Writer.send(frame)
    
    def ack(self, ack_id):
        for s in self.Subscriptions.values():
            try:    return s.ack(ack_id)
            except KeyError:    pass
        return None     # not found

    def nack(self, ack_id):
        for s in self.Subscriptions.values():
            try:    return s.nack(ack_id)
            except KeyError:    pass
        return None     # not found
        
    def error(self, message, body=b"", original_frame=None):
        if original_frame:
            dump = original_frame.to_bytes()
            if dump[-1:] == b"\x00":
                dump = dump[:-1]
            print(repr(dump))
            body = body + b"\n- original frame ---------------------\n" \
                + dump \
                +         b"\n- end of original frame --------------\n"
        self.send(STOMPFrame("ERROR", body, message=message))
        
    TRANSACTION_COMMANDS = {"BEGIN","ABORT","COMMIT","SEND","ACK","NACK"}
        
    def process_transaction(self, frame):
        
        # implements transaction handling functionality
        if frame.Command not in self.TRANSACTION_COMMANDS:
            return False            # ignore

        txn_id = frame.get("transaction")
        if txn_id is None:
            if frame.Command in ("BEGIN","ABORT","COMMIT"):
                self.error(f"Protocol error: transaction header not found", original_frame=frame)
                return True
            else:
                return False        # not in a transaction

        txn = self.Transactions.get(txn_id)

        if frame.Command == "BEGIN":
            if txn is None:
                self.Transactions[txn_id] = Transaction(txn_id)
            else:
                self.error(f"Protocol error. Transaction {txn_id} already in progress", original_frame=frame)

        elif txn is None:
            self.error(f"Protocol error. Transaction {txn_id} does not exist", original_frame=frame)
        
        elif frame.Command in ("SEND","ACK","NACK"):
            txn.add(frame)

        elif frame.Command == "COMMIT":
            txn.commit(self)

        else:           # command == "ABORT"
            txn.abort()

        return True

    def process(self, frame):
        print(self, "<< received:", frame)
        if not self.Connected:
            if frame.Command == "STOMP" or frame.Command == "CONNECT":
                versions = frame["accept-version"].split(",")
                self.send(STOMPFrame("CONNECTED", version=max(versions)))
                self.Connected = True
            else:
                self.error("Protocol error. Expected CONNECT or STOMP frame", original_frame=frame)

        else:
            receipt = frame.get("receipt")

            if self.process_transaction(frame):
                pass
            elif frame.Command == "ACK":
                self.ack(frame["id"])
            elif frame.Command == "NACK":
                message = self.nack(frame["ack"])
                if message:
                    self.Broker.nack(message)
            elif frame.Command == "SEND":
                self.Broker.message_received(frame)
            elif frame.Command == "SUBSCRIBE":
                sid = frame["id"]
                subscription = Subscription(self, sid, frame["destination"], frame["ack"])
                self.Subscriptions[sid] = subscription
                self.Broker.add_subscription(subscription)
            elif frame.Command == "UNSUBSCRIBE":
                sid = frame["id"]
                try:    
                    subscription = self.Subscriptions.pop(sid)
                    self.Broker.remove_subscription(subscription)
                except KeyError:
                    pass
            elif frame.Command == "DISCONNECT":
                receipt = frame.get("receipt")
                if receipt:
                    self.send(STOMPFrame("RECEIPT", headers={"receipt-id":receipt}))
                self.disconnect()
            else:
                self.error(f"Protocol error. Unrecognized or invalid command {frame.Command}", original_frame=frame)
                self.disconnect()

            if self.Connected and receipt:
                self.send(STOMPFrame("RECEIPT", headers={"receipt-id":receipt}))

class Broker(PyThread):
    
    def __init__(self, config):
        PyThread.__init__(self, name="Broker")
        self.SubscriptionsByDest = {}               # {dest -> [Subscription, ...]}
        self.Port = config.get("port", 61613)
        max_clients = config.get("max_clients", 250)
        self.Clients = TaskQueue(max_clients)
        self.MaxUndelivered = config.get("max_undelivered", 100)
        self.Undelivered = {}                       # {dest -> [undelivered frames]}

    @synchronized
    def add_undelivered(self, frame):
        assert frame.Command == "SEND"
        dest = frame["destination"]
        lst = self.Undelivered.setdefault(dest, [])
        lst.append(frame)
        while len(lst) > self.MaxUndelivered:
            lst.pop(0)

    @synchronized
    def add_subscription(self, subscription):
        self.SubscriptionsByDest.setdefault(subscription.Destination, []).append(subscription)
        undelivered = self.Undelivered.get(subscription.Destination, [])
        self.Undelivered[subscription.Destination] = []
        for frame in undelivered:
            self.route(frame)

    @synchronized
    def remove_subscription(self, subscription):
        lst = self.SubscriptionsByDest.get(subscription.Destination, [])
        try:    lst.remove(subscription)
        except: pass

    def message_received(self, frame):
        self.route(frame)
        
    def route_queue(self, frame, dest):
        subscriptions = self.SubscriptionsByDest.get(dest)
        if subscriptions:
            s = subscriptions.pop(0)        # round-robin
            subscriptions.append(s)
            s.send_message(frame)
        else:
            self.add_undelivered(frame)
            
    def route_topic(self, frame, dest):
        subscriptions = self.SubscriptionsByDest.get(dest, [])
        for s in subscriptions:
            s.send_message(frame)

    def route(self, frame):
        dest = frame.destination
        if dest.startswith("/topic/"):
            self.route_topic(frame, dest)
        else:
            self.route_queue(frame, dest)

    @synchronized
    def nack(self, frame):
        assert frame.Command == "SEND"  # make sure it's the original frame
        dest = frame.destination
        if dest.startswith("/queue/")
            self.route_queue(frame, dest)

    def run(self):
        lsn_sock = socket(AF_INET, SOCK_STREAM)
        lsn_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        lsn_sock.bind(('', self.Port))
        lsn_sock.listen(5)
        while True:
            conn, addr = lsn_sock.accept()
            #print("Connection accepted from", addr)
            self.Clients << Client(self, addr, conn)

    @synchronized
    def remove_client(self, client):
        subscriptions = list(client.subscriptions())
        for s in subscriptions:
            self.remove_subscription(s)
        for s in subscriptions:
            for f in s.unacked():
                self.nack(f)
                
def main():
    import yaml, os, getopt
    Usage = """
    Usage: 
    $ stompy_broker [-c <config.yaml>]
        if -c is missing environment variable STOMPY_BROKER_CFG will be used
    """
    
    opts, args = getopt.getopt(sys.argv[1:], "c:h?")
    opts = dict(opts)
    if "-h" in opts or "-?" in opts:
        print(Usage)
        sys.exit(2)
    config = opts.get("-c") or os.environ.get("STOMPY_BROKER_CFG")
    if not config or not os.path.isfile(config):
        print(Usage)
        sys.exit(2)
    config = yaml.load(open(config, "r"), Loader = yaml.SafeLoader)
    broker = Broker(config)
    broker.start()
    broker.join()
    

if __name__ == "__main__":
    main()
