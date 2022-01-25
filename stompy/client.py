from .frame import STOMPFrame, STOMPStream, STOMPError, AckMode
from socket import socket, AF_INET, SOCK_STREAM
from .util import to_str, to_bytes
from pythreader import Primitive, synchronized, Promise

class STOMPSubscription(object):
    
    def __init__(self, client, id, dest, ack_mode, send_acks):
        self.ID = id
        self.Destination = dest
        self.AckMode = ack_mode
        self.Client = client
        self.SendAcks = send_acks
        
    def cancel(self):
        self.Client.unsubscribe(self.ID)
        self.Client = None
        
class STOMPTransaction(object):
    
    def __init__(self, client, txn_id):
        self.ID = txn_id
        self.Client = client
        self.Closed = False
        
    def send(self, command, **args):
        """
        Sends a STOMP frame to the broker, associating it with the transaction.
        
        :param str command: frame command

        Other arguments are the same as for the STOMPClient.send()
        """
        if self.Closed:
            raise STOMPError("Transaction already closed")
        return self.Client.send(command, transaction=self.ID, **args)
        
    def message(self, *params, **args):
        """
        Sends MESSAGE frame and associates it with the transaction. The method has same arguments as the
        STOMPClient.message() method.
        """
        if self.Closed:
            raise STOMPError("Transaction already closed")
        return self.Client.message(*params, transaction=self.ID, **args)
        
    def recv(self, timeout=None):
        """
        Receives next frame from the Broker. If the subscription allows sendig ACKs, the ACK will be associated
        with the transaction.
        """
        if self.Closed:
            raise STOMPError("Transaction already closed")
        return self.Client.recv(transaction=self.ID, timeout=timeout)
        
    def commit(self, receipt=None):
        """
        Commits the transaction.
        
        :param str or boolean receipt: if True or non-empty string, the frame will include "receipt" header.
            If receipt is a str, it will be used as is.
            If receipt=True, the client will generate new receipt id.
            If receipt=False, do not require a receipt.
        :return: Promise object if the receipt was requested (``receipt`` was not False), otherwise None
        """
        if self.Closed:
            raise STOMPError("Transaction already closed")
        out = self.Client.send("COMMIT", transaction=self.ID, receipt=receipt)
        self.Closed = True
        return out
        
    def abort(self, receipt=None):
        """
        Aborts the transaction.
        
        :param str or boolean receipt: if True or non-empty string, the frame will include "receipt" header.
            If receipt is a str, it will be used as is.
            If receipt=True, the client will generate new receipt id.
            If receipt=False, do not require a receipt.
        :return: Promise object if the receipt was requested (``receipt`` was not False), otherwise None
        """
        if self.Closed:
            raise STOMPError("Transaction already closed")
        out = self.Client.send("ABORT", transaction=self.ID, **args)
        self.Closed = True
        return out

    def nack(self, ack_id):
        """
        Sends NACK associated with the transaction
        
        :param string: ack id
        """
        if self.Closed:
            raise STOMPError("Transaction already closed")
        return self.Client.nack(ack_id, transaction=self.ID)

    def ack(self, ack_id, transaction=None):
        """
        Sends ACK associated with the transaction
        
        :param string: ack id
        """
        if self.Closed:
            raise STOMPError("Transaction already closed")
        return self.Client.ack(ack_id, transaction=self.ID)

        
class STOMPClient(Primitive):
    
    ProtocolVersion = "1.2"         # the only supported version
    
    def __init__(self):
        """
        STOMPClient constructor does not have any arguments.
        """
        Primitive.__init__(self)
        self.Sock = None
        self.BrokerAddress = None
        self.Connected = False
        self.Stream = None
        self.NextID = 1
        self.Subscriptions = {}         # id -> subscription
        self.Callbacks = []
        self.ReceiptPromises = {}              # receipt-id -> promise
        
    def next_id(self, prefix=""):
        out = self.NextID
        self.NextID += 1
        if prefix: prefix = prefix + "."
        return f"{prefix}{out}"
    
    @synchronized
    def connect(self, addr_list, login=None, passcode=None, headers={}, timeout=None, **kv_headers):
        """
        Connects to a broker. On successfull connection, sets the following attributes:
        
        client.BrokerAddress - tuple (ip_address, port) - actual address of the broker the connection was established to
        clint.Connected = True
        
        :param addr_list: a single broker address as tuple (ip_address, port), or a list of tuples - addresses
        :param str login: login id to use, default: None
        :param str passcode: pass code to use, default: None
        :param dict headers: additional headers for the CONNECT frame, default: none
        :param kv_headers: additional headers for the CONNECT frame
        :return: CONNECTED frame returned by the broker
        """
        if self.Connected:
            raise RuntimeError("Already connected")
            
        if not isinstance(addr_list, list):
            addr_list = [addr_list]
            
        last_error = None
        response = None
        broker_addr = None
        for addr in addr_list:
            sock = socket(AF_INET, SOCK_STREAM)
            try:    sock.connect(addr)
            except: continue
            
            stream = STOMPStream(sock)
            
            headers = {"accept-version":self.ProtocolVersion}
            if login is not None:   headers["login"] = login
            if passcode is not None:   headers["passcode"] = passcode
            frame = STOMPFrame("CONNECT", headers=headers)
            stream.send(frame)
            response = stream.recv(timeout=timeout)
            if response.Command == "ERROR":
                last_error = STOMPError(response.get("message", ""), response.Body)
            elif response.Command != "CONNECTED":
                last_error = STOMPError(f"Error connecting to the broker. Unknown response command: {response.Command}",
                    response)
            else:
                self.Connected = True
                self.Stream = stream
                self.Sock = sock
                self.BrokerAddress = addr
                break
        if not self.Connected:
            if last_error:  raise last_error
            else:   raise RuntimeError("Failed to connect to a broker")
        return response        

    @synchronized
    def add_callback(self, callback):
        """
        Add callback object to receive message frames during the loop(). The callback object has to be a callable.
        It will be called with 2 arguments: the STOMPClient instance and the STOMPFrame instance.
        If the call returns "stop", then the loop will be stopped. All other return values are ignored.
        
        .. code-block:: python
        
            def my_callback(client, frame):
                # ...
                if want_to_stop_loop:
                    return "stop"
                else:
                    return None

            client.add_callback(my_callback)
            client.loop()
            # the loop will end if:
            # - client disconnects or
            # - a callback returns "stop"
        
        :param object callback: callable
        """
        self.remove_callback(callback)
        self.Callbacks.append(callback)
        
    @synchronized
    def remove_callback(self, callback=None):
        """
        Remove a single callback object or all callback objects
        
        :param object callback: - callable previously added by add_callback() or None. If None, all callback objects will be removed.
        """
        
        if callback is None:
            self.Callbacks = []
        else:
            try:    self.Callbacks.remove(callback)
            except: pass

    def subscribe(self, dest, ack_mode="auto", send_acks=True):
        """
        Subscribe to messages sent to the specified destination
        
        :param str dest: destination
        :param str ack_mode: can be either "auto" (default), "client" or "client-individual"
        :param boolean send_acks: whether the client should automatically send ACKs received on this scubscription
        :return: subscription id
        :rtype: str
        """
        if not isinstance(ack_mode, AckMode):
            ack_mode = AckMode(ack_mode)
        subscription = STOMPSubscription(self, self.next_id("s"), dest, ack_mode, send_acks)
        self.send("SUBSCRIBE", headers={
            "destination":dest,
            "ack":ack_mode.value,
            "id":subscription.ID
        })
        self.Subscriptions[subscription.ID] = subscription
        return subscription.ID

    def unsubscribe(self, sub_id):
        """
        Remove subscription
        
        :param str sub_id: subscription id
        """
        subscription = self.Subscriptions.pop(sub_id, None)
        if subscription is not None:
            self.send("UNSUBSCRIBE", id=sub_id, receipt=True)

    def send(self, command, headers={}, body=b"", transaction=None, receipt=False, **kv_headers):
        """
        Send the frame. If a receipt was requested, then the frame sent by the client will incude "receipt" header
        and the method will return a Promise object, which can be used to wait for the recept to be returned. e.g.:
        
            promise = client.send("MESSAGE", body="hello", receipt=True)
            ...
            promise.wait() # will block until the message is processed by the broker
        
        :param str command: frame command
        :param dict headers: frame headers, default - {}
        :param bytes body: frame body, default - empty body
        :param str or boolean receipt: if True or non-empty string, the frame will include "receipt" header.
            If receipt is a str, it will be used as is.
            If receipt=True, the client will generate new receipt id.
            If receipt=False, do not require a receipt.
        :param kv_headers: additional headers to add to the frame
        :return: Promise object if the receipt was requested (``receipt`` was not False), otherwise None
        """
        h = {}
        h.update(headers)
        h.update(kv_headers)
        promise = None
        if receipt == True:
            receipt = self.next_id("r")
        if receipt:
            h["receipt"] = receipt
            promise = self.ReceiptPromises[receipt] = Promise(receipt)
        frame = STOMPFrame(command, headers=h, body=to_bytes(body))
        self.Stream.send(frame)
        return promise
        
    def message(self, destination, body=b"", id=None, headers={}, receipt=False,
                    transaction=None, **kv_headers):
        """
        Conventience method to send a message. Uses send().

        :param str destination: destination to send the message to
        :param bytes body: message body, default - empty
        :param str or None id: add message-id header, if not None
        :param dict headers: headers to add to the message, default - empty
        :param boolean or str receipt: if True or non-empty string, the frame will include "receipt" header.
            If ``receipt`` is a str, it will be used as is.
            If ``receipt`` is True, the client will generate new receipt id.
            If ``receipt`` is False, do not require a receipt.
        :param str transaction: transaction id to associate the frame with, or None
        :return: Promise object if the receipt was requested (``receipt`` was not False), otherwise None
        """
        h = {}
        h.update(headers)
        if id is not None:
            h["message-id"] = id
        return self.send("SEND", headers=h, body=body, destination=destination, 
                receipt=receipt, transaction=transaction, **kv_headers)

    def callback(self, frame):
        for cb in self.Callbacks:
            if cb(self, frame) == "stop":
                return "stop"

    @synchronized
    def recv(self, transaction=None, timeout=None):
        """
        Receive next frame. If the next frame is RECEIPT, notify those who are waiting for it and keep receiving.
        Return None if the connection closed. Raise STOMPError on ERROR.
        
        :param str or None transaction: transaction to associate the automatically sent ACK, or None
        :return: frame received or None, if the connection was closed
        :rtype: STOMPFrame or None
        """
        done = False
        frame = None
        while not done:
            #print("Client.recv: read...")
            frame = self.Stream.recv(timeout)
            #print("Client.recv: received:", frame)
            if frame is None:
                # EOF
                self.close()
                return None
            elif frame.Command == "RECEIPT":
                receipt = frame["receipt-id"]
                promise = self.ReceiptPromises.pop(receipt, None)
                if promise is not None:
                    #print("Client.recv: promise fulfilled:", receipt)
                    promise.complete(frame)
                else:
                    #print(f"Client.recv: promise for {receipt} not found")
                    #print("   promises:", [(k, p.Data) for k, p in self.ReceiptPromises.items()])
                    pass
            elif frame.Command == "ERROR":
                raise STOMPError(frame.get("message", ""), frame)
            else:
                if frame.Command == "MESSAGE" and "ack" in frame:
                    sub_id = frame["subscription"]
                    subscription = self.Subscriptions.get(sub_id)
                    if subscription is None or subscription.SendAcks:
                        self.ack(frame["ack"], transaction)
                done = True
        return frame

    def loop(self, transaction=None, timeout=timeout):
        """
        Run the client in the loop, receiving frames by the broker and sending them to the client callbacks.
        The loop will break once a callback retruns string "stop"
        """
        frame = 1
        while frame is not None:
            frame = self.recv(transaction=transaction, timeout=timeout)
            if frame is not None:
                if self.callback(frame) == "stop":
                    break
        return frame

    def nack(self, ack_id, transaction=None):
        """
        Send NACK frame
        
        :param str ack_id: NACK id to send
        :param str or None transaction: transaction id to associate the NACK with, default: None
        """
        headers = {"id":ack_id}
        if transaction is not None:
            headers["transaction"] = transaction
        self.send("NACK", headers)

    def ack(self, ack_id, transaction=None):
        """
        Send ACK frame
        
        :param str ack_id: NACK id to send
        :param str or None transaction: transaction id to associate the ACK with, default: None
        """
        headers = {"id":ack_id}
        if transaction is not None:
            headers["transaction"] = transaction
        self.send("ACK", headers)

    def transaction(self, txn_id=None):
        """
        Creates and begins new transaction
        
        :param str or None txn_id: transaction ID or None (default), in which case a new transaction ID will be generated
        """
        txn_id = txn_id or self.next_id("t")
        self.send("BEGIN", transaction=txn_id, receipt=True)
        return STOMPTransaction(self, txn_id)

    @synchronized
    def disconnect(self):
        """
        Send DISCONNECT frame, wait for receipt and close the connection.
        """
        if self.Connected:
            receipt = self.send("DISCONNECT", receipt=True)
            self.wait_for_receipt(receipt)
            self.close()

    @synchronized
    def close(self):
        if self.Connected:
            self.Sock.close()
            self.Sock = None
            self.Callbacks = None
            self.Subscriptions = None
            self.Connected = False
        
    def __del__(self):
        self.disconnect()

    def __iter__(self):
        """
        The client can be used as an iterator, returning next received frame on every iteration. The iteration stops
        when the connection closes:
    
        .. code-block:: python

            client = STOMPClient()
            client.connect(...)
            for frame in client:
                ...
            # connection closed
        
        """
        return MessageIterator(self)

class MessageIterator(object):

    def __init__(self, client):
        self.Client = client
        
    def __next__(self):
        frame = self.Client.recv()
        if frame is None:
            raise StopIteration()
        return frame
        
def connect(addr_list, login=None, passcode=None, headers={}):
    """
    Creates the client object and connects it to the Broker
    
    :param addr_list: a single broker address as tuple (ip_address, port), or a list of tuples - addresses
    :param str login: login id to use, default: None
    :param str passcode: pass code to use, default: None
    :param dict headers: additional headers for the CONNECT frame, default: none
    :return: STOMPlient instance connected to the Broker
    :rtype: STOMPClient    
    """
    client = STOMPClient()
    client.connect(addr_list, login=login, passcode=passcode, headers=headers)
    return client

