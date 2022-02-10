from .frame import STOMPFrame, STOMPStream, STOMPError, AckMode
from socket import socket, AF_INET, SOCK_STREAM
from .util import to_str, to_bytes
from pythreader import Primitive, synchronized
import ssl

def wait_for_receipt(client, frame, receipt):
    if frame is not None or frame.Command == "RECEIPT" and frame.get("receipt-id") == receipt:
        return receipt

class STOMPSubscription(object):
    
    def __init__(self, client, id, dest, ack_mode, send_acks):
        self.ID = id
        self.Destination = dest
        self.AckMode = ack_mode
        self.Client = client
        self.SendAcks = send_acks
        
    def __str__(self):
        return f"Subscription({self.ID}, dest={self.Destination}, mode={self.AckMode}, send_acks={self.SendAcks})"
        
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
        :return: receipt (str) the receipt was requested (``receipt`` was not False), otherwise None
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
        :return: receipt (str) if the receipt was requested (``receipt`` was not False), otherwise None
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
        self.SSLContext = None
        
    def next_id(self, prefix=""):
        out = self.NextID
        self.NextID += 1
        if prefix: prefix = prefix + "."
        return f"{prefix}{out}"
    
    @synchronized
    def connect(self, addr_list, login=None, passcode=None, headers={}, timeout=None, 
            cert_file = None, key_file = None, ca_file = None, key_password = None, **kv_headers):
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
            except Exception as e: 
                last_error = e
                continue
            else:   break
        else:
            raise last_error
            
        if cert_file or key_file:
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            context.load_cert_chain(cert_file, key_file, password=key_password)
            if ca_file:
                context.load_verify_locations(ca_file)
            sock = context.wrap_socket(sock, server_hostname = addr[0])
            self.SSLContext = context
            
        stream = STOMPStream(sock)
       
        headers = kv_headers.copy() 
        headers["accept-version"] = self.ProtocolVersion
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

        if not self.Connected:
            if last_error:  raise last_error
            else:   raise RuntimeError("Failed to connect to a broker")
        return response        

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
        and the method will return the receipt-id:
        
        :param str command: frame command
        :param dict headers: frame headers, default - {}
        :param bytes body: frame body, default - empty body
        :param str or boolean receipt: if True or non-empty string, the frame will include "receipt" header.
            If receipt is a str, it will be used as is.
            If receipt=True, the client will generate new receipt id.
            If receipt=False, do not require a receipt.
        :param kv_headers: additional headers to add to the frame
        :return: receipt (str) if the receipt was requested (``receipt`` was not False), otherwise None
        """
        h = {}
        h.update(headers)
        h.update(kv_headers)
        if receipt == True:
            receipt = self.next_id("r")
        if receipt:
            h["receipt"] = receipt
        frame = STOMPFrame(command, headers=h, body=to_bytes(body))
        self.Stream.send(frame)
        #print("sent:", frame)
        return receipt
        
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
        :return: receipt (str) if the receipt was requested (``receipt`` was not False), otherwise None
        """
        h = {}
        h.update(headers)
        if id is not None:
            h["message-id"] = id
        return self.send("SEND", headers=h, body=body, destination=destination, 
                receipt=receipt, transaction=transaction, **kv_headers)

    def process_receipt(self, frame):
        return
        """ old code
        if frame.Command == "RECEIPT":
            receipt = frame["receipt-id"]
            promise = self.ReceiptPromises.pop(receipt, None)
            if promise is not None:
                #print("Client.recv: promise fulfilled:", receipt)
                promise.complete(frame)
        """
        
    @synchronized
    def recv(self, transaction=None, timeout=None, exception_on_error=True):
        """
        Receive next frame. If the next frame is RECEIPT, notify those who are waiting for it and keep receiving.
        Return None if the connection closed. Raise STOMPError on ERROR.
        
        :param str or None transaction: transaction to associate the automatically sent ACK, or None
        :return: frame received or None, if the connection was closed
        :rtype: STOMPFrame or None
        """
        frame = self.Stream.recv(timeout)
        #print("Client.recv: received:", frame)
        if frame is None:
            # EOF
            self.close()
            return None
        elif frame.Command == "ERROR" and exception_on_error:
            raise STOMPError(frame.get("message", ""), frame)
        elif frame.Command == "RECEIPT":
            self.process_receipt(frame)
        elif frame.Command == "MESSAGE" and "ack" in frame:
            sub_id = frame["subscription"]
            subscription = self.Subscriptions.get(sub_id)
            #print("subscription:", subscription)
            if subscription is None or subscription.SendAcks:
                self.ack(frame["ack"], transaction)
        return frame

    def loop(self, *params, transaction=None, timeout=None, **callback_args):
        """
        The method can have zero or more positional arguments. First positional arguments will be the ``callback``.
        The remaining positional arguments will be passed as positional arguments to the ``callback``
        in addition to client and the frame.
        Keyword arguments will be passed to the ``callback`` except ``transaction`` and ``timeout``.

        The method will run the client in the loop, receiving frames from the broker, calling the ``callback``, 
        if present.
        The loop() will return once the callback (if any) returns something which evaluates to True or the connection closes.
        The loop() will return the last value returned by the callback or None if the loop stopped due to the
        disconnection.
        
        :param numeric timeout: read time-out in seconds, or None
        :param str transaction: transaction id to associate ACKs and NACKs sent during the loop, or None
        :return: The value returned by the last call to the ``callback``. If the loop stopped due to 
            disconnecrtion, returns None
        """
        
        callback = None if not params else params[0]
        callback_params = params[1:]
        
        frame = 1
        value = None
        while frame is not None and value is None:
            frame = self.recv(transaction=transaction, timeout=timeout, exception_on_error=False)
            #print("loop: frame:", frame)
            if frame is not None and callback is not None:    
                value = callback(self, frame, *callback_params, **callback_args)
        return value

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
            self.loop(wait_for_receipt, receipt)
            self.close()

    @synchronized
    def close(self):
        if self.Connected:
            self.Sock.close()
            self.Sock = None
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
        
def connect(addr_list, **args):
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
    client.connect(addr_list, **args)
    return client

