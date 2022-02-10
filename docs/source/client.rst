STOMP Client
============

STOMPClient is used to connect to and communicate with the message broker. Here is how to create a client object and
connect it to the Broker:

Using STOMP Client
------------------

Connecting to the Broker
~~~~~~~~~~~~~~~~~~~~~~~~
.. code-block:: python

    import stompy

    port = 61613
    host = "host.domain.com"
    client = stompy.connect((host, port))

connect() function arguments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stompy.connect

Reading messages using iterator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import stompy

    port = 61613
    host = "host.domain.com"
    client = stompy.connect((host, port))
    
    client.subscribe("/queue/Q")
    client.subscribe("/topic/T")

    for frame in client:
        command = frame.Command
        headers = frame.headers()               # copy of the headers array
        source = frame["source"]                # headers can be accessed via mapping interface
        option = frame.get("option", "none")
        destination = frame.destination         # convenience, same as frame["destination"]
        body_as_bytes = frame.Body
        body_as_text = frame.text               # decoded with UTF-8
        # ... process frame
    
    # iterator stops when the connection closes

Reading messages using recv()
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    frame = client.recv()
    while frame is not None:
        # ... process frame
        if ...:
            break
        frame = client.recv()
            

Reading messages using callback
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def process_message(client, frame):
        # process message received by the client
        if ...:
            return frame     # non-False return will stop the loop()

    last_frame = client.loop(callback = process_message)


The ``loop`` method can have zero or more positional arguments. First positional arguments will be the ``callback``.
The remaining positional arguments will be passed as positional arguments to the ``callback``
in addition to the ``client`` and the ``frame``.
The method accepts 2 optional keyword arguments:

    transaction
        str, transaction ID to associate with all the ACKs and NACKs sent automatically during the loop
    timeout
        numeric, time-out used to receive individual frames. In case of time-out, STOMPTimeout exception will be
        raised
    
Additional keyword arguments can be specified. They will be passed to the ``callback`` as is. 

For example:

.. code-block:: python

    client.loop()                       # loop until disconnection without calling any callback

    def my_callback_1(client, frame):   # no additional arguments
        ...
        if ...:
            return True
    client.loop(my_callback_1)          # loop until my_callback_1 returns True

    def my_callback_2(client, frame, param1, param2, param3=None):  # 2 positional and 1 keyword arguments
        ...
        if ...:
            return True
    client.loop(my_callback_2, param1_value, param2_value, param3="hello")

    def my_callback_3(client, frame, param1, param2, param3=None):  # 2 positional and 1 keyword arguments
        ...
        if ...:
            return True
    client.loop(my_callback_3, param1_value, param2_value, transaction="txn", timeout=10.0, param3="hello")

The `loop()` method will run the client in the loop, receiving frames from the broker, calling the ``callback``, 
if present.
The `loop()` will return once the callback (if any) returns something which evaluates to True or the connection closes.
The `loop()` will return the last value returned by the callback or None if the loop stopped due to the
disconnection.
    
Waiting for a receipt
~~~~~~~~~~~~~~~~~~~~~
    
.. code-block:: python

    def wait_for_receipt(_, frame, receipt):
        # process message received by the client
        return frame is not None and frame.Command == "RECEIPT" and frame["receipt-id"] == receipt

    closed = client.loop(wait_for_receipt, receipt="the-receipt") == None
    

Sending ACKs/NACKs
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    client = stompy.connect((host, port))
    
    client.subscribe("/queue/Q", send_acks = False)         # disable auto-sending ACKs

    for frame in client:
        if frame.Command == "MESSAGE" and "ack" in frame:
            if ...:
                client.ack(frame["ack"])
            else:
                client.nack(frame["ack"])
                
Sending messages and other frames
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    client = stompy.connect((host, port))
    client.send("SEND", 
            destination="/queue/Q", 
            body="Hello there",                         # can by bytes or str
            source=str(os.getpid())                     # custom header
    )
    client.message("/queue/Q", "Hello there", source=str(os.getpid()))       # same as above
                
Sending messages and waiting for receipt
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    client = stompy.connect((host, port))
    my_receipt = client.message("/queue/Q", "Hello there", receipt=True)       # will generate and return receipt-id

    def wait_for_receipt(_, frame, receipt):
        # process message received by the client
        return frame is not None and frame.Command == "RECEIPT" and frame["receipt-id"] == receipt

    if client.loop(wait_for_receipt, receipt=my_receipt):
        # receipt received
    else:
        # connection closed
    
Transactions
~~~~~~~~~~~~

.. code-block:: python

    client = stompy.connect((host, port))
    transaction = client.transaction()
    trnsaction.message("/queue/Q", "Message part #1")
    trnsaction.message("/queue/Q", "Message part #2")
    receipt = transaction.commit(receipt=True)

    # wait for receipt
    if client.loop(wait_for_receipt, receipt=receipt):
        # receipt received
    else:
        # connection closed

STOMPClient object methods
--------------------------

.. autoclass:: stompy.STOMPClient
   :members:
   :special-members:
   :exclude-members: connect,add_callback,remove_callback,disconnect
          
   .. automethod:: connect(self, addr_list, login=None, passcode=None, headers={}, **kv_headers)
   .. automethod:: disconnect()

STOMPTransaction object
-----------------------

``STOMPClient.transaction()`` method returns STOMPTransaction object, which has the following methods:

.. autoclass:: stompy.client.STOMPTransaction
   :members:
   
STOMPFrame object
-----------------

STOMPFrame object represents a STOMP frame received from the Broker

.. autoclass:: stompy.frame.STOMPFrame
   :members:
   
   


