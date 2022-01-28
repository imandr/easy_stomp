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

connect function arguments
..........................

.. autofunction:: stompy.connect

Listening to messages
---------------------

.. code-block:: python

    import stompy
    
    port = 61613
    host = "host.domain.com"
    client = stompy.connect((host, port))
    
    client.subscribe("/queue/A")
    client.subscribe("/topic/T")
    for frame in client:
        command = frame.Command
        destination = frame.destination 
        headers = frame.headers()       # copy of the headers dictionary
        body = frame.Body               # body as bytes
        text = frame.text               # frame.Body decoded using utf-8 encoding
        source = frame["source"]        # headers can be accesses using mapping interface
        # ... process frame

Listening using loop
--------------------

.. code-block:: python

    client = stompy.connect((host, port))
    
    client.subscribe("/queue/A")
    client.subscribe("/topic/T")
    client.loop(lambda frame: )
    for frame in client:
        command = frame.Command
        destination = frame.destination 
        headers = frame.headers()       # copy of the headers dictionary
        body = frame.Body               # body as bytes
        text = frame.text               # frame.Body decoded using utf-8 encoding
        source = frame["source"]        # headers can be accesses using mapping interface
        # ... process frame

STOMPClient object methods
--------------------------

.. autoclass:: stompy.STOMPClient
   :members:
   :special-members:
   :exclude-members: connect,add_callback,remove_callback,disconnect
          
   .. automethod:: connect(self, addr_list, login=None, passcode=None, headers={}, **kv_headers)
   .. automethod:: add_callback(callback)
   .. automethod:: remove_callback(callback=None)
   .. automethod:: disconnect()

STOMPTransaction object
-----------------------

``STOMPClient.transaction()`` method returns STOMPTransaction object, which has the following methods:

.. autoclass:: stompy.client.STOMPTransaction
   :members:
