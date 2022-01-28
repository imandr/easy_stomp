from stompy import connect
from pythreader import PyThread
import random

class Sender(PyThread):
    
    def __init__(self, client, my_id=None):
        PyThread.__init__(self, name="Sender")
        self.ID = my_id or ("%x" % id(self))[-4:]
        self.Client = client
        self.Stop = False
        self.MessageID = 1
        
    def run(self):
        while not self.Stop:
            t = random.random()*5.0     # Average sleep time = 2.5 seconds
            self.sleep(t)               # sleep will be interrupted is someone calls self.stop()
            if not self.Stop:
                message = f"message {self.ID}:{self.MessageID}"
                self.MessageID += 1
                promise = self.Client.message("chat", message, receipt=True, headers = {"from":self.ID})
                print(f"[{self.ID}] >>", message)
                promise.wait()
    
    def stop(self):
        self.Stop = True
        self.wakeup()                   # this will interrupt self.sleep(t) in run()


class Listener(PyThread):
    
    def __init__(self, client, my_id=None):
        PyThread.__init__(self, name="Listener")
        self.ID = my_id or ("%x" % id(self))[-4:]
        self.Client = client
        self.Stop = False
    
    def on_message(self, client, frame):
        src = frame["from"]
        text = frame.text
        print(f"<< [{src}] {text}")

    def on_error(self, client, frame):
        text = frame.text
        message = frame.get("message", "")
        print("error: {message}\n"+message)

    def run(self):
        self.Client.subscribe("chat")
        for frame in self.Client:
            if frame.Command == "MESSAGE":
                self.on_message(self.Client, frame)
            elif frame.Command == "ERROR":
                self.on_error(self.Client, frame)
            else:
                pass #ignore
            if self.Stop:
                self.Client.disconnect()
                break
    
    def stop(self):
        self.Client.disconnect()
        self.Stop = True
        
def main():
    import sys, getopt, os
    
    opts, args = getopt.getopt(sys.argv[1:], "qi:")
    opts = dict(opts)

    broker_host, broker_port = args
    broker_port = int(broker_port)

    my_id = opts.get("-i", str(os.getpid()))
    quiet = "-q" in opts
    client = connect((broker_host, broker_port))
    listener = Listener(client, my_id)
    listener.start()
    if not quiet:
        sender = Sender(client, my_id)
        sender.start()
        sender.join()
    listener.join()

if __name__ == "__main__":
    main()
