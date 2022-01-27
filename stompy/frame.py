import uuid, time
from enum import Enum
from .util import to_str, to_bytes
from pythreader import Primitive, synchronized
from socket import timeout as socket_timeout

class AckMode(Enum):
    Auto = "auto"
    Client = "client"
    ClientInd = "client-individual"

class STOMPError(Exception):
    
    def __init__(self, message, frame=None):
        self.Message = message
        self.Frame = frame
        
    def __str__(self):
        out = f"STOMPError: {self.Message}"
        if self.Frame is not None:
            dump = self.Frame.to_bytes()
            if dump[-1:] == b"\x00":
                dump = dump[:-1]
            out += "\n- frame ---------------------\n" \
                + to_str(dump) \
                + "\n- end of frame --------------\n"
        return out

class STOMPTimeout(Exception):
    
    def __str__(self):
        return "STOMP timeout"

class FrameParser(object):
    
    def __init__(self):
        self.Body = b""
        self.Command = None
        self.Headers = {}
        self.HeadReceived = self.BodyReceived = False
        self.ContentLength = None
        self.RemainingBodyBytes = 0
        self.Frame = None       # parsed Frame
    
    def read_line(self, buf):
        if b"\n" in buf:
            line, rest = buf.split(b"\n", 1)
            return to_str(line).strip(), rest
        else:
            return None, buf
    
    def process(self, buf):
        while self.Command is None and buf:
            line, buf = self.read_line(buf)
            if line:        # A valid frame may be preceeded by a number of empty lines, sent as heart-beats
                self.Command = line
                
        while not self.HeadReceived and buf:
            line, buf = self.read_line(buf)
            if line is None:
                return buf
            if not line:        # end of headers
                length = self.Headers.get("content-length")
                if length is not None:
                    self.RemainingBodyBytes = int(length)
                self.HeadReceived = True
            else:
                name, value = line.split(":", 1)
                self.Headers[name] = value

        while buf and not self.BodyReceived:
            if self.RemainingBodyBytes > 0:
                body, buf = buf[:self.RemainingBodyBytes], buf[self.RemainingBodyBytes:]
                self.RemainingBodyBytes -= len(body)
            elif b"\x00" in buf:
                body, buf = buf.split(b"\x00", 1)
                self.BodyReceived = True
            else:
                body, buf = buf, b""
            self.Body = self.Body + body

        if self.BodyReceived:
            self.Frame = STOMPFrame(self.Command, self.Body, self.Headers)

        return buf 

class STOMPFrame(object):
    
    def __init__(self, command=None, body=b"", headers=None, **headers_kv):
        self.Command = command
        self.Body = body
        self.Headers = {}
        if body:
            self.Headers["content-length"] = len(body)
        self.Headers.update(headers or {})
        self.Headers.update(headers_kv)
        self.Buf = []
        self.Rest = []
        self.Received = False
        
    def __str__(self):
        return f"STOPMFrame(cmd={self.Command}, headers={self.Headers}, body={self.Body})"

    def to_bytes(self):
        parts = [self.Command]
        for h, v in self.Headers.items():
            parts.append(f"{h}:{v}")
        return to_bytes("\n".join(parts) + "\n\n") + to_bytes(self.Body) + b"\x00"

    @property
    def destination(self):
        return self.Headers["destination"]
        
    def headers(self):
        return self.Headers.copy()

    @property
    def text(self, encoding=None):
        if encoding is None:
            content_type = self.get("content-type")
            if content_type and "charset=" in content_type:
                words = content_type.split(';')
                for w in words:
                    w = w.strip()
                    if w.startswith("charset="):
                        encoding = w.split('=', 1)[1]
        encoding = encoding or "utf-8"
        return self.Body.decode(encoding)

    #
    # dict interface, headers access
    #
    def __getitem__(self, name):
        return self.Headers[name]
        
    def get(self, name, default=None):
        return self.Headers.get(name, default)
        
    def __contains__(self, name):
        return name in self.Headers
        
class STOMPStream(Primitive):
    
    def __init__(self, sock, read_size=4096):
        Primitive.__init__(self)
        self.Sock = sock
        self.Buf = b""
        self.ReadSize = read_size
        self.LastHearbeat = 0

    def send(self, frame):
        self.Sock.sendall(frame.to_bytes())

    @synchronized
    def recv(self, timeout=None):
        saved_timeout = self.Sock.gettimeout()
        try:
            if timeout is not None:
                self.Sock.settimeout(timeout)
            parser = FrameParser()
            frame = None
            eof = False
            while not eof and frame is None:
                buf = self.Buf
                if not buf: 
                    try:    
                        buf = self.Sock.recv(self.ReadSize)
                    except socket_timeout:
                        raise STOMPTimeout()
                    except:
                        buf = b""
                    self.LastHeartBeat = time.time()
                if not buf: 
                    eof = True     # eof
                else:
                    self.Buf = parser.process(buf)
                    frame = parser.Frame
            return frame
        finally:
            try:    self.Sock.settimeout(saved_timeout)
            except: pass        # probably already closed

    def __iter__(self):
        return self
        
    def __next__(self):
        frame = self.recv()
        if frame is None:
            raise StopIteration()
        else:
            return frame

    def close(self):
        try:    self.Sock.close()
        except: pass