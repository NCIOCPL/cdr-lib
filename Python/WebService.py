"""
Simple Web service helper classes.
"""

import os
import sys
from lxml import etree
from cdrapi import db


class WrongMethod(Exception):
    """
    Custom exception class

    Allows handlers catch problems with HTTP method specification.
    """


class Request:
    """
    Object representing a client request, extracted from the XML
    document used to transmit the request.

    Attributes:
      message  - raw XML for the request, UTF-8 encoded
      doc      - the node for top-level element of the document
      type     - string for the name of the command
      logLevel - value controlling how much logging is performed
                 by the server; set by the client using the HTTP
                 headers.

    2015-12-08: add option to use lxml.etree instead of minidom
    """

    def __init__(self, standalone=False, logger=None):
        """
        Set the request object's attributes

        Optional keyword arguments:
          standalone  - set to True if debugging using redirected message
          logger      - optional object for logging the service's activity
        """

        self.message = None
        self.doc = None
        self.type = None
        self.logLevel = 0
        self.logger = logger
        if standalone:
            self.message = sys.stdin.read()
            self.client = 'Standalone'
            debugLevel = "0"
        else:
            defaultLevel = self.defaultLevel()
            requestMethod = os.getenv("REQUEST_METHOD")
            self.client = os.getenv("REMOTE_ADDR")
            remoteHost = os.getenv("REMOTE_HOST")
            debugLevel = os.getenv("HTTP_X_DEBUG_LEVEL") or defaultLevel
            if debugLevel > "1" and logger is not None:
                logger.setLevel("DEBUG")
                logger.info("debugging level set to %s", debugLevel)
                if debugLevel > "2":
                    self.dumpenv()
            if remoteHost and remoteHost != self.client:
                self.client += f" ({remoteHost})"
            if not requestMethod:
                raise WrongMethod("Request method not specified")
            if requestMethod == "OPTIONS":
                print("""\
Content-Type: text/plain
Content-Length: 0
Access-Control-Allow-Headers: Content-Type,SOAPAction
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: POST, GET, OPTIONS
""")
                sys.exit(0)

            if requestMethod != "POST":
                message = f"Request method should be POST; was {requestMethod}"
                raise WrongMethod(message)
            lenString = os.getenv("CONTENT_LENGTH")
            if not lenString:
                raise Exception("Content length not specified")
            try:
                contentLength = int(lenString)
            except Exception:
                raise Exception(f"Invalid content length: {lenString}")
            if contentLength < 1:
                raise Exception(f"Invalid content length: {lenString}")
            try:
                blocks = []
                nRead = 0
                while nRead < contentLength:
                    block = sys.stdin.buffer.read(contentLength - nRead)
                    nRead += len(block)
                    blocks.append(block)
            except Exception as e:
                raise Exception(f"Failure reading request: {e}")
            self.message = b"".join(blocks)
            try:
                self.message_text = str(self.message, "utf-8")
            except Exception:
                self.message_text = None
            if logger and self.message_text:
                logger.debug("WebService message: %s", self.message_text)
        try:
            self.doc = etree.fromstring(self.message)
            self.type = self.doc.tag
        except Exception as e:
            if logger:
                logger.exception("Failure parsing request")
            raise Exception(f"Failure parsing request: {e}")
        try:
            self.logLevel = int(debugLevel)
        except Exception:
            self.logLevel = 1

    def defaultLevel(self):
        query = db.Query("ctl", "val")
        query.where("grp = 'WebService'")
        query.where("name = 'DebugLevel'")
        query.where("inactivated IS NULL")
        row = query.execute().fetchone()
        return row.val if row else "1"

    def dumpenv(self):
        lines = ["WebService environment", "=" * 60]
        for e in sorted(os.environ):
            lines.append(f"{e}={os.environ[e]}")
        self.logger.debug("\n".join(lines))


class Response:
    """
    Object for the server's response to the client's request.

    Contains a send() method for returning the request.
    """

    def __init__(self, body, logger=None):
        """
        Capture the response object's attributes

        Ensure that the body attribute is UTF-8 encoded.
        Pass:
          body - xml tree or its serialization
          logger - optional object for logging what we do
        """
        self.logger = logger
        if isinstance(body, bytes):
            self.body = str(body, "utf-8")
        elif isinstance(body, str):
            self.body = body
        else:
            opts = dict(pretty_print=True, encoding="unicode")
            self.body = etree.tostring(body, **opts)

    def send(self, contentType="text/xml"):
        body = self.body
        if isinstance(body, str):
            body = body.encode("utf-8")
        headers = (
            f"Content-Type: {contentType}; charset=utf-8",
            f"Content-Length: {len(body):d}",
        )
        for header in headers:
            sys.stdout.buffer.write(header.encode("utf-8"))
            sys.stdout.buffer.write(b"\n")
        sys.stdout.buffer.write(b"\n")
        sys.stdout.buffer.write(body)
        if self.logger:
            self.logger.debug("sending %s", self.body)
        sys.exit(0)


class ErrorResponse(Response):
    """Message sent back to the client if a failure is detected"""

    def __init__(self, error, logger=None):
        """
        Capture the object's attributes

        Make sure the body is utf-8 encoded.
        """

        self.logger = logger
        if isinstance(error, bytes):
            error = error.decode("utf-8")
        self.body = f"<ERROR>{error}</ERROR>"
