#!/usr/bin/env python
# -*- coding: utf-8 -*-

from io import BytesIO
from logging import getLogger

logger = getLogger("Packet")


class Packet:
    def __init__(self, header={}, body=b""):
        self.header = {}
        self.header.update(header)
        self.set(body)

    def set(self, header, value=None):
        if value is None:
            logger.info("Set body: %s", header)
            self.set("content-length", len(header))
            self._body = header
        else:
            logger.info("Set header: %s -> %s", header, value)
            self.header[header] = value

    def get(self, header=None):
        if header is None:
            return self._body
        elif header in self.header:
            return self.header[header]
        else:
            return None

    @property
    def headers(self):
        return self.header

    @property
    def body(self):
        return self._body

    def to_bytes(self):
        logger.info("To bytes")
        logger.info("Header: %s", self.header)
        logger.info("Body: %s", self._body)
        header = {k: v for k, v in self.header.items() if v is not None}
        s = b""
        for x in header:
            s += x.encode("utf-8") + b": " + str(self.header[x]).encode("utf-8") + b"\n"
        s += b"\n"
        body = self._body
        if type(body) is not bytes:
            body = bytearray(body, "utf-8")
        s += body
        return s

    def check(self):
        length = int(self.get("content-length"))
        if length is None:
            logger.error("Check fail: format error")
            return None
        if length == len(self.get()):
            return True
        return False

    def _set_body(self, buf):
        length = int(self.get("content-length"))
        self._body = self._body + buf
        content = self._body
        if len(content) > length:
            self._body = content[0:length]

    @staticmethod
    def parse(pkt, x):
        buf = BytesIO(x)

        logger.info("Start parse")
        if pkt is None:
            pkt = Packet()
            while True:
                tmp = buf.readline().decode("utf-8").rstrip()
                if tmp == "":
                    break
                tmp = tmp.split(":", 1)
                logger.info("Parse header: %s: %s", tmp[0], tmp[1])
                if len(tmp) == 2:
                    pkt.set(tmp[0], tmp[1].strip())

        pkt._set_body(buf.read())

        return pkt
