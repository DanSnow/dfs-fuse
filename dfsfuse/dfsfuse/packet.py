#!/usr/bin/env python
#-*- coding: utf-8 -*-

from io import StringIO
from logging import getLogger
import select

logger = getLogger('Packet')

class Packet:
  def __init__(self, header = {}, body = ""):
    self.header = {}
    self.header.update(header)
    self.set(body)

  def set(self, header, value = None):
    if value is None:
      self._body = header
      logger.info('Set body: %s', header)
      self.set('content-length', len(header))
    else:
      logger.info('Set header: %s -> %s', header, value)
      self.header[header] = value

  def get(self, header = None):
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
    logger.info('To bytes')
    logger.info('Header: %s', self.header)
    logger.info('Body: %s', self._body)
    header = { k:v for k, v in self.header.items() if v is not None }
    s = b''
    for x in header:
      s += x.encode('utf-8') + b': ' + str(self.header[x]).encode('utf-8') + b'\n'
    s += b'\n'
    body = self._body
    if type(body) is not bytes:
      body = bytearray(body, 'utf-8')
    s += body
    return s

  def check(self):
    l = self.get('content-length')
    if l == None:
      logger.error('Check fail: format error')
      return None
    if l == len(self.get()):
      return True
    return False

  @staticmethod
  def parse(pkt, x):
    if pkt == None:
      pkt = Packet()
      buf = StringIO(x.decode('utf-8'))
      while True:
        tmp = str(buf.readline().rstrip())
        if tmp == '':
          break
        tmp = tmp.split(':', 1)
        if len(tmp) == 2:
          pkt.set(tmp[0], tmp[1].strip())
    pkt.set(pkt.get() + buf.read().rstrip())
    return pkt
