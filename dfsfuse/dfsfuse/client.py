from logging import getLogger
import os
import hashlib
import socket
import json
from .packet import Packet
from .memoryfs import MemoryFS

logger = getLogger('Client')

class Client():
  def __init__(self, host = 'localhost', port = 4096, psk = ''):
    logger.info('Initialize')
    self._host = host
    self._port = port
    self._psk = hashlib.md5(psk.encode('utf-8')).hexdigest()
    self._fs = MemoryFS()
    self._connect()
    self._init()

  def login(self):
    logger.info('Send login')
    _, body = self.request('auth#login', header = { 'psk': self._psk  })
    if body != 'OK':
      logger.error('Login fail')
      raise RuntimeError('Login fail')
    logger.info('Login success')

  def ping(self):
    logger.info('Ping')
    _, body = self.request('echo#echo', body = b'ping')
    if body != 'ping':
      logger.error('Ping fail')
      raise RuntimeError('Ping: unexpected response')

  def write(self, path, content):
    parent_path = os.path.dirname(path)
    if not self._fs.isdir(parent_path):
      raise RuntimeError('Write: path is not dir')

    name = os.path.basename(path)
    id = self._fs.getid(parent_path)
    _, body = self.request('file#put', header = { 'id': id, 'name': name }, body = content)
    if body != 'OK':
      raise RuntimeError('Write fail')
    self.readdir(parent_path)
    return True

  def read(self, path):
    if not self._fs.isfile(path):
      return None
    id = self._fs.getid(path)
    header, body = self.request('file#get', header = { 'id': id })
    if header['result'] != 'OK':
      raise RuntimeError('Read fail')
    return body

  def rm(self, path):
    if not self._fs.isfile(path):
      return False
    parent_path = os.path.dirname(path)
    id = self._fs.getid(path)
    header, body = self.request('file#rm', header = { 'id': id })
    if body != 'OK':
      raise RuntimeError('Rm fail')
    self.readdir(parent_path)
    return True

  def readdir(self, path):
    id = self._fs.getid(path)
    data = self._readdir(id)
    self._fs.adddir(path, data)
    return data

  def _readdir(self, id = None):
    _, body = self.request('dir#list', header = { 'id': id })
    data = json.loads(body)
    return data

  def mkdir(self, path, name):
    parent_id = self._fs.getid(path)
    _, body = self.request('dir#add', header = { 'id': parent_id, 'name': name })
    if body != 'OK':
      raise RuntimeError('Mkdir fail')
    return self.readdir(path)

  def rmdir(self, path):
    id = self._fs.getid(path)
    _, body = self.request('dir#rm', header = { 'id': id })
    if body != 'OK':
      raise RuntimeError('Rmdir fail')
    return self.readdir(path)

  def request(self, request, body = b'', header = {}):
    controller, action = request.split('#')
    logger.info('Request: action: %s, header: %s, body: %s', action, header, body)
    _header = { 'controller': controller, 'action': action }
    _header.update(header)
    self._send(Packet(_header, body))
    pkt = self._read_response()
    if not pkt:
      raise RuntimeError('connection lost')
    return (pkt.headers, pkt.body)

  def send(self, packet):
    if type(packet) is not Packet:
      raise TypeError('Must be Packet')
    self._send(packet)

  def _init(self):
    self.login()
    self._fs.reset()
    self._init_root()

  def _init_root(self):
    data = self._readdir()
    self._fs.adddir('/', data)

  def _send(self, packet):
    data = packet.to_bytes()
    logger.info('Send: %s', data)
    self._socket.sendall(data)

  def reconnect(self):
    if self._socket:
      self.close()
    self._connect()
    self._init()

  def _connect(self):
    logger.info('Host: %s, Port: %s', self._host, self._port)
    self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self._socket.connect((self._host, self._port))

  def close(self):
    self._socket.close()
    self._socket = None

  def _read_response(self):
    logger.info('Read response')
    buf = self._socket.recv(4096)
    if len(buf) == 0:
      return None
    pkt = Packet.parse(None, buf)
    while True:
      if not pkt:
        break
      if not pkt.check():
        buf = self._socket.recv(4096)
        if len(buf) == 0:
          pkt = None
          break
        pkt = Packet.parse(pkt, buf)
      else:
        break
    return pkt
