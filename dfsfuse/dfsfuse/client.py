from logging import getLogger
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

  def readdir(self, path, force = False):
    if not force and self._fs.hasdir(path):
      return self._fs.readdir(path)

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
    return self.readdir(path, force = True)

  def rmdir(self, path):
    id = self._fs.getid(path)
    _, body = self.request('dir#rm', header = { 'id': id })
    if body != 'OK':
      raise RuntimeError('Rmdir fail')
    return self.readdir(path, force = True)

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
