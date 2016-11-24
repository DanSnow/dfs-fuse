from logging import getLogger
import sys
import os
import collections
import hashlib
import socket
import json
from functools import wraps
from hirlite import Rlite
from .packet import Packet
from .memoryfs import MemoryFS
from .exception import TimeoutError, ServerError, InternalError, DisconnectError, AuthError


logger = getLogger('Client')
socket.setdefaulttimeout(5)

def inject_socket(func):
  @wraps(func)
  def _wrapper(*args, **kargs):
    self = args[0]
    sock = self._connect()
    res = func(self, sock, *args[1:])
    sock.close()
    return res
  return _wrapper

class Client():
  def __init__(self, host = 'localhost', port = 4096, psk = '', cache = True):
    logger.info('Initialize')
    self._host = host
    self._port = port
    self._psk = hashlib.md5(psk.encode('utf-8')).hexdigest()
    self._rlite = Rlite()
    self._cache = cache
    self._fs = MemoryFS()
    self._connect()
    self._init()


  @inject_socket
  def login(self, sock):
    logger.info('Send login')
    _, body = self.request(sock, 'auth#login', header = { 'psk': self._psk  })
    if body != b'OK':
      logger.error('Login fail')
      raise AuthError('Login fail')
    logger.info('Login success')

  @inject_socket
  def ping(self, sock):
    logger.info('Ping')
    _, body = self.request(sock, 'echo#echo', body = b'ping')
    if body != 'ping':
      logger.error('Ping fail')
      raise ServerError('Ping: unexpected response')

  def stat(self, path):
    if self._fs.has(path):
      return self._fs.getmeta(path)
    if self.has(path):
      return self._fs.getmeta(path)
    raise TypeError('Path not exist')

  @inject_socket
  def has(self, sock, path):
    deque = collections.deque()
    cur_path = path

    while True:
      (head, tail) = os.path.split(cur_path)
      if tail:
        deque.appendleft(tail)
      if head == '/':
        deque.appendleft(head)
        break
      cur_path = head

    cur_path = deque.popleft()
    while len(deque) > 0:
      parent_path = cur_path
      logger.info('has: Recursive find file: cur_path: %s', cur_path)
      cur_path = os.path.join(cur_path, deque.popleft())
      if not self._fs.has(cur_path):
        if self._fs.isdir(parent_path):
          self.readdir(parent_path)
        else:
          break
    return self._fs.has(path)

  @inject_socket
  def write(self, sock, path, content):
    parent_path = os.path.dirname(path)
    if not self._fs.isdir(parent_path):
      raise RuntimeError('Write: path is not dir')

    name = os.path.basename(path)
    id = self._fs.getid(parent_path)
    logger.info('Write to %s, content len %s', path, len(content))
    _, body = self.request(sock, 'file#put', header = { 'id': id, 'name': name }, body = content)
    if body != b'OK':
      raise ServerError('Write fail')
    self._readdir(sock, parent_path)
    self._fs.loadfile(path, content)
    return True

  @inject_socket
  def read(self, sock, path):
    if not self._fs.isfile(path):
      return None
    id = self._fs.getid(path)
    header, body = self.request(sock, 'file#get', header = { 'id': id })
    if header['result'] != 'OK':
      raise ServerError('Read fail')
    self._fs.loadfile(path, body)
    return body

  @inject_socket
  def rm(self, sock, path):
    if not self._fs.isfile(path):
      return False
    parent_path = os.path.dirname(path)
    id = self._fs.getid(path)
    header, body = self.request(sock, 'file#rm', header = { 'id': id })
    if body != b'OK':
      raise ServerError('Rm fail')
    self._readdir(sock, parent_path)
    return True

  @inject_socket
  def mv(self, sock, old, new):
    if not self._fs.has(old):
      raise RuntimeError('{0} not exist'.format(old))
    id = self._fs.getid(old)
    meta = self._fs.getmeta(old)
    (head, tail) = os.path.split(new)
    if not self._fs.has(head):
      raise RuntimeError('target {0} not exist'.format(head))
    parent_id = self._fs.getid(head)
    if meta['type'] == 'file':
      return self._mvfile(sock, id, parent_id, tail)
    else:
      return self._mvdir(sock, id, parent_id, tail)

  def _mvfile(self, sock, id, parent_id, name):
    _, body = self.request(sock, 'file#mvfile', header = {
      'id': id,
      'pdid': parent_id,
      'name': name
    })
    if body != b'OK':
      raise ServerError('mvfile fail')
    return True

  def _mvdir(self, sock, id, parent_id, name):
    _, body = self.request(sock, 'dir#mvdir', header = {
      'id': id,
      'pdid': parent_id,
      'name': name
    })
    if body != b'OK':
      raise ServerError('mvdir fail')
    return True

  @inject_socket
  def readdir(self, sock, path):
    cache_key = '{0}:cache'.format(path)
    if self._cache and self._rlite.exists(cache_key):
      dirents = self._fs.readdir(sock, path)
      logger.info('cached dirents: %s', dirents)
      return dirents
    return self._readdir(sock, path)

  def _readdir(self, sock, path):
    logger.info('_readdir path: %s', path)
    cache_key = '{0}:cache'.format(path)
    self._rlite.expire(cache_key, '0')
    id = self._fs.getid(path)
    logger.info('_readdir id: %s', id)
    data = self._readdir_with_id(sock, id)
    self._fs.adddir(path, data)
    return data.keys()

  def _readdir_with_id(self, sock, id = None):
    _, body = self.request(sock, 'dir#list', header = { 'id': id })
    data = json.loads(body.decode('utf-8'))
    return data

  @inject_socket
  def mkdir(self, sock, path, name):
    parent_id = self._fs.getid(path)
    _, body = self.request(sock, 'dir#add', header = { 'id': parent_id, 'name': name })
    if body != b'OK':
      raise ServerError('Mkdir fail')
    return self._readdir(sock, path)

  @inject_socket
  def rmdir(self, path):
    id = self._fs.getid(path)
    _, body = self.request(sock, 'dir#rm', header = { 'id': id })
    if body != b'OK':
      raise ServerError('Rmdir fail')
    self._readdir(sock, path)
    return True

  def request(self, sock, request, body = b'', header = {}):
    controller, action = request.split('#')
    logger.info('Request: action: %s, header: %s', action, header)
    _header = { 'controller': controller, 'action': action }
    _header.update(header)
    self._send(sock, Packet(_header, body))
    pkt = self._read_response(sock)
    if not pkt:
      raise DisconnectError('connection lost')
    return (pkt.headers, pkt.body)

  @inject_socket
  def send(self, sock, packet):
    if type(packet) is not Packet:
      raise TypeError('Must be Packet')
    self._send(sock, packet)

  def _init(self):
    self._fs.reset()
    self._rlite.flushall()
    self._init_root()

  @inject_socket
  def _init_root(self, sock):
    data = self._readdir_with_id(sock, 1)
    assert data['.']['id'] == 1
    self._fs.adddir('/', data)

  def _send(self, sock, packet):
    data = packet.to_bytes()
    logger.info('_send: Send: %s', packet.headers)
    sock.sendall(data)

  def reconnect(self):
    self._init()

  def _connect(self):
    logger.info('Create connection')
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      sock.connect((self._host, self._port))
    except ConnectionError:
      logger.error('Connection fail')
      sys.exit('Connection fail')
    return sock

  def close(self):
    pass

  def _read_response(self, sock):
    logger.info('Read response')
    try:
      buf = sock.recv(4096)
    except socket.timeout:
      raise TimeoutError()
    if len(buf) == 0:
      return None
    pkt = Packet.parse(None, buf)
    while True:
      if not pkt:
        break
      if not pkt.check():
        try:
          buf = sock.recv(4096)
        except socket.timeout:
          raise TimeoutError()
        if len(buf) == 0:
          pkt = None
          break
        pkt = Packet.parse(pkt, buf)
      else:
        break
    return pkt
