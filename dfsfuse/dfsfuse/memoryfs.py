import os
from threading import Lock
from logging import getLogger
from .utils.leftpad import leftpad

logger = getLogger('MemoryFS')

class MemoryFS:
  def __init__(self):
    self._meta_lock = Lock()
    self.reset()

  def reset(self):
    with self._meta_lock:
      logger.info('Enter lock')
      self._meta = {}
      self._paths = {}
      logger.info('Leave lock')

  def has(self, path):
    if path in self._meta:
      return True
    return False

  def tree(self, root = '/'):
    indent = 0
    if root != '/':
      indent = (len(root.split('/')) - 1) * 2
    for child in self._meta[root]['children']:
      print(leftpad(child, width = indent))
      if self.isdir(child):
        self.tree(child)

  def readdir(self, path):
    if path not in self._meta:
      raise TypeError('Path not exist')
    return list(self._meta[path]['children'])

  def isdir(self, path):
    return self.has(path) and self._meta[path]['type'] == 'dir'

  def isfile(self, path):
    return self.has(path) and self._meta[path]['type'] == 'file'

  def adddir(self, path, content):
    logger.info('adddir: path: %s, content: %s', path, content)
    if path == '/':
      assert content['.']['id'] == 1
    with self._meta_lock:
      logger.info('Enter lock')
      self._meta[path] = content['.']
      self._meta[path]['children'] = set()

      for name, meta in content.items():
        if name == '..' or name == '.':
          continue
        child_path = os.path.join(path, name)
        self._paths[child_path] = meta
        self._meta[path]['children'].add(name)
        self._meta[child_path] = meta
      logger.info('Leave lock')
    assert self._meta['/']['id'] == 1

  def loadfile(self, path, content):
    if not self.isfile(path):
      raise TypeError('Path is not file')
    with self._meta_lock:
      logger.info('Enter lock')
      self._meta[path]['content'] = content
      logger.info('Leave lock')

  def getid(self, path):
    return self._meta[path]['id']

  def getmeta(self, path):
    if not self.has(path):
      raise TypeError('Path not exist')
    return self._meta[path]

  def getcontent(self, path):
    if not self.isfile(path):
      raise TypeError('Path is not file')
    content = getattr(self._meta[path], 'content')
    if content is None:
      raise RuntimeError('Content not loaded')
    return content
