import os
from .utils.leftpad import leftpad

class MemoryFS:
  def __init__(self):
    self.reset()

  def reset(self):
    self._meta = {}
    self._paths = {}

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
    return self._paths[path]

  def isdir(self, path):
    return self.has(path) and self._meta[path]['type'] == 'dir'

  def isfile(self, path):
    return self.has(path) and self._meta[path]['type'] == 'file'

  def adddir(self, path, content):
    self._paths[path] = content['.']
    self._meta[path] = content['.']
    self._meta[path]['children'] = set()

    for name, meta in content.items():
      if name == '..' or name == '.':
        continue
      child_path = os.path.join(path, name)
      self._paths[child_path] = meta
      self._meta[path]['children'].add(child_path)
      self._meta[child_path] = meta

  def loadfile(self, path, content):
    if not self.isfile(path):
      raise TypeError('Path is not file')
    self._meta[path]['content'] = content

  def getid(self, path):
    return self._paths[path]['id']

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
