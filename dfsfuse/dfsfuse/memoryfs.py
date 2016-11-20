import os

class MemoryFS:
  def __init__(self):
    self.reset()

  def reset(self):
    self._meta = {}
    self._dirs = {}

  def hasdir(self, path):
    if path in self._meta:
      return True
    return False

  def readdir(self, path):
    if path not in self._meta:
      raise TypeError('Path not exist')
    return self._meta[path]

  def adddir(self, path, content):
    self._meta[path] = content
    if path not in self._dirs:
      self._dirs[path] = content['.']['id']
    for name, meta in content.items():
      if name == '.' or name == '..':
        continue

      if meta['type'] == 'dir':
        self._dirs[os.path.join(path, name)] = meta['id']

  def getid(self, path):
    return self._dirs[path]
