import os

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
    self._meta[path].update({
      'children': set()
    })

    for name, meta in content.items():
      if name == '..' or name == '.':
        continue
      child_path = os.path.join(path, name)
      self._paths[child_path] = meta
      self._meta[path]['children'].add(child_path)

  def getid(self, path):
    return self._paths[path]['id']
