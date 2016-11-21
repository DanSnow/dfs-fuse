#!/usr/bin/env python3
# encoding: utf-8

import errno
from stat import S_IFDIR, S_IFREG
from dateutil import parser as dateparser
from logging import getLogger
from fuse import Operations, LoggingMixIn, FuseOSError

logger = getLogger('DFSFuse')

class DFSFuse(LoggingMixIn, Operations):
  def __init__(self, client, config):
    self._client = client
    self._config = config

  def access(self, path, mode):
    logger.info('access path: %s, mode: %s', path, mode)
    if self._client.has(path):
      return 0
    raise FuseOSError(errno.ENOENT)

  def chmod(self, path, mode):
    return 0 # Not support

  def chown(self, path, mode):
    return 0 # Not support

  def getattr(self, path, fh=None):
    logger.info('getattr: path: %s', path)
    if not self._client.has(path):
      logger.info('getattr: No such file %s', path)
      raise FuseOSError(errno.ENOENT)
    logger.info('getattr: Found %s', path)

    # Deal with root
    if path == '/':
      return {
        'st_mode': (S_IFDIR | 0o755),
        'st_nlink': 2
      }

    meta = self._client.stat(path)
    time = int(dateparser.parse(meta['ctime']).timestamp())

    mode = 0o750
    # Here must set file type
    if meta['type'] == 'dir':
      mode |= S_IFDIR
    else:
      mode |= S_IFREG

    return {
      'st_atime': time,
      'st_mtime': time,
      'st_ctime': time,
      'st_gid': self._config.gid,
      'st_uid': self._config.uid,
      'st_mode': mode,
      'st_nlink': 2
    }

  def getxattr(self, path, name, position = 0):
    return ''

  def listxattr(self, path):
    return []

  def readdir(self, path, fh):
    logger.info('readdir: path: %s', path)
    dirents = self._client.readdir(path).keys()
    logger.info('readdir: dirents: %s', dirents)
    return dirents

  def readlink(self, path):
    raise FuseOSError(errno.ENOSYS)

  def rmdir(self, path):
    raise NotImplementedError()
    full_path = self._full_path(path)
    return os.rmdir(full_path)

  def mkdir(self, path, mode):
    raise NotImplementedError()
    return os.mkdir(self._full_path(path), mode)

  def statfs(self, path):
    return {
      'f_bsize': 512,
      'f_blocks': 4096,
      'f_bavail': 2048
    }

  def unlink(self, path):
    raise NotImplementedError()
    return os.unlink(self._full_path(path))

  def symlink(self, name, target):
    raise FuseOSError(errno.EROFS)

  def rename(self, old, new):
    raise NotImplementedError()
    return os.rename(self._full_path(old), self._full_path(new))

  def link(self, target, name):
    raise FuseOSError(errno.EROFS)

  # File methods
  # ============

  def open(self, path, flags):
    raise NotImplementedError()
    full_path = self._full_path(path)
    return os.open(full_path, flags)

  def create(self, path, mode, fi=None):
    raise NotImplementedError()
    full_path = self._full_path(path)
    return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

  def read(self, path, length, offset, fh):
    raise NotImplementedError()
    os.lseek(fh, offset, os.SEEK_SET)
    return os.read(fh, length)

  def write(self, path, buf, offset, fh):
    raise NotImplementedError()
    os.lseek(fh, offset, os.SEEK_SET)
    return os.write(fh, buf)

  def truncate(self, path, length, fh=None):
    raise NotImplementedError()
    full_path = self._full_path(path)
    with open(full_path, 'r+') as f:
      f.truncate(length)

  def destroy(self, path):
    self._client.close()

