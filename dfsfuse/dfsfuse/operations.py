#!/usr/bin/env python3
# encoding: utf-8

import errno
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
    if not self._client.has(path):
      raise FuseOSError(errno.ENOENT)
    meta = self._client.stat(path)
    time = dateparser.parse(meta['ctime']).timestamp()
    return {
      'st_atime': time,
      'st_mtime': time,
      'st_ctime': time,
      'st_gid': self._config.gid,
      'st_uid': self._config.uid,
      'st_mode': 0o700,
      'st_nlink': 1,
      'st_size': 1
    }

  def getxattr(self, path, name, position = 0):
    return ''

  def listxattr(self, path):
    return []

  def readdir(self, path, fh):
    dirents = self._client.readdir(path)
    return dirents.keys()
    # full_path = self._full_path(path)
    #
    # dirents = ['.', '..']
    # if os.path.isdir(full_path):
    #   dirents.extend(os.listdir(full_path))
    #   for r in dirents:
    #     yield r

  def readlink(self, path):
    raise FuseOSError(errno.ENOSYS)

  def mknod(self, path, mode, dev):
    raise NotImplementedError()
    return os.mknod(self._full_path(path), mode, dev)

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
    raise FuseOSError(errno.ENOSYS)

  def rename(self, old, new):
    raise NotImplementedError()
    return os.rename(self._full_path(old), self._full_path(new))

  def link(self, target, name):
    raise FuseOSError(errno.ENOSYS)

  def utimens(self, path, times=None):
    raise NotImplementedError()
    return os.utime(self._full_path(path), times)

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

  def flush(self, path, fh):
    return 0

  def release(self, path, fh):
    return 0

  def fsync(self, path, fdatasync, fh):
    return 0

  def destroy(self, path):
    self._client.close()

