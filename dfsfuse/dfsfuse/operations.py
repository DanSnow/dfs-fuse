#!/usr/bin/env python3
# encoding: utf-8

from logging import getLogger
from fuse import Operations, LoggingMixIn

logger = getLogger('DFSFuse')

class DFSFuse(LoggingMixIn, Operations):
  def __init__(self, client):
    self._client = client

  def access(self, path, mode):
    logger.info('access path: %s, mode: %s', path, mode)
    return 0
    raise NotImplementedError()

  def chmod(self, path, mode):
    return 0 # Not support

  def chown(self, path, mode):
    return 0 # Not support

  def getattr(self, path, fh=None):
    raise NotImplementedError()
    full_path = self._full_path(path)
    st = os.lstat(full_path)
    return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                    'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

  def readdir(self, path, fh):
    raise NotImplementedError()
    # full_path = self._full_path(path)
    #
    # dirents = ['.', '..']
    # if os.path.isdir(full_path):
    #   dirents.extend(os.listdir(full_path))
    #   for r in dirents:
    #     yield r

  def readlink(self, path):
    raise NotImplementedError()
    pathname = os.readlink(self._full_path(path))
    if pathname.startswith("/"):
      # Path name is absolute, sanitize it.
      return os.path.relpath(pathname, self.root)
    else:
      return pathname

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
    raise NotImplementedError()
    full_path = self._full_path(path)
    stv = os.statvfs(full_path)
    return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                                                     'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
                                                     'f_frsize', 'f_namemax'))

  def unlink(self, path):
    raise NotImplementedError()
    return os.unlink(self._full_path(path))

  def symlink(self, name, target):
    raise NotImplementedError()
    return os.symlink(name, self._full_path(target))

  def rename(self, old, new):
    raise NotImplementedError()
    return os.rename(self._full_path(old), self._full_path(new))

  def link(self, target, name):
    raise NotImplementedError()
    return os.link(self._full_path(target), self._full_path(name))

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
    raise NotImplementedError()
    return os.fsync(fh)

  def release(self, path, fh):
    raise NotImplementedError()
    return os.close(fh)

  def fsync(self, path, fdatasync, fh):
    raise NotImplementedError()
    return self.flush(path, fh)

  def destroy(self, path):
    self._client.close()

