#!/usr/bin/env python3
# encoding: utf-8

import errno
import os
from stat import S_IFDIR, S_IFREG
from dateutil import parser as dateparser
from logging import getLogger
from fuse import Operations, LoggingMixIn, FuseOSError
from .fileoper import truncate
from .decorator import catch_client_exceptions, retryable, nonretryable
from io import BytesIO

logger = getLogger("DFSFuse")


@catch_client_exceptions
class DFSFuse(LoggingMixIn, Operations):
    def __init__(self, client, config):
        self._client = client
        self._config = config
        self._fhs = []

    @retryable
    def access(self, path, mode):
        logger.info("access path: %s, mode: %s", path, mode)
        if self._client.has(path):
            return 0
        raise FuseOSError(errno.ENOENT)

    def chmod(self, path, mode):
        return 0  # Not support

    def chown(self, path, mode):
        return 0  # Not support

    @retryable
    def getattr(self, path, fh=None):
        logger.info("getattr: path: %s", path)
        if not self._client.has(path):
            logger.info("getattr: No such file %s", path)
            raise FuseOSError(errno.ENOENT)
        logger.info("getattr: Found %s", path)

        # Deal with root
        if path == "/":
            return {"st_mode": (S_IFDIR | 0o755), "st_nlink": 2}

        meta = self._client.stat(path)
        logger.info("getattr: meta: %s", meta)
        time = int(dateparser.parse(meta["ctime"]).timestamp())

        mode = 0o750
        # Here must set file type
        if meta["type"] == "dir":
            mode |= S_IFDIR
        else:
            mode |= S_IFREG

        size = meta.get("size", 1)

        return {
            "st_atime": time,
            "st_mtime": time,
            "st_ctime": time,
            "st_gid": self._config.gid,
            "st_uid": self._config.uid,
            "st_mode": mode,
            "st_nlink": 2,
            "st_size": size,
        }

    def listxattr(self, path):
        return []

    @retryable
    def readdir(self, path, fh):
        logger.info("readdir: path: %s", path)
        dirents = self._client.readdir(path)
        logger.info("readdir: dirents: %s", dirents)
        return dirents

    def readlink(self, path):
        raise FuseOSError(errno.ENOENT)

    @retryable
    def rmdir(self, path):
        self._client.rmdir(path)
        return 0

    @retryable
    def mkdir(self, path, mode):
        if self._client.has(path):
            raise FuseOSError(errno.EEXIST)
        return self._mkdir(path)

    def _mkdir(self, path):
        (head, tail) = os.path.split(path)
        self._client.mkdir(head, tail)
        return 0

    def statfs(self, path):
        return {"f_bsize": 512, "f_blocks": 4096, "f_bavail": 2048}

    @retryable
    def unlink(self, path):
        if not self._client.rm(path):
            raise FuseOSError(errno.ENOENT)
        return 0

    def symlink(self, name, target):
        raise FuseOSError(errno.EROFS)

    @retryable
    def rename(self, old, new):
        if not self._client.has(old):
            raise FuseOSError(errno.ENOENT)
        if self._client.has(new):
            raise FuseOSError(errno.EEXIST)
        self._client.mv(old, new)
        return 0

    def link(self, target, name):
        raise FuseOSError(errno.EROFS)

    # File methods
    # ============

    @nonretryable
    def open(self, path, flags):
        if flags & os.O_RDONLY:
            if self.has(path):
                raise FuseOSError(errno.ENOENT)
        elif flags & os.O_WRONLY:
            if flags & os.O_CREAT and flags & os.O_EXCL and self.has(path):
                raise FuseOSError(errno.ENOENT)
            if not (flags & os.O_APPEND):
                self._client.write(path, "")
        length = len(self._fhs)
        self._fhs.append({"io": BytesIO(self._client.read(path)), "dirty": False})
        logger.debug("open file return: %s", length)
        return length

    def create(self, path, mode, fi=None):
        self._client.write(path, "")
        length = len(self._fhs)
        self._fhs.append({"io": BytesIO(), "dirty": False})
        return length

    @retryable
    def read(self, path, length, offset, fh):
        self._fhs[fh]["io"].seek(offset)
        return self._fhs[fh]["io"].read(length)

    @retryable
    def write(self, path, buf, offset, fh):
        self._fhs[fh]["dirty"] = True
        self._fhs[fh]["io"].seek(offset)
        return self._fhs[fh]["io"].write(buf)

    @nonretryable
    def release(self, path, fh):
        if self._fhs[fh]["dirty"]:
            self._fhs[fh]["io"].seek(0)
            self._client.write(path, self._fhs[fh]["io"].read())
        self._fhs[fh]["io"].close()
        return 0

    @retryable
    def truncate(self, path, length, fh=None):
        new_content = truncate(self._client.read(path), length)
        self._client.write(path, new_content)
        return 0

    def destroy(self, path):
        self._client.close()
