import errno
from types import FunctionType
from functools import wraps
from logging import getLogger
from fuse import FuseOSError
from .exception import TimeoutError, ServerError, InternalError, DisconnectError

logger = getLogger('decorator')

def retryable(func):
  @wraps(func)
  def _wrapper(*args, **kargs):
    retry = 1
    while retry < 3:
      try:
        return func(*args, **kargs)
      except DisconnectError:
        logger.error('Connection lost, retry %s', retry)
        args[0]._client.reconnect()
        retry += 1
    logger.error('Too many retries')
    raise FuseOSError(errno.EIO)
  return _wrapper

def nonretryable(func):
  @wraps(func)
  def _wrapper(*args, **kargs):
    try:
      return func(*args, **kargs)
    except DisconnectError:
      logger.error('Connection lost, not retryable, reconnecting...')
      args[0]._client.reconnect()
      raise FuseOSError(errno.EIO)
  return _wrapper

def _catch_exceptions(func):
  @wraps(func)
  def _wrapper(*args, **kargs):
    try:
      return func(*args, **kargs)
    except OSError as err:
      logger.exception(err)
      raise err
    except ServerError as err:
      logger.exception(err)
      raise FuseOSError(errno.EIO)
    except TimeoutError:
      logger.error('Timeout')
      raise FuseOSError(errno.EIO)
    except (TypeError, InternalError) as err:
      logger.exception(err)
      raise FuseOSError(errno.EFAULT)
  return _wrapper

def catch_client_exceptions(klass):
  for attr_name in klass.__dict__:
    if attr_name.startswith('_'):
      continue
    attr = getattr(klass, attr_name)
    if isinstance(attr, FunctionType):
      setattr(klass, attr_name, _catch_exceptions(attr))
  return klass
