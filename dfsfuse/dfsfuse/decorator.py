import errno
from types import FunctionType
from functools import wraps
from fuse import FuseOSError
from .exception import TimeoutError

def _catch_exceptions(func):
  @wraps(func)
  def _wrapper(*args, **kargs):
    try:
      return func(*args, **kargs)
    except TimeoutError:
      raise FuseOSError(errno.EIO)
  return _wrapper

def catch_client_exceptions(klass):
  for attr_name in klass.__dict__:
    if attr_name.startswith('_'):
      continue
    attr = getattr(klass, attr_name)
    if isinstance(attr, FunctionType):
      setattr(klass, attr_name, _catch_exceptions(attr))
  return klass
