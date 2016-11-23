class DFSError(RuntimeError):
  pass

class TimeoutError(DFSError):
  pass

class ServerError(DFSError):
  pass

class InternalError(DFSError):
  pass

class DisconnectError(DFSError):
  pass

class AuthError(DFSError):
  pass

