from io import StringIO

def read(content, offset, length):
  return content[offset : offset + length]

def write(content, data, offset):
  # FIXME: Maybe should use bytes here
  buf = StringIO(content)
  buf.seek(offset)
  buf.write(data.decode('utf-8'))
  return buf.getvalue()

def truncate(content, length):
  return content[0 : length]
