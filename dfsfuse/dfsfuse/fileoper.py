from io import StringIO

def read(content, offset, length):
  return content[offset : offset + length]

def write(content, data, offset):
  buf = StringIO(content)
  buf.seek(offset)
  buf.write(data)
  return buf.getvalue()

def truncate(content, length):
  return content[0 : length]
