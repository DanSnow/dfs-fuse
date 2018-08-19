from io import BytesIO


def read(content, offset, length):
    buf = BytesIO(content)
    buf.seek(offset)
    return buf.read(length)


def write(content, data, offset):
    buf = BytesIO(content)
    buf.seek(offset)
    buf.write(data)
    return buf.getvalue()


def truncate(content, length):
    return content[0:length]
