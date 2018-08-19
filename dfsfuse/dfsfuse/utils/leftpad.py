def leftpad(msg, width=0, fill=" "):
    return "{msg:{fill}>{width}}".format(msg=msg, fill=fill, width=width)
