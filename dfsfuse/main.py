import argparse
from logging import getLogger
from fuse import FUSE
from dfsfuse import DFSFuse, Client

def run_fuse(args):
  logger = getLogger('FUSE')
  logger.info('Run fuse')
  client = Client(host = args.host, port = args.port, psk = args.key)

  fuse = FUSE(DFSFuse(client), args.mount, foreground=True)

def main(argv):
  logger = getLogger('main')
  parser = argparse.ArgumentParser()
  parser.add_argument(
    'mount',
    help = 'Mount point',
    type = str
  )
  parser.add_argument(
    '-k',
    '--key',
    help = 'PSK key',
    default = '',
    type = str
  )
  parser.add_argument(
    '--host',
    help = 'Host',
    default = 'localhost',
    type = str
  )
  parser.add_argument(
    '-p',
    '--port',
    help = 'Port',
    default = 4096,
    type = int
  )
  args = parser.parse_args()
  run_fuse(args)

