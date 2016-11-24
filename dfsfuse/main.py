import os
import argparse
from logging import getLogger, WARNING
from fuse import FUSE
from dfsfuse import DFSFuse, Client

def run_fuse(args):
  logger = getLogger('FUSE')
  logger.info('Run fuse')
  if not args.debug:
    getLogger('Packet').setLevel(WARNING)
  client = Client(host = args.host, port = args.port, psk = args.key, cache = args.nocache)

  fuse = FUSE(DFSFuse(client, args), args.mount, foreground=True)

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

  parser.add_argument(
    '-u',
    '--uid',
    help = 'Uid',
    default = os.getuid(),
    type = int
  )

  parser.add_argument(
    '-g',
    '--gid',
    help = 'Gid',
    default = os.getgid(),
    type = int
  )

  parser.add_argument(
    '-d',
    '--debug',
    help = 'Debug message',
    action = 'store_true',
    default = False
  )

  parser.add_argument(
    '--nocache',
    help = 'Disable cache',
    action = 'store_false',
    default = True
  )

  args = parser.parse_args()
  run_fuse(args)

