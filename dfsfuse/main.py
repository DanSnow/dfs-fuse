from logging import getLogger
from fuse import FUSE
from dfsfuse import DFSFuse, Client
from sys import argv

def run_fuse():
  logger = getLogger('FUSE')
  logger.info('Run fuse')
  client = Client(host='140.123.241.61')

  fuse = FUSE(DFSFuse(client), argv[1], foreground=True)

def main(argv):
  logger = getLogger('main')
  run_fuse()

