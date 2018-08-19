#!/usr/bin/env python3
# encoding: utf-8

import logging
from logging import DEBUG, getLogger

FORMAT = "%(levelname)s %(asctime)-15s %(name)s: %(message)s"
logging.basicConfig(format=FORMAT, level=DEBUG)

logger = getLogger("bootstrap")
logger.info("start")
