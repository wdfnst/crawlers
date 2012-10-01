#!/bin/python
# -*- coding: utf-8 -*-
from util import Util

import sys, os, re, operator, datetime, time, signal, threading, gc
import hashlib, urlparse
import urllib, cookielib
import urllib2
from urllib2 import Request

from settings import Settings

#import numpy as np
import psutil

reload(sys)
sys.setdefaultencoding('utf-8')

class Watcher:
	def __init__(self):
		self.child = os.fork()
		if self.child == 0:
			return
		else:
			self.watch()

	def watch(self):
		try:
			os.wait()
		except KeyboardInterrupt:
			print '\n'
			print 'KeyBoardInterrupt,begin to clear...'
			#time.sleep(5)
			print '\n'
			self.kill()
		sys.exit()

	def kill(self):
		try:
			print 'clear finish and exit...'
			os.kill(self.child, signal.SIGKILL)
		except OSError: pass
