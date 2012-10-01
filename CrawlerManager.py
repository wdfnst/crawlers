#!/bin/python
# -*- coding: utf-8 -*-
from redisoperation import RedisOperation
from mysqloperation import DataSynch
from util import Util

import sys, os, re, operator, datetime, time, signal, threading, gc
import hashlib, urlparse
import urllib, cookielib
import urllib2
from urllib2 import Request
import encodings.idna

import lxml.html
#from guppy import hpy

from settings import Settings
import numpy as np
import psutil

from BlogCrawler import BlogCrawler
from ProductCrawler import ProductCrawler

reload(sys)
sys.setdefaultencoding('utf-8')

class Watcher:

	settings = Settings()
	Crawlers = settings.crawlername

	def __init__(self):
		# Dynamic import module and crawlerclass
		self.crawlerclass_list   = []
		self.crawlerprocess_list = [[]] * (len(self.Crawlers))
		for i in range(len(self.Crawlers)):
			dynamic_module = __import__(self.Crawlers[i])
			crawlerclass   = getattr(dynamic_module, self.Crawlers[i])
			self.crawlerclass_list.append(crawlerclass)

		# Create Crawler Process and Thread
		for m in range(len(self.Crawlers)):
			for n in range(self.settings.crawlersettings[m]['process_no']):
				child = os.fork()
				if child == 0:
					# Exec the job
					self.create_crawlers(self.Crawlers[m], self.settings.crawlersettings[m]['thread_no'])
					time.sleep(3)
				else:
					print "%s child process:%d"%(self.Crawlers[m], child)
					self.crawlerprocess_list[m].append(os.getpid())
					self.watch(child)

	def watch(self,pid):
		try:
			os.waitpid(pid, 0)
			#parent = os.getpid()
			#p_parent =  psutil.Process(parent)
			#p_child  =  psutil.Process(self.child)
			while True:
				#print p_parent.get_memory_info()
				#print p_child.get_memory_info()
				time.sleep(3)
		except KeyboardInterrupt:
			print 'KeyBoardInterrupt,begin to clear...'
			#time.sleep(5)
			self.kill()
		sys.exit()

	def kill(self):
		try:
			print 'clear finish and exit...'
			for p in range(len(self.Crawlers)):
				for q in range(self.settings.crawlersettings[p]['process_no']):
					os.kill(self.crawlerprocess_list[p][q], signal.SIGKILL)
		except OSError:
			pass

	def create_crawlers(self, crawlername, threadno = 12):
		#print "create_crawler"
		crawler_threads = []
		for i in range(threadno):
			crawler_threads.append(eval(crawlername)("T" + str(i), i))
		for i in range(threadno):
			crawler_threads[i].start()
		#for i in range(threadno):
			#crawler_threads[i].join()
		#for i in range(threadno):
			#crawler_threads[i].stop()

def main(script, flag='with'):
	if flag == 'with':
		Watcher()
	elif flag != 'without':
		print 'unrecognized flag: ' + flag
		sys.exit()

if __name__ == '__main__':
	main(*sys.argv)
