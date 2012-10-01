BlogCrawler.py                                                                                      0000664 0000771 0000772 00000023046 12032212077 013515  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                #!/bin/python
# -*- coding: utf-8 -*-
from redisoperation import RedisOperation
from mysqloperation import DataSynch
from util import Util
from blogsettings import Settings

import sys, os, re, operator, datetime, time, signal, threading, gc, socket
import hashlib, urlparse
import urllib
#import cookielib
import urllib2
from urllib2 import Request
#import encodings.idna
import lxml.html
import psutil
from DBUtils.PooledDB import PooledDB
import MySQLdb as mdb
import redis

# Global settings
reload(sys)
sys.setdefaultencoding('utf-8')
# timeout in seconds
timeout = 180
socket.setdefaulttimeout(timeout)

# Get mysqldb/redis host info
settings					= Settings()
delete_level				= settings.blogcrawlersettings['init_level']
mysql_host					= settings.mysqlsettings['host']
mysql_port					= settings.mysqlsettings['port']
mysql_user					= settings.mysqlsettings['user']
mysql_passwd				= settings.mysqlsettings['passwd']
mysql_db					= settings.mysqlsettings['db']
mysql_charset				= settings.mysqlsettings['charset']
redis_host				    = settings.redissettings['host']
redis_port				    = settings.redissettings['port']
redis_db				    = settings.redissettings['db']
# Init the connections to mysqldb/redis
pool = PooledDB( creator = mdb, mincached = 5, db = mysql_db, host = mysql_host, user = mysql_user, passwd= mysql_passwd, charset = "utf8", use_unicode = True)
redis_pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=int(redis_db))
# Datastures stored in redis
blog_page_url_set            = settings.blogcrawlersettings['page_url_set']
blog_nothrow_urljson_list	 = settings.blogcrawlersettings['nothrow_urljson_list']
ro							 = RedisOperation(redis_pool)

class Watcher:
	def __init__(self):
		settings = Settings()
		self.process_no = settings.blogcrawlersettings['process_no']
		self.children = []
		self.watch()

	def watch(self):
		schedule_pid = os.getpid()
		counter = 0
		try:
			while True:
				child = os.fork()
				counter += 1
				if child == 0:
					runcrawler()
				else:
					self.children.append(child)
					print "create child process: %d"%child
					time.sleep(3)
					
				while counter >= self.process_no:
					for pid in self.children:
						p_child = psutil.Process(pid)
						memory_info = p_child.get_memory_info()
						#print str(pid) + " " + str(memory_info.rss)
						if memory_info.rss > 524288000:#  34M  35651584:    #120M 524288000 500M:
							os.kill(pid, signal.SIGKILL)
							counter -= 1
							self.children.remove(pid)
					time.sleep(5)
		except OSError: pass
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
			for pid in self.children:
				os.kill(pid, signal.SIGKILL)
		except OSError: pass

class BlogCrawler(threading.Thread):

	running   = True

	def __init__(self, threadname, thread_no):
		threading.Thread.__init__(self,name=threadname)
		signal.signal(signal.SIGINT, self.sigint_handle)
		self.settings			  = settings
		self.depth_limit          = self.settings.depth_limit
		self.page_url_set         = self.settings.blogcrawlersettings["page_url_set"]
		self.image_url_set        = self.settings.blogcrawlersettings["image_url_set"]
		self.nothrow_urljson_list = self.settings.blogcrawlersettings["nothrow_urljson_list"]
		self.delay_time           = 0.25
		self.datasyn			  = DataSynch(pool, ro, thread_no)
		self.ro					  = RedisOperation(redis_pool)
		self.util				  = Util()

	def sigint_handle(self, signum, frame):
		self.running = False
		#self.logger.info("Catch SINGINT interrupt signal...")

	def init_delay_flag(self, domain_sh):
		self.ro.set(domain_sh, 0)

	def reset_delay_flag(self, domain_sh):
		self.ro.set(domain_sh, 0)

	def wait_delay_ok(self, domain_sh):
		while self.ro.get(domain_sh) == 1:
			time.sleep(0.05)
		return 1

	# Set delay_flag to 1 after accessed on domain, Set delay_flag to 0 after delay_time
	def set_delay_timer(self, domain_sh):
		self.ro.set(domain_sh, 1)
		t = threading.Timer(self.delay_time, self.reset_delay_flag, [domain_sh])
		t.start()

	def get_urljson(self, urljson_list):
		# Wait for more urljson
		try:
			wait_amount = 0
			while self.ro.conn.llen(urljson_list) <= 0:
				print "wait 3 secs"
				time.sleep(3)
				wait_amount += 1
				if wait_amount > 3:
					self.running = False
					return None

			while self.ro.conn.llen(urljson_list) > 0:
				#print self.getName() + " - " + str(self.ro.conn.llen(urljson_list))
				return self.ro.rpop(urljson_list)
		except:
			pass

	# Append url to nothrow url list or level url list
	def appendnothrowurllist(self, urls, base_url, seed_url, seed_ext, seed_id, depth, page_type):
		#print str(urls)
		for uu in urls:
			if uu == None or uu == "":
				continue
			if type(uu) == tuple or type(uu) == list:
				urls.extend(uu)
				continue
			if not(uu.startswith("http://")):
				curl = urlparse.urljoin(base_url, uu)
			else:
				curl = uu
			curl = self.util.normalize_url(curl)
			import re
			if curl is None or re.findall(r"#[0-9a-zA-Z-_]*$", curl):
				continue
			# If crawler type is blog-crawler, then all urls must start with seed_url
			if self. settings.crawlertype['crawlertype'] == 2 and (not curl.startswith(seed_url)) and (not curl.startswith(seed_ext)):
				continue
			# Exclude invalid urls
			if not(curl == None or curl == "") and not(curl.startswith('javascript:')) and not(curl.startswith('mailto:')) and \
						 not(('.jpg' in curl) or ('.png' in curl) or ('.gif' in curl) or  ('.gpeg' in curl) or  \
						('.bmp' in curl) or ('.tiff' in curl) or ('.pcx' in curl) or ('.tga' in curl) or \
						('facebook.com' in curl) or ('google.com' in curl) or ('twitter.com' in curl) or ('google.com' in curl)):
				page_url_sh = hashlib.sha1(curl).hexdigest()
				#saddre = self.ro.sadd(self.page_url_set,  page_url_sh)		# add into page_url_set
				urljson = {'url': curl, 'seed_id': seed_id, 'depth':depth + 1, 'pagetype':page_type}
				re = self.ro.check_lpush(self.page_url_set, page_url_sh, self.nothrow_urljson_list, urljson)
				#self.ro.lpush(self.level_url_list + str(depth + 1), req_url_seed)

	def extract_blog_image(self, tree, urljson, blogpage_id):
		elements = tree.xpath("//img")
		for element in elements:
			imgitem = {}
			if element.xpath('name()') == 'img' and element.xpath('@src') is not None and len(element.xpath('@src')) > 0:
				#print element.xpath('name()') + " - " + element.xpath('@src')[0]
				imgitem['src']          = self.util.concat_image_url(urljson['url'], element.xpath('@src')[0])
				#print imgitem['src']
				imgitem['desc']         = self.util.get_start_one(element.xpath('@alt'))
				imgitem['postdate']     = datetime.datetime.now().date().isoformat()
				imgitem['sourcetypeid'] = 2
				#print imgitem['src']
				# Insert this image
				image_url_sh = hashlib.sha1(imgitem['src']).hexdigest()
				if self.ro.sadd(self.image_url_set, image_url_sh) > 0:
					image_id = self.datasyn.insert_image_with_download(imgitem, 1)
					if image_id > 0:
						self.datasyn.insert_blogpage_photo_relationship_with_id(blogpage_id, image_id)
			del imgitem

	def parse_blog_page(self, urljson, base_url):
		url = urljson['url']
		seed_url = self.ro.get(urljson['seed_id'])
		try:
			r = Request(urljson['url'])
			r.add_header('User-Agent', 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)')
			response = urllib2.urlopen(r)
			content = response.read()
			tree    = lxml.html.fromstring(content)
			#tree = lxml.html.parse(url)
			urls = tree.xpath("//a/@href")
			self.appendnothrowurllist(urls, urljson['url'], seed_url, self.util.getcompleteurl(seed_url), urljson['seed_id'], urljson['depth'], urljson['pagetype'])
			title = tree.xpath("//title/text()")
			tagstxts = tree.xpath('//*[not(self::a | self::style | self::script | self::head | self::img | self::noscript | self::form | self::option)]/text()')
		except:
			return -1

		text = ""
		try:
			for tagtxt in tagstxts:
				text += re.sub(r'\s', '', tagtxt)
		except:
			text = "no text"
		#print self.util.standardize_url(base_url, url)
		match = self.settings.pattern.match(url)
		if match == None:
			isdetailed = 0
		else:
			isdetailed = 1

		blogpageitem = {}
		blogpageitem['title']			  = self.util.get_start_one(title)
		blogpageitem['blogurl']			  = self.util.standardize_url(base_url, url)
		blogpageitem['blogdetail']		  = text
		blogpageitem['crawlerseedurl_id'] = urljson['seed_id']
		blogpageitem['detailed']		  = isdetailed

		blogpage_url_sh = hashlib.sha1(blogpageitem['blogurl']).hexdigest()
		blogpage_id = self.datasyn.insertblogpage_on_duplicate(blogpageitem)
		if blogpage_id > 0:
			self.datasyn.insertblogpageseedrelation_with_ignore(blogpage_id, urljson['seed_id'])
			self.extract_blog_image(tree, urljson, blogpage_id)

		del tree
		del urls
		del title
		del tagstxts
		del blogpageitem

    #def parse(self):
	def run(self):
		print "Thread - " + self.getName() + " started run"
		while self.running:
			urljson   = self.get_urljson(self.nothrow_urljson_list)
			if urljson is not None:
				urljson  = eval(urljson)
				self.parse_blog_page(urljson, urljson['url'])

	def stop(self):
		#self.thread_stop = True
		pass

def runcrawler():
	scs = []
	for i in range(36):
		scs.append(BlogCrawler("T" + str(i), i))
	for i in range(36):
		scs[i].start()
	for i in range(36):
		scs[i].join()
	for i in range(36):
		scs[i].stop()

def main(script, flag='with'):

	#ro.cleardb()
	ro.deletedb(blog_nothrow_urljson_list, blog_page_url_set, delete_level)
	time.sleep(3)
	init_datasyn = DataSynch(pool, ro, 100)
	init_datasyn.read_seeds(2)
	time.sleep(5)

	if flag == 'with':
		Watcher()
	elif flag != 'without':
		print 'unrecognized flag: ' + flag
		sys.exit()

if __name__ == '__main__':
	main(*sys.argv)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          blogsettings.py                                                                                     0000664 0000771 0000772 00000002464 12031246623 014022  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                import re

class Settings(object):

	pattern = re.compile(r'.*(\d{4})[-/](0?[1-9]|1[012])[-/](0?[1-9]|[12]\d|3[01])[-/]$|.*(\d{4})[-/](0?[1-9]|1[012])[-/](0?[1-9]|[12]\d|3[01])$|.*(\d{4})[-/](0?[1-9]|1[012])$|.*(\d{4})[-/](0?[1-9]|1[012])[-/]$|.*(\?showComment).*|.*(/page/).*|.*(\?=p).*|.*(\?cat=).*|.*(\?=page).*|.*(_archive).*|.*(#).*|.*(/search\?).*|.*(search/label).*|.*(index\.).*|.*(\?paged=).*|.*(/category/).*|.*(january)$|.*(january)/$|.*(february)$|.*(february)/$|.*(march)$|.*(march)/$|.*(april)$|.*(april)/$|.*(may)$|.*(may)/$|.*(june)$|.*(june)/$|.*(july)$|.*(july)/$|.*(august)$|.*(august)/$|.*(september)$|.*(september)/$|.*(october)$|.*(october)/$|.*(november)$|.*(november)/$|.*(december)$|.*(december)/$')

	mysqlsettings   = {'host':'weardex.com', 'port':3306, 'user':'fashion', 'passwd':'lmsi3229fashion', 'db':'fashion4_ads_test_1', 'charset':'utf8'}

	redissettings   = {'host':'localhost', 'port':6379, 'db':1}

	blogcrawlersettings    = {'page_url_set':'blog_page_url_set', 'nothrow_urljson_list':'blog_nothrow_urljson_list', 'image_url_set':'blog_image_url_set', \
								'seeds_set':'blog_seeds_set', 'process_no':20, 'thread_no':36, 'init_level':0}
	
	foldersettings		   = {'original_image_folder':'/public2/ads/image_test/original_image_folder/'}

	crawlertype = {'crawlertype':2}

	depth_limit = 1000
                                                                                                                                                                                                            CrawlerManager.py                                                                                   0000664 0000771 0000772 00000005112 12027737477 014222  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                #!/bin/python
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
                                                                                                                                                                                                                                                                                                                                                                                                                                                      filter_image.py                                                                                     0000664 0000771 0000772 00000002745 12031177350 013747  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                import MySQLdb as mdb
import re
from datetime import datetime, timedelta
from PIL import Image
import os.path
import sys

def func():
	conn = mdb.connect(host='137.132.145.238', user='fashion', passwd='lmsi3229fashion', db='fashion4_ads_test_1')
	cur = conn.cursor()
	
	sql = "select id, photourl, datepost, photourlhash from photocache limit 4000000"
	cur.execute(sql)
	rows = []
	rows = cur.fetchall()
	print len(rows)
	to = datetime.now().date() - timedelta(days=1)
	print to
	notice_counter = 0
	counter = 0
	less_counter = 0
	err_counter  = 0
	#print " before for"

	fi_err = open('err_url.file', 'w')
	fi_snq = open('snq_url.file', 'w')

	for row in rows:
		notice_counter += 1
		if notice_counter % 100000 == 0:
			print notice_counter
		if row[2] < to:
			file_path = '/public2/ads/image_test/original_image_folder/' + row[3][0:3] + "/" + row[3] + ".jpg"
			if os.path.exists(file_path):
				try:
					im = Image.open(file_path)
				except:
					#print file_path
					fi_err.write(str(row))
					fi_err.write("\n")
					fi_err.flush()
					err_counter += 1
					continue
				if im.mode != "RGB":
					im = im.convert("RGB")
				width, height = im.size
				if width * 1.0 / height > 3.5 or height * 1.0 / width > 3.5:
					counter += 1
					fi_snq.write(str(row) + " " + str(width) + " " + str(height))
					fi_snq.write("\n")
					fi_snq.flush()
			else:
				less_counter += 1

	print "counter: " + str(counter) + " , less_counter: " + str(less_counter) + ", err_counter: " + str(err_counter)

func()
                           lxml_multithread.py                                                                                 0000664 0000771 0000772 00000002104 12027737477 014704  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                import lxml.html
from mysqloperation import DataSynch
import threading

class Test(threading.Thread):

    def __init__(self, threadname, thread_no=0):
        threading.Thread.__init__(self,name=threadname)
        self.datasyn = DataSynch(thread_no)

    def test(self):
        #seedurl_list = file('seedurls.file').readlines()
    	self.datasyn.cur.execute("select url from crawlerseedurl where deleted=0 and photosourcetype_id=2")
        seeds = self.datasyn.cur.fetchall()
        for url in seeds:
            if url is not None and len(url) > 0:
                print self.getName() + " - " + url[0]
                try:
                    tree = lxml.html.parse(url[0])
                    #tag  = tree.xpath('//a/@href')
                    #print tag
                except:
                    pass
    def run(self):
        while True:
            self.test()

    def stop(self):
        pass
tt = []
for i in range(5):
    tt.append(Test("Thread_no_" + str(i), i))
for i in range(5):
    tt[i].start()
for i in range(5):
    tt[i].join()
for i in range(5):
    tt[i].stop()
                                                                                                                                                                                                                                                                                                                                                                                                                                                            mysqloperation.py                                                                                   0000664 0000771 0000772 00000111767 12032300576 014412  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                #!/bin/python
# -*- coding: utf-8 -*-
#This file is to define the mysql operations
#
# Only Provide the getconn service
# Project: Weardex.com
# Author : Zhang Weidong
# Class  : DataSynch
#
# Provide the following interface and inplement
#   1.__init__(self)
#   2.save_original_image(self)
#   3.insert_image_with_download(self, key, value)
#   4.insertproductpage(self)
#   5.insertproductpageseedrelation(self, userset, uservalue)
#   .....
###############################################
import time, re, glob, os, sys, socket
from datetime import datetime, date
import ImageFile
from PIL import Image
import logging, logging.handlers
from urlparse import urlparse
import urllib2, hashlib
import simplejson
import MySQLdb as mdb
import redis

#import json, httplib

from redisoperation import RedisOperation
from settings import Settings
from util import Util

# Global settings
#reload(sys)
#sys.setdefaultencoding('utf-8')
# timeout in seconds
timeout = 180
socket.setdefaulttimeout(timeout)

# create logging
now = datetime.now()
nowstr = str(now.year) + str(now.month) + str(now.day) + str(now.hour)
logger = logging.getLogger('crawler')
hdlr = logging.FileHandler('blogcrawler-' + nowstr + '.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)

class DataSynch(object):
	
	# Special characters which needed to be replace by blackspace
	punctuations_to_erase = ['`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', \
							'+', '-', '=', '{', '[', '}', ']', '|', ':', ';', '\'', '"', '<', ',', '>', '.', '/', '$', '\\', '•']

	settings					= Settings()
	blog_page_url_set            = settings.blogcrawlersettings['page_url_set']
	blog_nothrow_urljson_list	 = settings.blogcrawlersettings['nothrow_urljson_list']

	# Save image folder path
	original_image_folder		= settings.foldersettings['original_image_folder']
	util = Util()

	def __init__(self, connection_pool, ro, connection_no):
		self.pool = connection_pool
		self.ro   = ro
		self.mysql_conn = self.pool.connection(connection_no)
		self.cur = self.mysql_conn.cursor()

		self.logger = logger

	# Save original image directly
	def save_original_image(self, im, new_im_path):
		try:
			if im.mode != "RGB":
   				im = im.convert("RGB")
			im.save(new_im_path, "JPEG",quality=100)
			return 1
		except Exception, e:
			self.logger.error('save original image failed...error:%s'%(str(e)))
			return 0

	def init_delay_flag(self, domain_sh):
		self.ro.set(domain_sh, 0)

	def read_seeds(self, seed_type):
		#print self.getName() + " - read seeds from mysql..."
		#if self.ro.conn.llen(self.nothrow_urljson_list) > 0:
			#return 1
		try:
			if seed_type == 2:
				self.cur.execute("select id, url, webkit, photosourcetype_id from crawlerseedurl where deleted=0 and photosourcetype_id=%d"%(seed_type))
				seeds = self.cur.fetchall()
				print "len(seeds):" + str(len(seeds))
				for seed in seeds:
					urljson = {'url':seed[1], 'seed_id':seed[0], 'depth':1, 'pagetype':seed[3], 'jsmw':seed[2]}
					self.ro.set(seed[0], seed[1])
					#print urljson
					self.ro.check_lpush(self.blog_page_url_set, hashlib.sha1(seed[1]).hexdigest(), self.blog_nothrow_urljson_list, urljson)
					self.init_delay_flag(hashlib.sha1(self.util.get_domain(seed[1])).hexdigest())
			elif seed_type == 3:
				self.cur.execute("select url, id, SELLER_URL, CATEGORY, TITLE_XPATH, DETAILURL_XPATH, DETAILURLHEADER, NEXTPAGE_XPATH, NEXTPAGEHEADER, IMAGESOURCE, IMAGEURL_XPATH, IMAGEURLHEADER, BRAND_XPATH, DESCRIPTION_XPATH, PRODUCTID_XPATH, COLOR_XPATH, PRICE_XPATH, SIZE_XPATH, MAINIMAGEURL_XPATH from crawlerseedurl where photosourcetype_id=3 and deleted=0 and webkit=0")
				rows = self.cur.fetchall()
				print "len(product seeds):" + str(len(rows))
				for row in rows:
					#print str(row[18])
					self.ro.set(str(row[1]) + "_SELLER_URL",         row[2])
					self.ro.set(str(row[1]) + "_CATEGORY",           row[3])
					self.ro.set(str(row[1]) + "_TITLE_XPATH",        row[4])
					self.ro.set(str(row[1]) + "_DETAILURL_XPATH",    row[5])
					self.ro.set(str(row[1]) + "_DETAILURLHEADER",    row[6])
					self.ro.set(str(row[1]) + "_NEXTPAGE_XPATH",	 row[7])
					self.ro.set(str(row[1]) + "_NEXTPAGEHEADER",     row[8])
					self.ro.set(str(row[1]) + "_IMAGESOURCE",        row[9])
					self.ro.set(str(row[1]) + "_IMAGEURL_XPATH",     row[10])
					self.ro.set(str(row[1]) + "_IMAGEURLHEADER",	 row[11])
					self.ro.set(str(row[1]) + "_BRAND_XPATH",        row[12])
					self.ro.set(str(row[1]) + "_DESCRIPTION_XPATH",  row[13])
					self.ro.set(str(row[1]) + "_PRODUCTID_XPATH",    row[14])
					self.ro.set(str(row[1]) + "_COLOR_XPATH",        row[15])
					self.ro.set(str(row[1]) + "_PRICE_XPATH",        row[16])
					self.ro.set(str(row[1]) + "_SIZE_XPATH",         row[17])
					self.ro.set(str(row[1]) + "_MAINIMAGEURL_XPATH", row[18])
					if row[0] != None and row[0] != "" and row[1] != None and row[1] != "":
						seed_url = re.sub(r'\s', '', row[0])
						self.ro.set(row[1], seed_url)
						urljson = {'url': row[0], 'seed_id': row[1], 'depth':1, 'pagetype':3}
						#print urljson
						self.ro.check_lpush(self.product_page_url_set, hashlib.sha1(seed_url).hexdigest(), self.product_nothrow_urljson_list, urljson)
						self.init_delay_flag(hashlib.sha1(self.util.get_domain(seed_url)).hexdigest())
						
						#self.ro.lpush(self.page_url_nothrow_list, req_url_seed)
						#self.ro.lpush(self.level_url_list + "1", req_url_seed)
		except mdb.Error, e:
			self.logger.error("Error %d:%s"%(e.args[0], e.args[1]))
			sys.exit()

	# Check blogpage is existed
	def check_blogpage_exists(self, page_url_sh):
		try:
			ex_sql = "select id from blogpage where pageurlhash='%s'"%(page_url_sh)
			self.cur.execute(ex_sql)
			ex_re  = self.cur.fetchone()
			if ex_re != None and len(ex_re) > 0:
				return 1
			else:
				return 0
		except mdb.Error, e:
			self.logger.error("mysql check blogpage exists error, table: blogpage, errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql check blogpage exits error, table: blogpage, errormsg:%s - %s"%(e, ex_sql))

	# synchronize blog pge info
	def insertblogpage(self, item):
		import re
		#title = re.escape(item['title'])
		title = item['title'].replace('\'', '\\\'')
		if isinstance(item['title'], unicode):
			title = item['title'].encode('utf-8', 'ignore')
		else:
			title = item['title']
		if isinstance(item['blogdetail'], unicode):
			blogdetail = item['blogdetail'].encode('utf-8', 'ignore')
		else:
			blogdetail = item['blogdetail']
		
		blogurl	   = item['blogurl']
		blogurl_sh = hashlib.sha1(blogurl).hexdigest()
		for c in range(len(self.punctuations_to_erase)):
			blogdetail = blogdetail.replace(self.punctuations_to_erase[c], ' ')

		try:
			sql = "insert into blogpage(title, pageurl, pagetext, pageurlhash, detailed, dateinsert) values('%s', '%s', '%s', '%s', %d, UNIX_TIMESTAMP())"%(title, blogurl, blogdetail, blogurl_sh, int(item['detailed']))
			re = self.cur.execute(sql)
			self.mysql_conn.commit()
			#print "insert blogpage re:%d"%re
			if re >= 1:
				self.logger.info("Blog insert re: %d"%re)
			return re
		except mdb.Error, e:
			self.logger.error("mysql insert blogpage error, table: blogpage, errormsg:%d-%s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql insert blogpage error, table: blogpage, errormsg: %s"%(e))
			return 0
	
	# synchronize blog pge info
	def insertblogpage_on_duplicate(self, item):
		import re
		#title = re.escape(item['title'])
		title = item['title'].replace('\'', '\\\'')
		if isinstance(item['title'], unicode):
			title = item['title'].encode('utf-8', 'ignore')
		else:
			title = item['title']
		if isinstance(item['blogdetail'], unicode):
			blogdetail = item['blogdetail'].encode('utf-8', 'ignore')
		else:
			blogdetail = item['blogdetail']
		
		blogurl	   = item['blogurl']
		blogurl_sh = hashlib.sha1(blogurl).hexdigest()
		for c in range(len(self.punctuations_to_erase)):
			blogdetail = blogdetail.replace(self.punctuations_to_erase[c], ' ')
			title = title.replace(self.punctuations_to_erase[c], ' ')

		try:
			sql = "insert into blogpage(title, pageurl, pagetext, pageurlhash, detailed, dateinsert) values('%s', '%s', '%s', '%s', %d, UNIX_TIMESTAMP()) on duplicate key update title=values(title), pagetext=values(pagetext), daterefresh=UNIX_TIMESTAMP()"%(title, blogurl, blogdetail, blogurl_sh, int(item['detailed']))
			re = self.cur.execute(sql)
			self.mysql_conn.commit()
			#print "insert blogpage re:%d"%re
			if re >= 1:
				self.logger.info("Blog insert re:%s"%(re))
			return self.cur.lastrowid
		except mdb.Error, e:
			self.logger.error("mysql insert blogpage error, table: blogpage, errormsg:%d-%s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql insert blogpage error, table: blogpage, errormsg: %s"%(e))
			return 0

	# synchronize blog pge info
	def updateblogpage(self, item):
		import re
		title = re.escape(item['title'])
		title = item['title'].replace('\'', '\\\'')
		if isinstance(item['title'], unicode):
			title = item['title'].encode('utf-8', 'ignore')
		else:
			title = item['title']
		if isinstance(item['blogdetail'], unicode):
			blogdetail = item['blogdetail'].encode('utf-8', 'ignore')
		else:
			blogdetail = item['blogdetail']
		
		blogurl		   = item['blogurl']
		blogurl_sh = hashlib.sha1(blogurl).hexdigest()
		for c in range(len(self.punctuations_to_erase)):
			blogdetail = blogdetail.replace(self.punctuations_to_erase[c], ' ')
		sql = "update blogpage set title='%s', pagetext='%s', daterefresh=UNIX_TIMESTAMP() where pageurlhash='%s'"%(title, blogdetail, blogurl_sh)
		try:
			re = self.cur.execute(sql)
			self.mysql_conn.commit()
			if re == 1:
				self.logger.info("Blogpage update re:%s"%(re))
			return re
		except mdb.Error, e:
			self.logger.error("mysql update blogpage error, table: blogpage, errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql update blogpage error, table: blogpage, errormsg: %s"%(e))
			return 0
	
	# synchronize image info
	def insertimage(self, item):
		import re
		desc = re.escape(item['desc'])
		desc = item['desc'].replace('\'', '\\\'')
		if isinstance(item['desc'], unicode):
			desc = item['desc'].encode('utf-8', 'ignore')
		else:
			desc = item['desc']
		if isinstance(item['src'], unicode):
			itemsrc = item['src'].encode('utf-8', 'ignore')
		else:
			itemsrc = item['src']
		itemsrc = itemsrc.replace("'", "\\'")
		itemsrc = itemsrc.replace('"', '\\"')

		for c in range(len(self.punctuations_to_erase)):
			desc = desc.replace(self.punctuations_to_erase[c], ' ')

		photourlhash = hashlib.sha1(itemsrc).hexdigest()

		# Check image exists in mysql
		if self.check_image_exists(photourlhash) == 1:
			self.update_image(item)
		else:
			pass
			
		try:
			#print "CCCCCCCCCC"
			sql = "insert into photocache(datepost, photosourcetype_id, photourl, title, photourlhash) value('%s', %d, '%s', '%s', '%s')"%(item['postdate'], int(item['sourcetypeid']) , itemsrc , desc, photourlhash)
			#print "DDDDDDDDD"
			re = self.cur.execute(sql)
			self.mysql_conn.commit()
			#print "insert image re:%d"%re
			self.logger.info("Image insert re:%s"%(re))
			return re
		except mdb.Error, e:
			self.logger.error("mysql insert image error, table: photocache, errormsg: %d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql insert image error, table: photocache, errormsg:%s, sql:%s"%(e, sql))
			return 0

	# Update image info
	def update_image(self, item):
		import re
		desc = re.escape(item['desc'])
		desc = item['desc'].replace('\'', '\\\'')
		if isinstance(item['desc'], unicode):
			desc = item['desc'].encode('utf-8', 'ignore')
			#desc = item['desc'].encode('string_escape')
		else:
			desc = item['desc']
		if isinstance(item['src'], unicode):
			itemsrc = item['src'].encode('utf-8', 'ignore')
		else:
			itemsrc = item['src']
		itemsrc = itemsrc.replace("'", "\\'")
		itemsrc = itemsrc.replace('"', '\\"')

		for c in range(len(self.punctuations_to_erase)):
			desc = desc.replace(self.punctuations_to_erase[c], ' ')

		try:
			#print "AAAAAAAAAAAA"
			sql = "update photocache set title='%s' where photourlhash='%s'"%(desc, hashlib.sha1(itemsrc).hexdigest())
			re = self.cur.execute(sql)
			self.mysql_conn.commit()
			#print "BBBBBBBBBBBBBBBBB"
			self.logger.info("Image update re:%s"%(re))
			return re
		except mdb.Error, e:
			self.logger.error("mysql update image error, table: photocache, errormsg: %d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql update image error, table: photocache, errormsg: %s - sql:%s"%(e, sql))
			return 0

	# Simplely insert page info: pageurl, pageurlhash
	def insert_productpage_simple(self, page_url, page_url_sh):
		try:
			if self.check_product_exists(pageurl_sh) == 1:
				return 1;
			sql = "insert into productpage(pageurl, pageurlhash, dateinsert) values('%s', '%s', UNIX_TIMESTAMP())"%(page_url, page_url_sh)
			re = self.cur.execute(sql)
			self.mysql_conn.commit()
			if re >= 1:
				self.logger.info("Product insert re:%s"%(re))
			return re
		except mdb.Error, e:
			self.logger.error("mysql insert product error, table: productpage, errormsg: %d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql insert product page error, table: product page, errormsg: %s - sql:%s"%(e, sql))
			return 0
	
	# Check relation exists
	def check_relation_exists(self, image_url_sh, page_url_sh):
		try:
			sql = "select id from productphotopagerelation where photo_id=(select id from photocache where photourlhash='%s') and page_id=(select id from  productpage where pageurlhash='%s')"%(image_url_sh, page_url_sh)
			re = self.cur.execute(sql)
			relation_re  = self.cur.fetchone()
			if relation_re != None and len(relation_re) > 0:
				return 1
			else:
				return 0
		except mdb.Error, e:
			self.logger.error("mysql check relation error, table: productphotopagerelation, errormsg: %d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql check relation error, errormsg: %s - sql:%s"%(e, sql))
			return 0
	
	# Insert blogpage photo relation by pageurlhash and photourlhash
	def insert_blogpage_photo_relationship_sh(self, image_url_sh, page_url_sh):		
		#print "get into insert relation function"
		try:	
			imagesql     = "select id from photocache where photourlhash like '%s'"%(image_url_sh)
			pagesql      = "select id from blogpage where pageurlhash like '%s'"%(page_url_sh)
			imagere      = []
			pagere       = []
			self.cur.execute(imagesql)
			imagere = self.cur.fetchone()
			self.cur.execute(pagesql)
			pagere  = self.cur.fetchone()
			relationsql = "no sql"
			if imagere != None and pagere != None and len(imagere) > 0 and len(pagere) > 0:
				image_id = imagere[0]
				page_id  = pagere[0]
				relationsql = "insert into blogphotopagerelation(page_id, photo_id) values(%d, %d)"%(page_id, image_id)
				re = self.cur.execute(relationsql)
				#self.mysql_conn.commit()
				print "insert relation re:%d"%re
				self.logger.info("Relationship insert re:%d, %s"%(re, relationsql))
				#self.mysql_conn.commit()
				return re
			else:
				self.logger.error("Relationship insert failed no page or image error:%s - %s"%(image_url_sh, page_url_sh))
				return 0
		except mdb.Error, e:
			self.logger.error("mysql insert relation error, table: blogphotopagerelation, errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("insert relation error:%s - %s"%(e, relationsql))
			return 0

	# Insert blogpage photo relation by pageurlhash and photourlhash
	def insert_blogpage_photo_relationship_with_id(self, page_id, image_id):		
		#print "get into insert relation function"
		try:	
			relationsql = "insert ignore into blogphotopagerelation(page_id, photo_id) values(%d, %d)"%(page_id, image_id)
			re = self.cur.execute(relationsql)
			#self.mysql_conn.commit()
			#print "insert relation re:%d"%re
			self.logger.info("Relationship insert re:%d, %s"%(re, relationsql))
			#self.mysql_conn.commit()
			return re
		except mdb.Error, e:
			self.logger.error("mysql insert relation error, table: blogphotopagerelation, errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("insert relation error:%s - %s"%(e, relationsql))
			return 0
	
	# Insert blogpage photo relation by pageurlhash and photourlhash
	def insert_relationship_with_id(self, tablename, field, page_id, image_id):		
		#print "get into insert relation function"
		try:	
			relationsql = "insert ignore into %s(page_id, %s) values(%d, %d)"%(tablename, field, page_id, image_id)
			re = self.cur.execute(relationsql)
			#self.mysql_conn.commit()
			#print "insert relation re:%d"%re
			self.logger.info("Relationship insert re:%d, %s"%(re, relationsql))
			#self.mysql_conn.commit()
			return re
		except mdb.Error, e:
			self.logger.error("mysql insert relation error, table: %s, errormsg:%d - %s"%(tablename, e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("insert relation error:%s - %s"%(e, relationsql))
			return 0

	# Insert relation by pageurlhash and photourlhash
	def insert_productpage_photo_relationship_sh(self, image_url_sh, page_url_sh):		
		#print "get into insert relation function"
		try:	
			imagesql     = "select id from photocache where photourlhash like '%s'"%(image_url_sh)
			pagesql      = "select id from productpage where pageurlhash like '%s'"%(page_url_sh)
			imagere      = []
			pagere       = []
			self.cur.execute(imagesql)
			imagere = self.cur.fetchone()
			self.cur.execute(pagesql)
			pagere  = self.cur.fetchone()
			relationsql = "no sql"
			if imagere != None and pagere != None and len(imagere) > 0 and len(pagere) > 0:
				image_id = imagere[0]
				page_id  = pagere[0]
				relationsql = "insert into productphotopagerelation(page_id, photo_id) values(%d, %d)"%(page_id, image_id)
				re = self.cur.execute(relationsql)
				self.mysql_conn.commit()
				#print "insert relation re:%d"%re
				self.logger.error("Relationship insert re:%d, %s"%(re, relationsql))
				self.mysql_conn.commit()
				return re
			else:
				self.logger.error("Relationship insert failed no page or image error:%s - %s"%(image_url_sh, page_url_sh))
				return 0
		except mdb.Error, e:
			self.logger.error("mysql insert relation error, table: photophotopagerelation, errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("insert relation error:%s - %s"%(e, relationsql))
			return 0

	# Insert [photo, page]
	def insertrelationship(self, relationship_json):
		#print "get into insert relation function"
		try:	
			if relationship_json != None and relationship_json != "" and len(relationship_json) >= 2:
				relationshipitem = RelationshipItem()
				relationshipitem = simplejson.loads(relationship_json)
				image_url = relationshipitem[0]
				page_url = relationshipitem[1]
				if page_url.endswith('/'):
					page_url = page_url[0:-1]
				image_url_sh = hashlib.sha1(image_url).hexdigest()
				page_url_sh  = hashlib.sha1(page_url).hexdigest()

				imagesql     = "select id from photocache where photourlhash like '%s'"%(image_url_sh)
				pagesql      = "select id from productpage where pageurlhash like '%s'"%(page_url_sh)
				imagere      = []
				pagere       = []
				self.cur.execute(imagesql)
				imagere = self.cur.fetchone()
				self.cur.execute(pagesql)
				pagere  = self.cur.fetchone()
				relationsql = "no sql"
				if imagere != None and pagere != None and len(imagere) > 0 and len(pagere) > 0:
					image_id = imagere[0]
					page_id  = pagere[0]
					relationsql = "insert into productphotopagerelation(page_id, photo_id) values(%d, %d)"%(page_id, image_id)
					re = self.cur.execute(relationsql)
					self.mysql_conn.commit()
					#print "insert relation re:%d"%re
					self.logger.info("Relationship insert re:%d, %s"%(re, relationsql))
					self.mysql_conn.commit()
					return re
				else:
					self.logger.error("Relationship insert failed no page or image error: %s"%(relationsql))
					return 0
			else:
				self.logger.error("relationship_json error,relationship_json:%s"%(relationship_json))
				return 0
		except mdb.Error, e:
			self.logger.error("mysql insert relation error, table: photocache, errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("insert relation error:%s - %s"%(e, relation_json))
			return 0
	
	# Chekc blogpageseedrelation is existed
	def check_blogpageseedrelation_exists(self, page_url_sh, seed_id):
		try:
			sql = "select id from blogpageseedrelation where page_id=(select id from blogpage where pageurlhash='%s') and seed_id=%d"%(page_url_sh, seed_id)
			re = self.cur.execute(sql)
			ex_re  = self.cur.fetchone()
			if ex_re != None and len(ex_re) > 0:
				return 1
			else:
				return 0
		except mdb.Error, e:
			self.logger.error("mysql insert blogpageseedrelation error, table: productpageseedrelation errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("check blogpageseedrelation blogpage-seed relation error:%d - '%s - %s'"%(seed_id, page_url_sh, e))
			return 0


	# Insert blogpageseedrelation
	def insertblogpageseedrelation(self, blogpage_url_sh, seed_id):
		if blogpage_url_sh == None or seed_id == None or blogpage_url_sh == "" or seed_id == "":
			return 0
		try:
			# select page's id from blogpage
			select_sql = "select id from blogpage where pageurlhash='%s'"%blogpage_url_sh
			pagere       = []
			self.cur.execute(select_sql)
			pagere  = self.cur.fetchone()
			if pagere != None and len(pagere) > 0:
				page_id  = pagere[0]
				insert_sql = "insert into blogpageseedrelation(page_id, seed_id) value(%d, %d)"%(page_id, seed_id)
				#print insert_sql;
				re = self.cur.execute(insert_sql)
				#self.mysql_conn.commit()
				self.logger.info("Blogpage seed relationship insert re:%d, %s"%(re, blogpage_url_sh))
				return re
			else:
				self.logger.error("Blogpage seed relationship insert failed no page, blogpage_url_sh error:%s"%(blogpage_url_sh))
				return 0
		except mdb.Error, e:
			self.logger.error("mysql insert blogpage-seed relation error, table: photocache, errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("insert blogpage-seed relation error:%s - %s"%(e, blogpage_url_sh))
			return 0

	# Insert blogpageseedrelation
	def insertblogpageseedrelation_with_ignore(self, blogpage_id, seed_id):
		if blogpage_id == None or seed_id == None or blogpage_id == 0 or seed_id == 0:
			return 0
		try:
			insert_sql = "insert ignore into blogpageseedrelation(page_id, seed_id) value(%d, %d)"%(blogpage_id, seed_id)
			#print insert_sql;
			re = self.cur.execute(insert_sql)
			#self.mysql_conn.commit()
			self.logger.info("Blogpage seed relationship insert re:%d,blogpage_id - seed_id: %d - %d"%(re, blogpage_id, seed_id))
			return re
		except mdb.Error, e:
			self.logger.error("mysql insert blogpage-seed relation error, table: photocache, errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("insert blogpage-seed relation error:%s - %d"%(e, blogpage_id))
			return 0

	def insert_url_hash(self, url, url_sh):
		try:
			#sql = "insert into urlhash(urlhash) values('" + url_sh + "')"
			#re = self.cur.execute(sql)
			#self.mysql_conn.commit()
			self.ro.urlhash_conn.sadd(self.url_hash_set, url_sh)
			return re
		except mdb.Error, e:
			self.logger.error("mysql insert urlhash error, table: urlhash, errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("insert into urlhash url(%s) failed: %s "%(e, url))
			return 0

	# Check product page exits
	def check_product_exists(self, pageurl_sh):
		ex_re = []
		try:
			ex_sql = "select id from productpage where pageurlhash='%s'"%(pageurl_sh)
			self.cur.execute(ex_sql)
			ex_re  = self.cur.fetchone()
			if ex_re != None and len(ex_re) > 0:
				return 1
			else:
				return 0
		except mdb.Error, e:
			self.logger.error("mysql check productpage exists error, table: productpage, errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql check product exits error, table: productpage, errormsg:%s - %s"%(e, ex_sql))

	# Check image exits
	def check_image_exists(self, imageurl_sh):
		ex_re = []
		try:
			ex_sql = "select id from photocache where photourlhash='%s'"%(imageurl_sh)
			self.cur.execute(ex_sql)
			ex_re  = self.cur.fetchone()
			if ex_re != None and len(ex_re) > 0:
				return 1
			else:
				return 0
		except mdb.Error, e:
			self.logger.error("mysql check image exists error, table: photocache, errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql check image exits error, table: productpage, errormsg:%s - %s"%(e, ex_sql))

	# Save image
	def save_image(self, image_url, image_url_sh, quality_size):
		if quality_size == 1:
			min_size = 200
		else:
			min_size = 80
		# try 4 times to download the image
		width = 1
		height = 1
		for i in range(4):
			try:
				if image_url != None:
					imagefile = urllib2.urlopen(image_url)
				else:
					return 0
				p = ImageFile.Parser()
				while 1:
					data = imagefile.read(1024)
					if not data:
						break
					p.feed(data)
				if p.image:
					width  = p.image.size[0]
					height = p.image.size[1]
					if width < min_size or height < min_size:
						return 0
					if quality_size == 1 and (width*1.0/height > 3.5 or height*1.0/width > 3.5):
						return 0
				else:
					return 0

				im = p.close()
				imagefile_name = image_url_sh + ".jpg"
				sub_dir = imagefile_name[0:3] + "/"
				save_re = self.save_original_image(im, self.original_image_folder + sub_dir + imagefile_name)
				if save_re == 1:
					return save_re
			except Exception, e:
				self.logger.error("save image failed url error: %s - %s"%(e, image_url))
				time.sleep(0.25)
		# If failed save image then recode the image url
		self.ro.lpush("failed_image_ur", image_url)

	# insert image and download
	def	insert_image_with_download(self, item, quality_size):
		#print "get into insert_image function"
		import re
		desc = re.escape(item['desc'])
		desc = item['desc'].replace('\'', '\\\'')
		if isinstance(item['desc'], unicode):
			desc = item['desc'].encode('utf-8', 'ignore')
		else:
			desc = item['desc']
		if isinstance(item['src'], unicode):
			itemsrc = item['src'].encode('utf-8', 'ignore')
		else:
			itemsrc = item['src']

		# note: the photourlhash is calcuted before the '/" was escaped
		photourlhash = hashlib.sha1(itemsrc).hexdigest()
		itemsrc = itemsrc.replace("'", "\\'")
		itemsrc = itemsrc.replace('"', '\\"')

		for c in range(len(self.punctuations_to_erase)):
			desc = desc.replace(self.punctuations_to_erase[c], ' ')

		# Check image exists in mysql
		if self.check_image_exists(photourlhash) == 1:
			self.update_image(item)
		else:
			pass
			
		sql = ""
		try:
			# If save failed
			if self.save_image(itemsrc, photourlhash, quality_size) == 0:
				return 0
			#print "CCCCCCCCCC"
			sql = "insert ignore into photocache(datepost, photosourcetype_id, photourl, title, photourlhash) value('%s', %d, '%s', '%s', '%s')"%(item['postdate'], int(item['sourcetypeid']) , itemsrc , desc, photourlhash)
			#print "DDDDDDDDD"
			re = self.cur.execute(sql)
			#self.mysql_conn.commit()
			#print "insert image re:%d"%re
			self.logger.info("Image insert re:%s"%(re))
			return self.cur.lastrowid
		except mdb.Error, e:
			self.logger.error("mysql insert image with download error, table: photocache, errormsg: %d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql insert image with download error, table: photocache, errormsg:%s - sql:%s"%(e, sql))
			return 0

	# synchronize blog pge info
	def insertproductpage(self, item):
		pageurl_sh      = hashlib.sha1(item['producturl']).hexdigest()
		mainimageurl_sh = hashlib.sha1(item['mainimageurl']).hexdigest()

		for c in range(len(self.punctuations_to_erase)):
			item['title']	 = (item['title']).replace(self.punctuations_to_erase[c], ' ')
			item['brand']    = (item['brand']).replace(self.punctuations_to_erase[c], ' ')
			item['desc']     = (item['desc']).replace(self.punctuations_to_erase[c], ' ')
			item['price']    = (item['price']).replace(self.punctuations_to_erase[c], ' ')
			item['color']    = (item['color']).replace(self.punctuations_to_erase[c], ' ')
			item['size']     = (item['size']).replace(self.punctuations_to_erase[c], ' ')
			item['pagetext'] = (item['pagetext']).replace(self.punctuations_to_erase[c], ' ')
		
		item['title']		= (item['title']).replace("'", "\\'")
		item['title']		= (item['title']).replace('"', '\ "')
		item['producturl']	= (item['producturl']).replace("'", "\\'")
		item['producturl']	= (item['producturl']).replace('"', '\\"')

		# Check page exists
		if self.check_product_exists(pageurl_sh) == 1:
			self.updateproductpage(item)
			return 1

		try:
			sql = "insert into productpage(title, brand, description, price, color, size, pageurl, imageamount, productid, pagetext, pageurlhash, dateinsert) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, '%s','%s', '%s', UNIX_TIMESTAMP())"%(item['title'], item['brand'], item['desc'], item['price'], item['color'], item['size'], item['producturl'], int(item['imageamount']), item['productid'], item['pagetext'], pageurl_sh)
			re = self.cur.execute(sql)
			self.mysql_conn.commit()
			if re >= 1:
				self.logger.info("Product insert re:%s"%(re))
			return re
		except mdb.Error, e:
			self.logger.error("mysql update productpage error, table: productpage errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql insert productpage page error, table: productpage, errormsg:%s"%(e))
			return 0

	# synchronize blog pge info
	def insertproductpage_on_duplicate(self, item):
		pageurl_sh      = hashlib.sha1(item['producturl']).hexdigest()
		mainimageurl_sh = hashlib.sha1(item['mainimageurl']).hexdigest()

		for c in range(len(self.punctuations_to_erase)):
			item['title']	 = (item['title']).replace(self.punctuations_to_erase[c], ' ')
			item['brand']    = (item['brand']).replace(self.punctuations_to_erase[c], ' ')
			item['desc']     = (item['desc']).replace(self.punctuations_to_erase[c], ' ')
			item['price']    = (item['price']).replace(self.punctuations_to_erase[c], ' ')
			item['color']    = (item['color']).replace(self.punctuations_to_erase[c], ' ')
			item['size']     = (item['size']).replace(self.punctuations_to_erase[c], ' ')
			item['pagetext'] = (item['pagetext']).replace(self.punctuations_to_erase[c], ' ')
		
		item['title']		= (item['title']).replace("'", "\\'")
		item['title']		= (item['title']).replace('"', '\ "')
		item['producturl']	= (item['producturl']).replace("'", "\\'")
		item['producturl']	= (item['producturl']).replace('"', '\\"')

		try:
			sql = "insert into productpage(title, brand, description, price, color, size, pageurl, imageamount, productid, pagetext, pageurlhash, dateinsert) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, '%s','%s', '%s', now()) on duplicate key update title=values(title), pagetext=values(pagetext), daterefresh=now()"%(item['title'], item['brand'], item['desc'], item['price'], item['color'], item['size'], item['producturl'], int(item['imageamount']), item['productid'], item['pagetext'], pageurl_sh)
			re = self.cur.execute(sql)
			self.mysql_conn.commit()
			if re >= 1:
				self.logger.info("Product insert re:%s"%(re))
			return self.cur.lastrowid
		except mdb.Error, e:
			self.logger.error("mysql update productpage error, table: productpage errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql insert productpage page error, table: productpage, errormsg:%s"%(e))
			return 0

	# update productpage's imageamount and mainimage_id
	def updateproductpage_simple(self, page_url_sh, image_amount, mainimage_sh):
		try:
			imageid_re       = []
			select_sql = "select id from photocache where photourlhash='%s'"%(mainimage_sh)
			self.cur.execute(select_sql)
			imageid_re  = self.cur.fetchone()
			if imageid_re != None and len(imageid_re) > 0:
				##print imageid_re
				image_id  = int(imageid_re[0])
				sql = "update productpage set imageamount=%d, mainimage_id=%d where pageurlhash='%s'"%(image_amount, image_id, page_url_sh)
				re = self.cur.execute(sql)
				self.mysql_conn.commit()
				return re
			else:
				self.logger.error("mysql update productpage error, table: productpage errormsg:%d - %s"%(e.args[0], e.args[1]))
		except mdb.Error, e:
			self.logger.error("mysql update productpage error, table: productpage errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql update productpage error, table: productpage, errormsg:%d-%s"%(e.args[0], e.args[1]))
			return 0

	# synchronize blog pge info
	def updateproductpage(self, item):
		#print "get into the updaterproduct page funcion"
		pageurl_sh      = hashlib.sha1(item['producturl']).hexdigest()
		mainimageurl_sh = hashlib.sha1(item['mainimageurl']).hexdigest()

		for c in range(len(self.punctuations_to_erase)):
			item['title']	 = (item['title']).replace(self.punctuations_to_erase[c], ' ')
			item['brand']    = (item['brand']).replace(self.punctuations_to_erase[c], ' ')
			item['desc']     = (item['desc']).replace(self.punctuations_to_erase[c], ' ')
			item['price']    = (item['price']).replace(self.punctuations_to_erase[c], ' ')
			item['color']    = (item['color']).replace(self.punctuations_to_erase[c], ' ')
			item['size']     = (item['size']).replace(self.punctuations_to_erase[c], ' ')
			item['pagetext'] = (item['pagetext']).replace(self.punctuations_to_erase[c], ' ')
		
		item['title']		= (item['title']).replace("'", "\\'")
		item['title']		= (item['title']).replace('"', '\ "')
		item['producturl']	= (item['producturl']).replace("'", "\\'")
		item['producturl']	= (item['producturl']).replace('"', '\\"')

		# Get main image id
		sql = ""
		try:
			imageid_re       = []
			select_sql = "select id from photocache where photourlhash='%s'"%(mainimageurl_sh)
			self.cur.execute(select_sql)
			imageid_re  = self.cur.fetchone()
			if imageid_re != None and len(imageid_re) > 0:
				#print imageid_re
				image_id  = int(imageid_re[0])
				sql = "update productpage set title='%s', price='%s', size='%s', productid='%s', pagetext='%s', mainimage_id=%d, daterefresh=UNIX_TIMESTAMP() where pageurlhash='%s'"%(item['title'], item['price'], item['size'], str(item['productid']), item['pagetext'], int(image_id), pageurl_sh)
			else:
				sql = "update productpage set title='%s', price='%s', size='%s', productid='%s', pagetext='%s', daterefresh=UNIX_TIMESTAMP() where pageurlhash='%s'"%(item['title'], str(item['price']), str(item['size']), str(item['productid']), item['pagetext'], pageurl_sh)
				#print "NNN - sql:"
		except Exception, e:
			self.logger.error("mysql update productpage error:get main image id error, table: productpage, errormsg:%s"%(e))
		#print "&&&&&&&&&&&&"
		try:
			re = self.cur.execute(sql)
			self.mysql_conn.commit()
			#print "update re:%d"%re
			if re == 0:
				#print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ %s - %s"%(pageurl_sh, item['producturl'])
				self.insertproductpage(item)
			if re == 1:
				self.logger.info("Product page update re:%s"%(re))
			return re
		except mdb.Error, e:
			self.logger.error("mysql update productpage error, table: productpage errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql update productpage error, table: productpage, errormsg: %s"%(e))
			return 0
	
	# Check productpageseedrelation is existed
	def check_productpageseedrelation_exists(self, page_url_sh, seed_id):
		try:
			sql = "select id from productpageseedrelation where seed_id=%d and page_id=(select id from productpage where pageurlhash='%s')"%(seed_id, page_url_sh)
			re = self.cur.execute(sql)
			ex_re  = self.cur.fetchone()
			if ex_re != None and len(ex_re) > 0:
				return 1
			else:
				return 0
		except mdb.Error, e:
			self.logger.error("mysql insert productpageseedrelation error, table: productpageseedrelation errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("check product page-seed relation error:%d - %s - %s"%(seed_id, productpage_url_sh, e))
			return 0

	# Insert productpageseedrelation
	def insertproductpageseedrelation(self, productpage_url_sh, seed_id):
		#print "get into the insertproductpageseedrelation function"
		if productpage_url_sh == None or seed_id == None or productpage_url_sh == "" or seed_id == "":
			return 0
		try:
			# select page's id from blogpage
			select_sql = "select id from productpage where pageurlhash='%s'"%(productpage_url_sh)
			pagere       = []
			self.cur.execute(select_sql)
			pagere  = self.cur.fetchone()
			if pagere != None and len(pagere) > 0:
				page_id  = pagere[0]
				insert_sql = "insert into productpageseedrelation(page_id, seed_id) value(%d, %d)"%(page_id, seed_id)
				#print insert_sql;
				re = self.cur.execute(insert_sql)
				self.mysql_conn.commit()
				self.logger.error("product page seed relationship insert re:%d, %s"%(re, productpage_url_sh))
				return re
			else:
				self.logger.error("Product page seed relationship insert failed no page, productpage_url_sh error:%s"%(productpage_url_sh))
				return 0
		except mdb.Error, e:
			self.logger.error("mysql insert productpageseedrelation error, table: productpageseedrelation errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("insert product page-seed relation error:'%s - %s'"%(productpage_url_sh, e))
			return 0
         ProductCrawler.py                                                                                   0000664 0000771 0000772 00000027074 12032301142 014247  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                #!/bin/python
# -*- coding: utf-8 -*-
from redisoperation import RedisOperation
from productmysqloperation import DataSynch
from util import Util

import sys, os, re, operator, datetime, time, signal, threading, socket
import hashlib, urlparse
import urllib, cookielib
import urllib2
from urllib2 import Request
import encodings.idna

import lxml.html
import MySQLdb as mdb
from DBUtils.PooledDB import PooledDB
import redis

from productsettings import Settings

# timeout in seconds
reload(sys)
sys.setdefaultencoding('utf-8')
timeout = 360
socket.setdefaulttimeout(timeout)

# Get mysqldb/redis host info
settings					= Settings()
delete_level				= settings.productcrawlersettings['init_level']
mysql_host					= settings.mysqlsettings['host']
mysql_port					= settings.mysqlsettings['port']
mysql_user					= settings.mysqlsettings['user']
mysql_passwd				= settings.mysqlsettings['passwd']
mysql_db					= settings.mysqlsettings['db']
mysql_charset				= settings.mysqlsettings['charset']
redis_host				    = settings.redissettings['host']
redis_port				    = settings.redissettings['port']
redis_db				    = settings.redissettings['db']
# Init the connections to mysqldb/redis
mysql_pool = PooledDB( creator = mdb, mincached = 5, db = mysql_db, host = mysql_host, user = mysql_user, passwd= mysql_passwd, charset = "utf8", use_unicode = True)
redis_pool = redis.ConnectionPool(host=redis_host, port=redis_port, db=int(redis_db))
# Datastures stored in redis
product_page_url_set         = settings.productcrawlersettings['page_url_set']
product_nothrow_urljson_list = settings.productcrawlersettings['nothrow_urljson_list']
ro							 = RedisOperation(redis_pool)

class Watcher:
	def __init__(self):
		settings = Settings()
		self.process_no = settings.productcrawlersettings['process_no']
		self.children = []
		self.watch()

	def watch(self):
		counter = 0
		try:
			while True:
				child = os.fork()
				counter += 1
				if child == 0:
					runcrawler()
				else:
					self.children.append(child)
					print "create child process: %d"%child
					time.sleep(3)
					
				while counter >= self.process_no:
					for pid in self.children:
						p_child = psutil.Process(pid)
						memory_info = p_child.get_memory_info()
						if memory_info.rss > 524288000:    #120M 524288000 500M:
							os.kill(pid, signal.SIGKILL)
							counter -= 1
							self.children.remove(pid)
					time.sleep(3)
		except OSError: pass
		except KeyboardInterrupt:
			print '\n'
			print 'KeyBoardInterrupt,begin to clear...'
			print '\n'
			self.kill()
		sys.exit()

	def kill(self):
		try:
			print 'clear finish and exit...'
			for pid in self.children:
				os.kill(pid, signal.SIGKILL)
		except OSError: pass

class ProductCrawler(threading.Thread):

	running   = True

	def __init__(self, threadname, thread_no):
		threading.Thread.__init__(self,name=threadname)
		signal.signal(signal.SIGINT, self.sigint_handle)

		self.settings = settings

		self.depth_limit			= self.settings.depth_limit
		self.url_set				= self.settings.productcrawlersettings["page_url_set"]
		self.image_url_set			= self.settings.productcrawlersettings["image_url_set"]
		self.nothrow_urljson_list	= self.settings.productcrawlersettings["nothrow_urljson_list"]
		self.failed_page_url_set_1	= "failed_page_url_set_1"
		self.failed_page_url_set_2	= "failed_page_url_set_2"
		self.failed_page_url_set_3	= "failed_page_url_set_3"
		self.delay_time				= 0.25
		self.datasyn				= DataSynch(mysql_pool, ro, thread_no)#DataSynch(mysql_conn, ro, thread_no)##DataSynch(thread_no)
		self.ro						= RedisOperation(redis_pool)
		self.util					= Util()

	# Deprecated
	def sigint_handle(self, signum, frame):
		self.running = False

	# Deprecated
	def init_delay_flag(self, domain_sh):
		self.ro.set(domain_sh, 0)

	# Deprecated
	def reset_delay_flag(self, domain_sh):
		self.ro.set(domain_sh, 0)

	# Deprecated
	def wait_delay_ok(self, domain_sh):
		while self.ro.get(domain_sh) == 1:
			time.sleep(0.05)
		return 1

	# Deprecated
	# Set delay_flag to 1 after accessed on domain
	# Set delay_flag to 0 after delay_time
	def set_delay_timer(self, domain_sh):
		self.ro.set(domain_sh, 1)
		t = threading.Timer(self.delay_time, self.reset_delay_flag, [domain_sh])
		t.start()

	def get_urljson(self, urljson_list):
		# Wait for more urljson
		wait_amount = 0
		while self.ro.conn.llen(urljson_list) <= 0:
			print "wait 3 secs"
			time.sleep(3)
			wait_amount += 1
			if wait_amount > 3:
				time.sleep(180)
				self.datasyn.read_seeds(3)

		while self.ro.conn.llen(urljson_list) > 0:
			return self.ro.rpop(urljson_list)

	def get_start_one(self, la):
		if la is not None and type(la) is list and len(la) > 0:
			return la[0]
		else:
			return ""

	def join_list(self, la):
		if la is not None and type(la) is list and len(la) > 0:
			return ' '.join(la)
		else:
			return ''
	
	# Append url to nothrow url list or level url list
	def appendnothrowurllist(self, urls, base_url, seed_url, seed_ext, seed_id, depth, page_type, category):
		#print str(urls)
		urllist = []
		if type(urls) is not list:
			urllist.append(urls)
		else:
			urllist = urls
		for uu in urllist:
			if uu == None or uu == "":
				continue
			if type(uu) == tuple or type(uu) == list:
				urls.extend(uu)
				continue
			curl = urlparse.urljoin(base_url, uu)
			#print str(page_type) + " " + seed_url + ' - ' + uu + ' = ' + curl
			#curl = self.util.normalize_url(curl)
			#print curl
			import re
			if curl is None or re.findall(r"#[0-9a-zA-Z-_]*$", curl):
				continue
			# Exclude invalid urls
			if not(curl == None or curl == "") and not(curl.startswith('javascript:')) and not(curl.startswith('mailto:')) \
						 and not(('.jpg' in curl) or ('.png' in curl) or ('.gif' in curl) or  ('.gpeg' in curl) or  ('.bmp' in curl)\
								 or ('.tiff' in curl) or ('.pcx' in curl) or ('.tga' in curl) or ('facebook.com' in curl) or \
								 ('google.com' in curl) or ('twitter.com' in curl) or ('google.com' in curl)):
				page_url_sh = hashlib.sha1(curl).hexdigest()
				#saddre = self.ro.sadd(self.page_url_set,  page_url_sh)		# add into page_url_set
				urljson = {'url': curl, 'seed_id': seed_id, 'depth':depth + 1, 'pagetype':page_type, 'category':category}
				#if page_type == 3 and self.ro.conn.llen(self.nothrow_urljson_list) < 500:
					#self.ro.lpush(self.nothrow_urljson_list, urljson)
				#else:
				re = self.ro.check_lpush(self.url_set, page_url_sh, self.nothrow_urljson_list, urljson)
				#self.ro.lpush(self.level_url_list + str(depth + 1), req_url_seed)

	# Extract urls from page-detail
	def parse_product_listpage(self, urljson):
		#print "in extracturls function"
		seed_id = urljson['seed_id']
		try:
			r = Request(urljson['url'])
			r.add_header('User-Agent', 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)')
			response = urllib2.urlopen(r)
			content = response.read()
			tree    = lxml.html.fromstring(content)
		except:
			self.ro.lpush(self.nothrow_urljson_list, urljson)
			return -1
		# Depth surpass the limit, then exit
		if urljson['depth'] >= self.depth_limit:
			return None
		urls = tree.xpath(self.ro.get(str(seed_id) + "_" + "DETAILURL_XPATH"))
		print str(urljson)
		print "detail page url num:", len(urls)
		restr6 = tree.xpath(self.ro.get(str(seed_id) + "_" + "NEXTPAGE_XPATH"))
		print "list page url num:", len(restr6)

		seed_url = self.ro.get(urljson['seed_id'])
		self.appendnothrowurllist(urls, urljson['url'], seed_url, self.util.getcompleteurl(seed_url), urljson['seed_id'], urljson['depth'], 31, urljson['category'])
		self.appendnothrowurllist(restr6, urljson['url'], seed_url, self.util.getcompleteurl(seed_url), urljson['seed_id'], urljson['depth'], 3, urljson['category'])
		
	# Extract product detail page info
	def parse_product_detailpage(self, url, seed_id, depth, category, urljson):
		#print "in parse_productdetailpage function=========="
		response_url = url
		try:
			#tree = lxml.html.parse(url)
			content = urllib.urlopen(url).read()
			tree    = lxml.html.fromstring(content)
		except:
			#time.sleep(600)
			return -1
		productpageitem = {}
		page_url_sh     = hashlib.sha1(response_url).hexdigest()
		#fi = open(str("tmp/" + page_url_sh), 'w')
		#fi.write(content)
		
		restr3  = tree.xpath(self.ro.get(str(seed_id) + "_" + "TITLE_XPATH"))
		restr9  = tree.xpath(self.ro.get(str(seed_id) + "_" + "IMAGEURL_XPATH"))
		restr11 = tree.xpath(self.ro.get(str(seed_id) + "_" + "BRAND_XPATH"))
		restr12 = tree.xpath(self.ro.get(str(seed_id) + "_" + "DESCRIPTION_XPATH"))
		restr13 = tree.xpath(self.ro.get(str(seed_id) + "_" + "PRODUCTID_XPATH"))
		restr14 = tree.xpath(self.ro.get(str(seed_id) + "_" + "COLOR_XPATH"))
		restr15 = tree.xpath(self.ro.get(str(seed_id) + "_" + "PRICE_XPATH"))
		restr16 = tree.xpath(self.ro.get(str(seed_id) + "_" + "SIZE_XPATH"))
		restr17 = tree.xpath(self.ro.get(str(seed_id) + "_" + "MAINIMAGEURL_XPATH"))

		restr9  = self.util.tuplelist2list(restr9)
		restr17 = self.util.tuplelist2list(restr17)
		restr17 = self.util.getfirstnullelement(restr17)
		restr9  = self.util.get_url_from_strings(restr9)
		restr9  = self.util.convt2compurl(url, restr9)
		restr17 = self.util.convt2compurl(url, restr17)
		productpageitem['title']			 = "".join(self.util.tuplelist2list(restr3))
		#productpageitem['imageamount']		 = len(restr9)
		productpageitem['imageamount']		 = -1
		productpageitem['brand']			 = "".join(self.util.tuplelist2list(restr11))
		productpageitem['desc']				 = "".join(self.util.tuplelist2list(restr12))
		productpageitem['productid']		 = "".join(self.util.tuplelist2list(restr13))
		productpageitem['color']			 = "".join(self.util.tuplelist2list(restr14))
		productpageitem['price']			 = "".join(self.util.tuplelist2list(restr15))
		productpageitem['size']				 = "".join(self.util.tuplelist2list(restr16))
		productpageitem['mainimageurl']		 = "".join(restr17)
		try:
			productpageitem['pagetext']			 = "".join(tree.xpath('//*[not(self::a | self::style | self::script | self::head | self::img | \
																		self::noscript | self::form | self::option)]/text()'))
		except:
			productpageitem['pagetext']      = 'no desc'
	
		productpageitem['producturl']		 = response_url
		productpageitem['crawlerseedurl_id'] = seed_id
	
		print urljson
		print "------- ", str(restr9)
		#print "------- ", str(restr17)

		# Check proudct page is existed: insert or update
		if restr17:
			productpageitem['mainimageid'] = 0
			productpageitem['category']    = category
			page_re = self.datasyn.insertproductpage_on_duplicate(productpageitem)
			if page_re > 0:
				img_re = self.datasyn.insertimage_on_duplicate(productpageitem['mainimageurl'], response_url, page_re)
		elif restr3 and restr15:
			self.ro.lpush(self.nothrow_urljson_list, urljson)

	#def parse(self):
	def run(self):
		print self.getName() + " start run"
		#self.datasyn.read_seeds(3)
		while self.running:
			urljson   = self.get_urljson(self.nothrow_urljson_list)
			if urljson is not None:
				#print urljson
				urljson  = eval(urljson)
				if urljson['pagetype'] == 3:
					self.parse_product_listpage(urljson)
				elif urljson['pagetype'] == 31:
					self.parse_product_detailpage(urljson['url'], urljson['seed_id'], urljson['depth'], urljson['category'], urljson)

	def stop(self):
		#self.thread_stop = True
		pass

def runcrawler():
	scs = []
	for i in range(12):
		scs.append(ProductCrawler("T" + str(i), i))
	for i in range(12):
		scs[i].start()
		time.sleep(2)
	for i in range(12):
		scs[i].join()
	for i in range(12):
		scs[i].stop()

#from Watcher import Watcher
def main(script, flag='with'):

	ro.deletedb(product_nothrow_urljson_list, product_page_url_set, delete_level)
	time.sleep(3)
	init_datasyn = DataSynch(mysql_pool, ro, 100)
	init_datasyn.read_seeds(3)
	time.sleep(5)

	if flag == 'with':
		Watcher()
	elif flag != 'without':
		print 'unrecognized flag: ' + flag
		sys.exit()

if __name__ == '__main__':
	main(*sys.argv)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                    productmysqloperation.py                                                                            0000664 0000771 0000772 00000021524 12032300434 015773  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                #!/bin/python
# -*- coding: utf-8 -*-
#This file is to define the mysql operations
#
# Only Provide the getconn service
# Project: Weardex.com
# Author : Zhang Weidong
# Class  : DataSynch
#
# Provide the following interface and inplement
#   1.__init__(self)
#   2.getconn(self)
#   3.set(self, key, value)
#   4.get(self)
#   5.sadd(self, userset, uservalue)
#   .....
###############################################
import re
import redis

from redisoperation import RedisOperation
from util import Util

import chardet
import urllib2
import httplib
import hashlib
import time
from urlparse import urlparse
import ImageFile

#from scrapy.conf import settings
from settings import Settings
from scrapy import log

from PIL import Image
import glob, os, socket
from datetime import datetime, date
import logging
import simplejson
import json

import MySQLdb as mdb
from DBUtils.PooledDB import PooledDB
import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')
# timeout in seconds
timeout = 180
socket.setdefaulttimeout(timeout)

# create logging
now = datetime.now()
nowstr = str(now.year) + str(now.month) + str(now.day) + str(now.hour)
logger = logging.getLogger('crawler')
hdlr = logging.FileHandler('productcrawler-' + nowstr + '.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)

class DataSynch(object):
	
	# Special characters which needed to be replace by blackspace
	punctuations_to_erase = ['`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', \
							'+', '-', '=', '{', '[', '}', ']', '|', ':', ';', '\'', '"', '<', ',', '>', '.', '/', '$', '\\', '•']

	# Get mysqldb/redis host info
	settings					= Settings()
	mysql_host					= settings.mysqlsettings['host']
	mysql_port					= settings.mysqlsettings['port']
	mysql_user					= settings.mysqlsettings['user']
	mysql_passwd				= settings.mysqlsettings['passwd']
	mysql_db					= settings.mysqlsettings['db']
	mysql_charset				= settings.mysqlsettings['charset']

	# Datastures stored in redis
	blog_page_url_set            = settings.blogcrawlersettings['page_url_set']
	blog_nothrow_urljson_list	 = settings.blogcrawlersettings['nothrow_urljson_list']
	product_page_url_set         = settings.productcrawlersettings['page_url_set']
	product_nothrow_urljson_list = settings.productcrawlersettings['nothrow_urljson_list']

	# Save image folder path
	original_image_folder		= settings.foldersettings['original_image_folder']

	# Init the connections to mysqldb/redis
	#pool = PooledDB( creator = mdb, mincached = 5, db = mysql_db, host = mysql_host, user = mysql_user, passwd= mysql_passwd, charset = "utf8", use_unicode = True)
	#ro = RedisOperation()
	util = Util()

	def __init__(self, mysql_pool, ro, connection_no):
		self.pool = mysql_pool
		self.ro   = ro
		self.mysql_conn = self.pool.connection(connection_no)
		self.cur = self.mysql_conn.cursor()
		self.logger = logger

	# Save original image directly
	def save_original_image(self, im, new_im_path):
		try:
			if im.mode != "RGB":
   				im = im.convert("RGB")
			im.save(new_im_path, "JPEG",quality=100)
			return 1
		except Exception, e:
			self.logger.error('save original image failed...error:%s'%(str(e)))
			return 0

	def read_seeds(self, seed_type):
		#print self.getName() + " - read seeds from mysql..."
		try:
			if seed_type == 3:
				self.cur.execute("select url, id, SELLER_URL, CATEGORY, TITLE_XPATH, DETAILURL_XPATH, DETAILURLHEADER, NEXTPAGE_XPATH, NEXTPAGEHEADER, IMAGESOURCE, IMAGEURL_XPATH, IMAGEURLHEADER, BRAND_XPATH, DESCRIPTION_XPATH, PRODUCTID_XPATH, COLOR_XPATH, PRICE_XPATH, SIZE_XPATH, MAINIMAGEURL_XPATH, CATEGORY from crawlerseedurl where photosourcetype_id=3 and deleted=0 and webkit=0")
				rows = self.cur.fetchall()
				print "len(product seeds):" + str(len(rows))
				for row in rows:
					#print str(row[18])
					self.ro.set(str(row[1]) + "_SELLER_URL",         row[2])
					self.ro.set(str(row[1]) + "_CATEGORY",           row[3])
					self.ro.set(str(row[1]) + "_TITLE_XPATH",        row[4])
					self.ro.set(str(row[1]) + "_DETAILURL_XPATH",    row[5])
					self.ro.set(str(row[1]) + "_DETAILURLHEADER",    row[6])
					self.ro.set(str(row[1]) + "_NEXTPAGE_XPATH",	 row[7])
					self.ro.set(str(row[1]) + "_NEXTPAGEHEADER",     row[8])
					self.ro.set(str(row[1]) + "_IMAGESOURCE",        row[9])
					self.ro.set(str(row[1]) + "_IMAGEURL_XPATH",     row[10])
					self.ro.set(str(row[1]) + "_IMAGEURLHEADER",	 row[11])
					self.ro.set(str(row[1]) + "_BRAND_XPATH",        row[12])
					self.ro.set(str(row[1]) + "_DESCRIPTION_XPATH",  row[13])
					self.ro.set(str(row[1]) + "_PRODUCTID_XPATH",    row[14])
					self.ro.set(str(row[1]) + "_COLOR_XPATH",        row[15])
					self.ro.set(str(row[1]) + "_PRICE_XPATH",        row[16])
					self.ro.set(str(row[1]) + "_SIZE_XPATH",         row[17])
					self.ro.set(str(row[1]) + "_MAINIMAGEURL_XPATH", row[18])
					if row[0] != None and row[0] != "" and row[1] != None and row[1] != "":
						seed_url = re.sub(r'\s', '', row[0])
						self.ro.set(row[1], seed_url)
						urljson = {'url': row[0], 'seed_id': row[1], 'depth':1, 'pagetype':3, 'category':row[19]}
						print urljson
						self.ro.check_lpush(self.product_page_url_set, hashlib.sha1(seed_url).hexdigest(), self.product_nothrow_urljson_list, urljson)
						#self.ro.lpush(self.product_nothrow_urljson_list, urljson)
						#self.ro.lpush(self.page_url_nothrow_list, req_url_seed)
						#self.ro.lpush(self.level_url_list + "1", req_url_seed)
		except mdb.Error, e:
			self.logger.error("Error %d:%s"%(e.args[0], e.args[1]))
			sys.exit()

	# insert image and download
	def	insertimage_on_duplicate(self, image_url, sourcepage_url, page_id):
		#print "get into insert_image function"
		import re
		if isinstance(image_url, unicode):
			itemsrc = image_url.encode('utf-8', 'ignore')
		else:
			itemsrc = image_url

		# note: the photourlhash is calcuted before the '/" was escaped
		photourlhash = hashlib.sha1(itemsrc).hexdigest()
		itemsrc = itemsrc.replace("'", "\\'")
		itemsrc = itemsrc.replace('"', '\\"')
		sourcepage_url = sourcepage_url.replace("'", "\\'")
		sourcepage_url = sourcepage_url.replace('"', '\\"')

		sql = ""
		try:
			#print "CCCCCCCCCC"
			sql = "insert ignore into photo(datepost, photosourcetype_id, photourl,  pageurl, photourlhash, photoinfo_id) value(now(), 3, '%s', '%s', '%s', %d) on duplicate key update datepost=now()"%(itemsrc , sourcepage_url, photourlhash, page_id)
			#print "DDDDDDDDD"
			re = self.cur.execute(sql)
			#self.mysql_conn.commit()
			#print "insert image re:%d"%re
			self.logger.info("Image insert re:%s"%(re))
			return self.cur.lastrowid
		except mdb.Error, e:
			self.logger.error("mysql insert image with download error, table: photocache, errormsg: %d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql insert image with download error, table: photocache, errormsg:%s - sql:%s"%(e, sql))
			return 0

	# synchronize blog pge info
	def insertproductpage_on_duplicate(self, item):
		pageurl_sh      = hashlib.sha1(item['producturl']).hexdigest()
		mainimageurl_sh = hashlib.sha1(item['mainimageurl']).hexdigest()

		for c in range(len(self.punctuations_to_erase)):
			item['title']	 = (item['title']).replace(self.punctuations_to_erase[c], ' ')
			item['brand']    = (item['brand']).replace(self.punctuations_to_erase[c], ' ')
			item['desc']     = (item['desc']).replace(self.punctuations_to_erase[c], ' ')
			item['price']    = (item['price']).replace(self.punctuations_to_erase[c], ' ')
			item['color']    = (item['color']).replace(self.punctuations_to_erase[c], ' ')
			item['size']     = (item['size']).replace(self.punctuations_to_erase[c], ' ')
			item['pagetext'] = (item['pagetext']).replace(self.punctuations_to_erase[c], ' ')
		
		item['title']		= (item['title']).replace("'", "\\'")
		item['title']		= (item['title']).replace('"', '\ "')
		item['producturl']	= (item['producturl']).replace("'", "\\'")
		item['producturl']	= (item['producturl']).replace('"', '\\"')

		try:
			sql = "insert into productphotoinfo(title, brand, description, price, color, size, producturl, imageamount, productid, mainimage_id, category, pageurlhash, dateinsert) values('%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, '%s', '%s', '%s', '%s', now()) on duplicate key update title=values(title), daterefresh=now()"%(item['title'], item['brand'], item['desc'], item['price'], item['color'], item['size'], item['producturl'], int(item['imageamount']), item['productid'], item['mainimageid'], item['category'], pageurl_sh)
			re = self.cur.execute(sql)
			self.mysql_conn.commit()
			if re >= 1:
				self.logger.info("Product insert re:%s"%(re))
			return self.cur.lastrowid
		except mdb.Error, e:
			self.logger.error("mysql update productpage error, table: productpage errormsg:%d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql insert productpage page error, table: productpage, errormsg:%s"%(e))
			return 0
                                                                                                                                                                            productsettings.py                                                                                  0000664 0000771 0000772 00000001267 12032252611 014552  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                import re

class Settings(object):

	mysqlsettings   = {'host':'productdemo.cz7savcjxymi.ap-southeast-1.rds.amazonaws.com', 'port':3306, 'user':'productdemo', 'passwd':'raulproductdemo', 'db':'productdemo', 'charset':'utf8'}

	redissettings   = {'host':'localhost', 'port':6379, 'db':3}

	productcrawlersettings = {'page_url_set':'product_page_url_set', 'nothrow_urljson_list':'product_nothrow_urljson_list', 'image_url_set':'product_image_url_set', \
								'seeds_set':'product_seeds_set', 'process_no':10, 'thread_no':12, 'init_level':3}

	foldersettings		   = {'original_image_folder':'/public2/ads/image_test/original_image_folder/'}

	crawlertype = {'crawlertype':2}

	depth_limit = 1000
                                                                                                                                                                                                                                                                                                                                         redisoperation.py                                                                                   0000644 0000771 0000772 00000012613 12030742522 014336  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                #This file is to define the redis operations
#
# Only Provide the getconn service
# Project: Weardex.com
# Author: Zhang Weidong
# Class: RedisOperation
#
# Provide the following interface and inplement
#	1.__init__(self)
#	2.getconn(self)
#	3.set(self, key, value)
#   4.get(self)
#   5.sadd(self, userset, uservalue)
#	.....
###############################################

from  datetime import datetime, date, time

#from scrapy.conf import settings
from settings import Settings
from scrapy import log
import redis

class RedisOperation(object):
	# Get the settings from the setting file
	#redis_host		 = settings['REDIS_HOST']
	#redis_port		 = settings['REDIS_PORT']
	#redis_db		 = settings['REDIS_DB']
	#redis_urlhash_db = 6
	settings         = Settings()
	
	#redis_host		 = settings.redissettings['host']
	#redis_port		 = settings.redissettings['port']
	#redis_db		 = settings.redissettings['db']
	# Constuctor
	def __init__(self, pool):
		# Get the start datetime
		now = datetime.now()
		nowstr = str(now.year) + str(now.month) + str(now.day) + str(now.hour)
		# Start the log
		log.start("redisoperation-" + nowstr + ".log")
		# Init the ConnectionPool and Connection
		self.pool = pool #redis.ConnectionPool(host=self.redis_host, port=self.redis_port, db=int(self.redis_db))
		self.conn = redis.Redis(connection_pool = self.pool)
		#self.urlhash_pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, db=int(self.redis_urlhash_db))
		#self.urlhash_conn = redis.Redis(connection_pool = self.urlhash_pool)

	# Connection Factory: connection provider
	def getconn(self):
		return self.conn
	
	# Get redis urlhash db connection
	#def geturlhashconn(self):
		#return self.urlhash_conn
	
	# Operate the redis through the following funcs is more
	# Safty than self.conn.sadd(...)
	def set(self, key, value):
		try:
			self.conn.set(key, value)
		except Exception:
			log.msg("set error, key:" + str(key) + ",value:" + value, level=log.ERROR)
	
	# Get the mapped valued of 'key' from redis
	def get(self, key):
		try:
			value = self.conn.get(key)
			if value != None:
				return value
			else:
				log.msg("get None, key:%s"%str(key), level=log.WARNING)
				return None
		except Exception:
			log.msg("get error, key:%s"%str(key), level=log.ERROR)
			return None
	
	# Add 'uservalue' to 'uesrset'
	def sadd(self, userset, uservalue):
		try:
			return self.conn.sadd(userset, uservalue)
		except Exception:
			log.msg("sadd error,set:" + userset + ",value:" + uservalue, level=log.ERROR)
			return 0
	
	# Push one member from 'userlist's front end
	def spop(self, userset):
		try:
			value = self.conn.spop(userset)
			if value != None:
				return value
			else:
				log.msg("spop None, set:" + userset, level=log.WARNING)
				return None
		except Exception:
			log.msg("spop error, set:" + userset, level=log.ERROR)
			return None

	# Push one member from 'userlist's tail end
	def lpush(self, userlist, uservalue):
		try:
			return self.conn.lpush(userlist, uservalue)
		except Exception:
			log.msg("lpush error,list:" + userlist + "value:" + uservalue, level=log.ERROR)
			return 0

	# Push one member from 'userlist's tail end
	def check_lpush(self, userset, usersetvalue, userlist, userlistvalue):
		try:
			re = self.sadd(userset, usersetvalue)
			if re == 1:
				return self.conn.lpush(userlist, userlistvalue)
			else:
				return 0
		except Exception:
			log.msg("lpush error,list:" + userlist + "value:" + userlistvalue, level=log.ERROR)
			return 0

	# Pop one member from 'userlist's tail end
	def lpop(self, userlist):
		try:
			value = self.conn.lpop(userlist)
			if value != None:
				return value
			else:
				log.msg("lpop error, list:" + userlist, level=log.WARNING)
				return None
		except Exception:
			log.msg("lpop error,list:" + userlist, level=log.ERROR)
			return None

	# Pop one member from 'userlist's front end
	def rpop(self, userlist):
		try:
			value = self.conn.rpop(userlist)
			if value != None:
				return value
			else:
				log.msg("rpop error, list:" + userlist, level=log.WARNING)
				return None
		except Exception:
			log.msg("rpop error,list:" + userlist, level=log.ERROR)
			return None
	
	# Pop one member from 'userset'
	def spop(self, userset):
		try:
			value = self.conn.spop(userset)
			if value != None:
				return value
			else:
				log.msg("spop error, list:" + userset, level=log.WARNING)
				return None
		except Exception:
			log.msg("spop error,list:" + userset, level=log.ERROR)
			return None

	# Check 'value' is in 'userset', if in return 1, or return 0
	def sismember(self, userset, value):
		try:
			re = self.conn.sismember(userset, value)
			if re == True:
				return 1
			else:
				return 0
		except Exception:
			log.msg("sismember error, set:" + userset + ",url:" + value, level=log.ERROR)
			return 0

	# Clear db
	def cleardb(self):
		try:
			# conn.flushdb(), if success return True, or return False
			re = self.conn.flushdb()
			if re:
				return 1
			else:
				return 0
		except Exception:
			log.msg("flushdb error", level=log.ERROR)
			return 0

	# level-0 do nothing, level-1 delete the list not set, level-2 delete all elements includes list and set
	def deletedb(self, listname, page_url_set, del_level):
		try:
			if del_level == 0:
				pass
			elif del_level == 1:
				self.conn.delete(listname)
			elif del_level == 2:
				self.conn.delete(listname, page_url_set)
			elif del_level == 3:
				self.cleardb()
			else:
				pass
		except Exception:
			log.msg("delete with level error", level=log.ERROR)
			return 0
                                                                                                                     settings.py                                                                                         0000664 0000771 0000772 00000005702 12030052170 013143  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                import re

class Settings(object):

	pattern = re.compile(r'.*(\d{4})[-/](0?[1-9]|1[012])[-/](0?[1-9]|[12]\d|3[01])[-/]$|.*(\d{4})[-/](0?[1-9]|1[012])[-/](0?[1-9]|[12]\d|3[01])$|.*(\d{4})[-/](0?[1-9]|1[012])$|.*(\d{4})[-/](0?[1-9]|1[012])[-/]$|.*(\?showComment).*|.*(/page/).*|.*(\?=p).*|.*(\?cat=).*|.*(\?=page).*|.*(_archive).*|.*(#).*|.*(/search\?).*|.*(search/label).*|.*(index\.).*|.*(\?paged=).*|.*(/category/).*|.*(january)$|.*(january)/$|.*(february)$|.*(february)/$|.*(march)$|.*(march)/$|.*(april)$|.*(april)/$|.*(may)$|.*(may)/$|.*(june)$|.*(june)/$|.*(july)$|.*(july)/$|.*(august)$|.*(august)/$|.*(september)$|.*(september)/$|.*(october)$|.*(october)/$|.*(november)$|.*(november)/$|.*(december)$|.*(december)/$')

	mysqlsettings   = {'host':'localhost', 'port':3306, 'user':'fashion', 'passwd':'lmsi3229fashion', 'db':'fashion4_ads_test_1', 'charset':'utf8'}

	redissettings   = {'host':'localhost', 'port':6379, 'db':1}

	crawlername     = ['BlogCrawler', 'ProductCrawler']
	"""
	crawlersettings = [{'page_url_set':'blog_page_url_set', 'nothrow_urljson_list':'blog_nothrow_urljson_list', 'image_url_set':'blog_image_url_set', \
								'process_no':25, 'thread_no':12},\
					   {'page_url_set':'product_page_url_set', 'nothrow_urljson_list':'product_nothrow_urljson_list', 'image_url_set':'product_image_url_set', \
								'process_no':25, 'thread_no':12},\
					   {'page_url_set':'form_page_url_set', 'nothrow_urljson_list':'form_nothrow_urljson_list', 'image_url_set':'form_image_url_set', \
								'process_no':25, 'thread_no':12}\
					  ]
	setlistsettings = {'page_url_set':'page_url_set', 'nothrow_urljson_list':'nothrow_urljson_list', 'image_url_set':'image_url_set'}
	"""

	"""
	Init_level: 1 read in seeds and del *_page_url_set
				2 del *_page_url_set, nothrow_urljson_list
				3 del *_page_url_set,  *_image_url_set, nothrow_urljson_list
				4 del all: *_page_url_set, *_image_url_set, *_seeds_set, nothrow_urljson_list
				5 read the seeds from redis' *_seeds_set
				6 do nothing, and go directly
	"""
	blogcrawlersettings    = {'page_url_set':'blog_page_url_set', 'nothrow_urljson_list':'blog_nothrow_urljson_list', 'image_url_set':'blog_image_url_set', \
								'seeds_set':'blog_seeds_set', 'process_no':20, 'thread_no':12, 'init_level':0}
	productcrawlersettings = {'page_url_set':'product_page_url_set', 'nothrow_urljson_list':'product_nothrow_urljson_list', 'image_url_set':'product_image_url_set', \
								'seeds_set':'product_seeds_set', 'process_no':10, 'thread_no':12, 'init_level':0}
	formcrawlersettings    = {'page_url_set':'form_page_url_set', 'nothrow_urljson_list':'form_nothrow_urljson_list', 'image_url_set':'form_image_url_set', \
								'seeds_set':'form_seeds_set', 'process_no':25, 'thread_no':12, 'init_level':0}
	
	crawlersettings        = [blogcrawlersettings, productcrawlersettings]

	foldersettings		   = {'original_image_folder':'/public2/ads/image_test/original_image_folder/'}

	crawlertype = {'crawlertype':2}
	depth_limit = 1000

	seeds = {}
                                                              test_lxml_1.py                                                                                      0000664 0000771 0000772 00000000666 12027737477 013574  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                import lxml.html
import mysqloperation


#self.datasyn = DataSynch(thread_no)
def main():
        
    seedurl_list = file('seedurls.file').readlines()
    for url in seedurl_list:
        if url is not None and url != "":
            print "- " + url
            try:
                tree = lxml.html.parse(url)
                #tag  = tree.xpath('//a/@href')
                #print tag
            except:
                pass


main()
                                                                          test_lxml.py                                                                                        0000664 0000771 0000772 00000001313 12027737477 013342  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                import lxml.html
import mysqloperation


#self.datasyn = DataSynch(thread_no)
class Test(threading.Thread):

    def __init__(self):
		threading.Thread.__init__(self,name=threadname)

    def test(self):
        seedurl_list = file('seedurls.file').readlines()
        for url in seedurl_list:
            if url is not None and url != "":
                print "- " + url
                try:
                    tree = lxml.html.parse(url)
                    #tag  = tree.xpath('//a/@href')
                    #print tag
                except:
                    pass
    def run(self):
        while True:
            self.test()

    def stop(self):
        pass

tt = Test()
tt.start()
tt.join()
tt.stop()
                                                                                                                                                                                                                                                                                                                     test_sigterm.py                                                                                     0000664 0000771 0000772 00000002270 12030222715 014015  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                import threading
import time
import signal
import os
import sys

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
			os.kill(self.child, signal.SIGINT)
		except OSError: pass

class Test(threading.Thread):

	def __init__(self, threadname):
		threading.Thread.__init__(self,name=threadname)
		signal.signal(signal.SIGINT, self.sigint_handle)
		self.running = True

	def sigint_handle(self, signum, frame):
		print "get sigterm signal"
		self.running = False

	def run(self):
		while self.running:
			print "1"
			time.sleep(1)
			print "2"
			time.sleep(1)
			print "3"
			time.sleep(1)
			print "4"
			time.sleep(1)
			print "5"
			time.sleep(1)


def main(script, flag='with'):
	if flag == 'with':
		Watcher()
	elif flag != 'without':
		print 'unrecognized flag: ' + flag
		sys.exit()

	tt = Test('thread - 1')
	tt.start()
	tt.join()
	tt.stop()

if __name__ == '__main__':
	main(*sys.argv)
                                                                                                                                                                                                                                                                                                                                        test_thread.py                                                                                      0000664 0000771 0000772 00000046667 12027737477 013662  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                #!/bin/python
# -*- coding: utf-8 -*-
from selenium import selenium
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys

#import MySQLdb as mdb
#from sqlobject import *
#from sqlobject.sqlbuilder import *
#from Items import Person
#from Items import Productpage
#from items import BlogpageItem
from redisoperation import RedisOperation
from mysqloperation import DataSynch
from util import Util

import sys, os, re, operator, datetime, time, signal, threading, gc
import hashlib, urlparse
import urllib, cookielib
import urllib2
from urllib2 import Request
import encodings.idna

from BeautifulSoup import BeautifulSoup as Soup
import lxml.html
#import xml.dom.minidom
#import xpath
#from xml.dom.ext.reader import Sax2
#from xml import xpath
#from elementtree.ElementTree import XML, fromstring, tostring
from guppy import hpy

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
		#sys.exit()

	def kill(self):
		try:
			print 'clear finish and exit...'
			os.kill(self.child, signal.SIGKILL)
		except OSError: pass

class SelCrawler(threading.Thread):

	running   = True
	settings  = Settings()
	gc_counter = 0

	def __init__(self, threadname, thread_no):
		threading.Thread.__init__(self,name=threadname)
		signal.signal(signal.SIGINT, self.sigint_handle)
		# Dynamic loading webdriver
		#self.browser = webdriver.Firefox() # Get local session of firefox
		self.depth_limit          = self.settings.depth_limit
		self.url_set              = self.settings.blogcrawlersettings["page_url_set"]
		self.image_url_set        = self.settings.blogcrawlersettings["image_url_set"]
		self.nothrow_urljson_list = self.settings.blogcrawlersettings["nothrow_urljson_list"]
		self.delay_time           = 0.25
		self.datasyn			  = DataSynch(thread_no)
		self.ro					  = RedisOperation()
		self.util				  = Util()

		# Clear redis set
		#self.ro.cleardb()
				
	def __del__(self):
		#self.browser.close()
		pass

	def sigint_handle(self, signum, frame):
		self.running = False
		#self.logger.info("Catch SINGINT interrupt signal...")

	def init_delay_flag(self, domain_sh):
		self.ro.set(domain_sh, 0)

	def reset_delay_flag(self, domain_sh):
		self.ro.set(domain_sh, 0)

	def wait_delay_ok(self, domain_sh):
		while self.ro.get(domain_sh) == 1:
			time.sleep(0.05)
		return 1

	# Set delay_flag to 1 after accessed on domain
	# Set delay_flag to 0 after delay_time
	def set_delay_timer(self, domain_sh):
		self.ro.set(domain_sh, 1)
		t = threading.Timer(self.delay_time, self.reset_delay_flag, [domain_sh])
		t.start()

	def get_urljson(self, urljson_list):
		# Wait for more urljson
		wait_amount = 0
		while self.ro.conn.llen(urljson_list) <= 0:
			print "wait 3 secs"
			time.sleep(3)
			wait_amount += 1
			if wait_amount > 3:
				self.running = False
				return None

		while self.ro.conn.llen(urljson_list) > 0:
			#print self.getName() + " - " + str(self.ro.conn.llen(urljson_list))
			return self.ro.rpop(urljson_list)

	def parse_blogpage(self, htmlbody, url, seed_id):
		#soup = Soup(htmlbody)
		#content = soup.findAll(re.compile(r'[^a|^style|^script|^head|^img|^noscript|^form|^option]'))
		#print content
		#doc = xml.dom.minidom.parseString("<a href='www.google.com'>aaaaaa</a>")
		#print htmlbody
		elem = fromstring(htmlbody)
		text = tostring(elem)
		doc = xml.dom.minidom.parseString(text)
		elements =  xpath.find('//a', doc)
		for el in elements:
			print el.getAttribute('href')

	# Extract urls from page-detail
	def extract_productdetailpageurls(self, urljson, seed_id):
		#print "in extracturls function"
		try:
			tree = lxml.html.parse(urljson['url'])
		except:
			return -1
		# Depth surpass the limit, then exit
		if urljson['depth'] >= self.depth_limit:
			return None
		#print self.ro.get(str(seed_id) + "_" + "DETAILURL_XPATH")  + " - " +  self.ro.get(str(seed_id) + "_" + "NEXTPAGE_XPATH")
		urls = hxs.select(self.ro.get(str(seed_id) + "_" + "DETAILURL_XPATH")).extract()
		print "detailpage urls num:", len(urls)
		#print str(urls)
		#urls  = re.findall(self.ro.get(str(seed_id) + "_" + "DETAILURL_XPATH"), pagebody)
		log.msg(str(len(urls)), level=log.INFO)
		#restr6  = re.findall(self.ro.get(str(seed_id) + "_" + "NEXTPAGE_XPATH"),  pagebody)
		restr6 = hxs.select(self.ro.get(str(seed_id) + "_" + "NEXTPAGE_XPATH")).extract()
		print "list page url num:", len(restr6)
		#print str(restr6)
		log.msg(str(len(restr6)), level=log.INFO)

		seed_url = self.ro.get(urljson['seed_id'])
		self.appendnothrowurllist(urls, urljson['url'], seed_url, self.util.getcompleteurl(seed_url), urljson['seed_id'], urljson['depth'], urljson['pagetype'])
		self.appendnothrowurllist(urls, urljson['url'], seed_url, self.util.getcompleteurl(seed_url), urljson['seed_id'], urljson['depth'], urljson['pagetype'])
		
	def extract_product_detailpage(self):
		print "get into extract_product detaile function..."
		try:
			title		          = (self.browser.find_element_by_xpath("//h1[@class='parseasinTitle ']/span")).text
			imageurl		      = (self.browser.find_element_by_id('main-image')).get_attribute('ref')
			brand		          = (self.browser.find_element_by_xpath("//span[@class='brandLink']/a")).text
			description_elements  = self.browser.find_elements_by_xpath("//div[@class='content']//span")
			color_elements        = self.browser.find_elements_by_xpath("//b[@class='variationLabel'] | //b[@class='variationDefault']/..")
			price		          = (self.browser.find_element_by_xpath("//span[@class='priceLarge']")).text
			size_elements         = self.browser.find_elements_by_xpath("//select[@id='size_name']/option")
			mainimageurl		  = (self.browser.find_element_by_xpath("//img[@id='main-image']")).get_attribute('ref')
		except:
			pass
		desc  = ""
		color = ""
		size  = ""
		for des_el in description_elements:
			desc.join(des_el.text)
		for color_el in color_elements:
			color.join(color_el.text)
		for size_el in size_elements:
			size.join(size_el.text)
	
		print re.sub(r'\s', '', title)
		print re.sub(r'\s', '', brand)
		print re.sub(r'\s', '', imageurl)
		print re.sub(r'\s', '', price)
		print re.sub(r'\s', '', size)
		print re.sub(r'\s', '', color)
		print re.sub(r'\s', '', desc)
		print re.sub(r'\s', '', mainimageurl)
		#p = Productpage(title=title1, price=price1)

	def get_start_one(self, la):
		if la is not None and type(la) is list and len(la) > 0:
			return la[0]
		else:
			return ""

	def join_list(self, la):
		if la is not None and type(la) is list and len(la) > 0:
			return ' '.join(la)
		else:
			return ''
	
	def extract_blog_image(self, tree, urljson, blogpage_id):
		print "extract image..."
		elements = tree.xpath("//img")
		for element in elements:
			imgitem = {}
			if element.xpath('name()') == 'img' and element.xpath('@src') is not None and len(element.xpath('@src')) > 0:
				#print element.xpath('name()') + " - " + element.xpath('@src')[0]
				imgitem['src']          = self.util.concat_image_url(urljson['url'], element.xpath('@src')[0])
				#print imgitem['src']
				imgitem['desc']         = self.get_start_one(element.xpath('@alt'))
				imgitem['postdate']     = datetime.datetime.now().date().isoformat()
				imgitem['sourcetypeid'] = 2
				# Insert this image
				image_url_sh = hashlib.sha1(imgitem['src']).hexdigest()
				#if self.ro.sadd(self.image_url_set, image_url_sh) > 0:
					#image_id = self.datasyn.insert_image_with_download(imgitem, 1)
					#if image_id > 0:
						#self.datasyn.insert_blogpage_photo_relationship_with_id(blogpage_id, image_id)
			#del imgitem

	# Append url to nothrow url list or level url list
	def appendnothrowurllist(self, urls, base_url, seed_url, seed_ext, seed_id, depth, page_type):
		#print str(urls)
		for uu in urls:
			if uu == None or uu == "":
				continue
			if type(uu) == tuple or type(uu) == list:
				urls.extend(uu)
				continue
			curl = urlparse.urljoin(base_url, uu)
			curl = self.util.normalize_url(curl)
			import re
			if curl is None or re.findall(r"#[0-9a-zA-Z]*$", curl):
				continue
			# If crawler type is blog-crawler, then all urls must start with seed_url
			if self. settings.crawlertype['crawlertype'] == 2 and (not curl.startswith(seed_url)) and (not curl.startswith(seed_ext)):
				continue
			# Exclude invalid urls
			if not(curl == None or curl == "") and not(curl.startswith('javascript:')) and not(curl.startswith('mailto:')) and not(('.jpg' in curl) or ('.png' in curl) or ('.gif' in curl) or  ('.gpeg' in curl) or  ('.bmp' in curl) or ('.tiff' in curl) or ('.pcx' in curl) or ('.tga' in curl) or ('facebook.com' in curl) or ('google.com' in curl) or ('twitter.com' in curl) or ('google.com' in curl)):
				page_url_sh = hashlib.sha1(curl).hexdigest()
				#saddre = self.ro.sadd(self.page_url_set,  page_url_sh)		# add into page_url_set
				urljson = {'url': curl, 'seed_id': seed_id, 'depth':depth + 1, 'pagetype':page_type}
				re = self.ro.check_lpush(self.url_set, page_url_sh, self.nothrow_urljson_list, urljson)
				#self.ro.lpush(self.level_url_list + str(depth + 1), req_url_seed)

	def parse_blog_page(self, urljson, base_url):
		url = urljson['url']
		seed_url = self.ro.get(urljson['seed_id'])
		# Find charset
		#charset_str = tree.xpath("//head/meta/@content")
		#charset = re.findall(r".*?charset=(.*)", charset_str[0])  #.find(r"charset='(.*?)'")
		#print charset_str[0]
		#print charset
		try:
			tree = lxml.html.parse(url)
			urls = tree.xpath("//a/@href")
			self.appendnothrowurllist(urls, urljson['url'], seed_url, self.util.getcompleteurl(seed_url), urljson['seed_id'], urljson['depth'], urljson['pagetype'])
			title = tree.xpath("//title/text()")
			tagstxts = tree.xpath('//*[not(self::a | self::style | self::script | self::head | self::img | self::noscript | self::form | self::option)]/text()')
		except:
			return -1
		text = ""
		for tagtxt in tagstxts:
			text += re.sub(r'\s', '', tagtxt)
		#print self.util.standardize_url(base_url, url)
		match = self.settings.pattern.match(url)
		if match == None:
			isdetailed = 0
		else:
			isdetailed = 1

		blogpageitem = {}
		blogpageitem['title']			  = self.get_start_one(title)
		blogpageitem['blogurl']			  = self.util.standardize_url(base_url, url)
		blogpageitem['blogdetail']		  = text
		blogpageitem['crawlerseedurl_id'] = urljson['seed_id']
		blogpageitem['detailed']		  = isdetailed

		blogpage_url_sh = hashlib.sha1(blogpageitem['blogurl']).hexdigest()
		# Insert blogpageitem into msyql
		#blogpage_id = self.datasyn.insertblogpage_on_duplicate(blogpageitem)
		#if blogpage_id > 0:
			#self.datasyn.insertblogpageseedrelation_with_ignore(blogpage_id, urljson['seed_id'])
			#self.extract_blog_image(tree, urljson, blogpage_id)
		#self.extract_blog_image(tree, urljson, -1)
		
		del blogpageitem
		del tree
		del urls
		del title
		del tagstxts


	def parse_product_page(self, urljson):
		url = urljson['url']
		productpageitem = {}
		tree = lxml.html.parse(url)
		product_images                       = tree.xpath("//img[@class='mainImage']/@src |//img[@class='border']/@onclick")
		productpageitem['title']             = self.join_list(tree.xpath("substring-before(substring-after(//title/text(),  '-'),  '-')"))
		productpageitem['brand']             = self.join_list(tree.xpath("//div[@class='pdp-item-container']//h1/text()"))
		productpageitem['desc']              = self.join_list(tree.xpath("//div[@class='productCopy-container']//li/text()"))
		productpageitem['price']             = self.join_list(tree.xpath("//span[@class='product-price']/text()"))
		productpageitem['color']             = self.join_list(tree.xpath("//div[@class='pdp-item-container']//select/option/text()"))
		productpageitem['size']				 = self.join_list(tree.xpath("//div[@class='pdp-item-container']//select/option/text()"))
		productpageitem['producturl']		 = url
		productpageitem['imageamount']		 = len(product_images)
		productpageitem['crawlerseedurl_id'] = urljson['seed_id']
		productpageitem['productid']         = self.join_list(tree.xpath("//div[@class='pdp-item-container']//span[@class='product-price']/../text()"))
		productpageitem['pagetext']          = self.join_list(tree.xpath('//*[not(self::a | self::style | self::script | self::head | self::img | self::noscript | self::form | self::option)]/text()'))

		print str(productpageitem)

	# Extract product detail page info
	def parse_product_detailpage(self, url, response_url, seed_id, depth):
		#print "in parse_productdetailpage function=========="
		try:
			tree = lxml.html.parse(url)
		except:
			return -1
		productpageitem = {}
		page_url_sh     = hashlib.sha1(response_url).hexdigest()
		#fi = open(str(response_url_sh), 'w')
		#fi.write(pagebody)
		
		restr3  = tree.xpath(self.ro.get(str(seed_id) + "_" + "TITLE_XPATH")).extract()       
		restr9  = tree.xpath(self.ro.get(str(seed_id) + "_" + "IMAGEURL_XPATH")).extract()
		restr11 = tree.xpath(self.ro.get(str(seed_id) + "_" + "BRAND_XPATH")).extract()       
		restr12 = tree.xpath(self.ro.get(str(seed_id) + "_" + "DESCRIPTION_XPATH")).extract() 
		restr13 = tree.xpath(self.ro.get(str(seed_id) + "_" + "PRODUCTID_XPATH")).extract()   
		restr14 = tree.xpath(self.ro.get(str(seed_id) + "_" + "COLOR_XPATH")).extract()       
		restr15 = tree.xpath(self.ro.get(str(seed_id) + "_" + "PRICE_XPATH")).extract()       
		restr16 = tree.xpath(self.ro.get(str(seed_id) + "_" + "SIZE_XPATH")).extract()        
		restr17 = tree.xpath(self.ro.get(str(seed_id) + "_" + "MAINIMAGEURL_XPATH")).extract()

		restr9  = self.util.tuplelist2list(restr9)
		restr17 = self.util.tuplelist2list(restr17)
		restr17 = self.util.getfirstnullelement(restr17)
		restr9  = self.util.get_url_from_strings(restr9)
		restr9  = self.util.convt2compurl(url, restr9)
		restr17 = self.util.convt2compurl(url, restr17)
		productpageitem['title']			 = "".join(self.util.tuplelist2list(restr3))
		#productpageitem['imageamount']		 = len(restr9)
		productpageitem['imageamount']		 = -1
		productpageitem['brand']			 = "".join(self.util.tuplelist2list(restr11))
		productpageitem['desc']				 = "".join(self.util.tuplelist2list(restr12))
		productpageitem['productid']		 = "".join(self.util.tuplelist2list(restr13))
		productpageitem['color']			 = "".join(self.util.tuplelist2list(restr14))
		productpageitem['price']			 = "".join(self.util.tuplelist2list(restr15))
		productpageitem['size']				 = "".join(self.util.tuplelist2list(restr16))
		productpageitem['mainimageurl']		 = "".join(restr17)
		productpageitem['pagetext']			 = "".join(tree.xpath('//*[not(self::a | self::style | self::script | self::head | self::img | self::noscript | self::form | self::option)]/text()').extract())
		productpageitem['producturl']		 = response_url
		productpageitem['crawlerseedurl_id'] = seed_id

		print "------- ", str(restr9)
		print "------- ", str(restr17)

		# Check proudct page is existed: insert or update
		page_re = 0
		if self.datasyn.check_product_exists(page_url_sh) == 1:
			page_re = self.datasyn.updateproductpage(productpageitem)
		else:
			page_re = self.datasyn.insertproductpage(productpageitem)

		# Check [productpage, seed] is existed: insert or update
		if self.datasyn.check_productpageseedrelation_exists(page_url_sh, seed_id) == 1:
			pass
		else:
			self.datasyn.insertproductpageseedrelation(page_url_sh, seed_id)

		# Extract image info and cache image/[imageurl, pageurl] info
		imageitem    = ImageItem()
		image_amount = 0
		for imageurl in restr9:
			imageitem['src']          = imageurl
			imageitem['desc']         = "".join(self.util.tuplelist2list(restr12))
			imageitem['sourcepage']   = response_url
			imageitem['sourcetypeid'] = 3
			imageitem['postdate']     = ""
			image_url_sh = hashlib.sha1(imageurl).hexdigest()
			saddre = self.ro.sadd(self.image_url_set, image_url_sh)
			if saddre == 1:
				self.image_url_amount += 1

			pirline = json.dumps([imageurl, response_url])
			# Check image is existed: insert or update
			image_re = 0
			if self.datasyn.check_image_exists(image_url_sh) == 1:
				pass
			else:
				image_re = self.datasyn.insert_image_with_download(imageitem, 0)
				if image_re == 1:
					image_amount += 1
					# Set the first downloaded image as the page's main image
					if image_amount == 1:
						mainimage_sh = image_url_sh

			# Check Relationship is existed: insert relation after insert image and page successfully
			if page_re == 1 and image_re == 1 and self.datasyn.check_relation_exists(image_url_sh, page_url_sh) == 1:
				pass
			elif page_re == 1 and image_re == 1 and self.datasyn.check_relation_exists(image_url_sh, page_url_sh) == 0:
				self.datasyn.insert_productpage_photo_relationship_sh(image_url_sh, page_url_sh)
		# Update page's mainimage info and image amount
		if image_amount > 0:
			self.datasyn.updateproductpage_simple(page_url_sh, image_amount, mainimage_sh)
		else:
			# If process page failed, then push this page to the last layer queue
			if self.ro.lpush(self.failed_page_url_set_1, hashlib.sha1(response_url).hexdigest()) != 0 and self.ro.lpush(self.failed_page_url_set_2, hashlib.sha1(response_url).hexdigest()) != 0 and self.ro.lpush(self.failed_page_url_set_3, hashlib.sha1(response_url).hexdigest()) != 0:
				req_url_seed = {'url': response_url, 'seed_id': seed_id, 'depth':depth, 'page_type':self.pagetype['productdetailpage']}
				self.ro.lpush(self.page_url_nothrow_list, req_url_seed)
				self.ro.lpush(self.level_url_list + str(self.depth_limit), req_url_seed)
			else:
				self.ro.lpush("failed_page_url", [response_url, restr9, restr17])

	def parse_page(self, urljson):
		try:
			if urljson['pagetype'] == 2:
				self.parse_blog_page(urljson, urljson['url'])
				#self.extract_image(hxs, base_url, response.url)
			elif urljson['pagetype'] == 3:
				self.extract_productdetailpageurls(urljson, seed_id)
				#self.parse_product_page(urljson)
			elif urljson['pagetype'] == 31:
				self.parse_product_detailpage(urljson['url'], urljson['url'], urljson['seed_id'], urljson['depth'])
				#self.parse_product_page(urljson)
			else:
				print "error"
		except:
			pass

	#def parse(self):
	def run(self):
		print self.getName() + " start run"
		pid = os.getpid()
		p = psutil.Process(pid)
		print pid
		#self.datasyn.read_seeds(2)
		while self.running:
			#print p.get_memory_info()
			#time.sleep(3)
			urljson   = self.get_urljson(self.nothrow_urljson_list)
			if urljson is not None:
				urljson  = eval(urljson)
				#print self.getName() + " - " + str(urljson)
				self.parse_page(urljson)
				print "gc collect:%d"%(gc.collect())
				#html_body = self.get_htmlbody(urljson)
				#self.parse_html(html_body, urljson['pagetype'], urljson['jsmw'], urljson['depth'], urljson['url'], urljson['seed_id'])
			#self.gc_counter += 1
			#if self.gc_counter % 10 == 0:
				#print "gc collect:%d - %d"%(gc.collect(), len(gc.garbage))
				#print hpy().heap()
				#print "gc collect:%d"%(gc.collect())

	def stop(self):
		#self.thread_stop = True
		#self.browser.close()
		pass


#def main(script, flag='with'):
	#scs = SelCrawler("T" + str(1), 1)
	#scs.start()
	#scs.join()
	#scs.stop()
def main(script, flag='with'):
	if flag == 'with':
		Watcher()
	elif flag != 'without':
		print 'unrecognized flag: ' + flag
		sys.exit()

	#ro = RedisOperation()
	#ro.cleardb()
	#init_datasyn = DataSynch(100)
	#init_datasyn.read_seeds(2)

	#time.sleep(10)
	#scs = SelCrawler("T" + str(1), 1)
	#scs.start()
	#scs.join()
	#scs.stop()

	scs = []
	for i in range(12):
		scs.append(SelCrawler("T" + str(i), i))
	for i in range(12):
		scs[i].start()
	for i in range(12):
		scs[i].join()
	for i in range(12):
		scs[i].stop()


if __name__ == '__main__':
	main(*sys.argv)
                                                                         util.py                                                                                             0000664 0000771 0000772 00000012522 12030320203 012251  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                #!/bin/python
# -*- coding: utf-8 -*-
from urlparse import urlparse
import re
#import urlparse

class Util(object):

	def find_domain(self, url):
		pos = url.find('/')
		if pos == -1:
		   pos = url.find('?')
		   if pos == -1:
			   return url
		url = url[0:pos]
		return url

	def get_domain(self, url):
		if url is None:
			return url
		# Process stardard urls
		if url.startswith('http://') or url.startswith('https://'):
			parts = urlparse(url)
			return parts.netloc
		# Process nostartd urls
		else:
			return self.find_domain(url)
	
	def standardize_url(self, base_url, raw_url):
		if raw_url == None or raw_url == "":
			return raw_url
		import urlparse
		url = urlparse.urljoin(base_url, raw_url)
		if url.startswith("http://www") or url.startswith("wwww") or url.startswith("https://www"):
			pass
		else:
			url = url.replace("//", "//www", 1)
		return url
	
	def concat_image_url(self, base_url, raw_url):
		if raw_url == None or raw_url == "" or raw_url.startswith("http://") or raw_url.startswith("https://"):
			return raw_url
		import urlparse
		url = urlparse.urljoin(base_url, raw_url)
		if url.startswith("http://www") or url.startswith("wwww") or url.startswith("https://www"):
			pass
		else:
			url = url.replace("//", "//www", 1)
		return url

	def getcompleteurl(self, incompleteurl):
		if incompleteurl.startswith("http://www") or incompleteurl.startswith("wwww") or incompleteurl.startswith("https://www"):
			return incompleteurl
		else:
			return incompleteurl.replace("//", "//www", 1)

	# Get urls from strings
	def get_url_from_strings(self, strs):
		substr_re = r"'(.*?)'"
		newurllist = []
		if type(strs) == tuple or type(strs) == list:
			for ss in strs:
				subs = re.findall(substr_re, ss)
				if len(subs) > 0:
					#strs.remove(ss)
					newurllist.append(subs[0])
				else:
					newurllist.append(ss)
		else:
			subs = re.findall(substr_re, strs)
			if len(subs) > 0:
				newurllist = subs
			else:
				newurllist.append(strs)
		return sorted(set(newurllist), key=newurllist.index)

	# Get the first no null element in a list
	def getfirstnullelement(self, ll):
		singlelist = []
		if len(ll) <= 0:
			return singlelist
		for l in ll:
			if l != None and l != "":
				singlelist.append(l)
				return singlelist
		return singlelist

	# convert embeded tuple or list to no-embed list
	def tuplelist2list(self, tl):
		newlist = []

		if len(tl) <= 0:
			return newlist
		for el in tl:
			if type(el) == tuple or type(el) == list:
				#el = re.sub(r'^\s', '', el)
				#el = re.sub(r'\s$', '', el)
				newlist.extend(el)
			else:
				newlist.append(el)
		# Remove duplicated element
		newlist = sorted(set(newlist), key=newlist.index)
		return filter(None, newlist)
	
	# Convert url or url-list to complete url
	def convt2compurl(self, base_url, urls):
		import re
		import urlparse
		if type(urls) == tuple or type(urls) == list:
			newurllist = []
			for url in urls:
				url = re.sub(r'^\s+', '', url)
				url = re.sub(r'\s+$', '', url)
				newurl = urlparse.urljoin(base_url, url)
				newurl = self.normalize_url(newurl)
				newurllist.append(newurl)
			return newurllist
		else:
			url = re.sub(r'^\s', '', url)
			url = re.sub(r'\s$', '', url)
			newurl = urlparse.urljoin(base_url, urls)
			return self.normalize_url(newurl)

	def get_start_one(self, la):
		if la is not None and type(la) is list and len(la) > 0:
			return la[0]
		else:
			return ""


	def normalize_url(self, curl):
		if not(curl):
			return curl

		# Converting the scheme and host to lower case
		# HTTP://www.Example.com/ → http://www.example.com/
		# Shopbop sense this
		# curl = curl.lower()

		# get rid of end's "/"
		if curl.endswith("/"):
			curl = curl[0:-1]

		return curl

		# Capitalizing letters in escape sequences. 
		# All letters within a percent-encoding triplet (e.g., "%3A") are case-insensitive, and should be capitalized. Example:
		# http://www.example.com/a%c2%b1b → http://www.example.com/a%C2%B1b 

		# Removing dot-segments. The segments “..” and “.” are usually removed from a URL according to 
		# the algorithm described in RFC 3986 (or a similar algorithm). Example:
		# http://www.example.com/../a/b/../c/./d.html → http://www.example.com/a/c/d.html 

		# Removing the default port. The default port (port 80 for the “http” scheme) may be removed from (or added to) a URL. Example:
		# http://www.example.com:80/bar.html → http://www.example.com/bar.html 

#uu = Util()
#print uu.get_domain("www.baidu.com")
#print uu.get_domain("www.baidu.com.cn/asdf/asdf")
#print uu.get_domain("ishare.iask.sina.com.cn")
#print uu.get_domain("www1.baidu.com.cn")
#print uu.get_domain("wwww.baasdfds.sdf.asdfasdf.asdfasdf.asdfasdf?a=b&b=c/asdfasdf.asp")
#print uu.get_domain("http://wwww.google.sdf.asdfasdf.asdfasdf.asdfasdf?a=b&b=c/asdfasdf.asp")
#print uu.get_domain("https://wwww.google.sdf.asdfasdf.asdfasdf.asdfasdf?a=b&b=c/asdfasdf.asp")
#print uu.get_domain("https://wwww1.google.sdf.asdfasdf.asdfasdf.asdfasdf?a=b&b=c/asdfasdf.asp")
#print uu.get_domain("https://wwwwa.google.sdf.asdfasdf.asdfasdf.asdfasdf?a=b&b=c/asdfasdf.asp")
#print "-----------------"
#print uu.standardize_url("http://www.baidu.com", "a.html")
#print uu.standardize_url("http://www.baidu.com/b.html", "c.html")
#print uu.standardize_url("http://www.baidu.com/a/b/c.html", "d.html")
#print uu.standardize_url("http://www.baidu.com?a=1&b=2", "a.html")
#print uu.standardize_url("http://www.baidu.com/a.html?a=1&b=2", "d.html")
                                                                                                                                                                              Watcher.py                                                                                          0000664 0000771 0000772 00000001372 12027737477 012731  0                                                                                                    ustar   weidong                         weidong                                                                                                                                                                                                                #!/bin/python
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
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      