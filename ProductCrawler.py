#!/bin/python
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
import psutil
import MySQLdb as mdb
from DBUtils.PooledDB import PooledDB
import redis

from productsettings import Settings

# Timeout in seconds
reload(sys)
sys.setdefaultencoding('utf-8')
timeout = 360
socket.setdefaulttimeout(timeout)

# Get mysqldb/redis host etc configure info
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

# The class to define the schedule process
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

# Definition of crawler thread
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

	# Get a urljson from redis' queue(list)
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

	# Append url to nothrow url list or level url list
	def appendnothrowurllist(self, urls, base_url, seed_url, seed_ext, seed_id, depth, page_type, category, category_1):
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
			import re
			if curl is None or re.findall(r"#[0-9a-zA-Z-_]*$", curl):
				continue
			# Exclude invalid urls
			if not(curl == None or curl == "") and not(curl.startswith('javascript:')) and not(curl.startswith('mailto:')) \
						 and not(('.jpg' in curl) or ('.png' in curl) or ('.gif' in curl) or  ('.gpeg' in curl) or  ('.bmp' in curl)\
								 or ('.tiff' in curl) or ('.pcx' in curl) or ('.tga' in curl) or ('facebook.com' in curl) or \
								 ('google.com' in curl) or ('twitter.com' in curl) or ('google.com' in curl)):
				page_url_sh = hashlib.sha1(curl).hexdigest()
				urljson = {'url': curl, 'seed_id': seed_id, 'depth':depth + 1, 'pagetype':page_type, 'category':category, 'category_1':category_1}
				re = self.ro.check_lpush(self.url_set, page_url_sh, self.nothrow_urljson_list, urljson)

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
		self.appendnothrowurllist(urls, urljson['url'], seed_url, self.util.getcompleteurl(seed_url), urljson['seed_id'], urljson['depth'], 31, urljson['category'], urljson['category_1'])
		self.appendnothrowurllist(restr6, urljson['url'], seed_url, self.util.getcompleteurl(seed_url), urljson['seed_id'], urljson['depth'], 3, urljson['category'], urljson['category_1'])
		
	# Extract product detail page info
	def parse_product_detailpage(self, url, seed_id, depth, category, category_1, urljson):
		response_url = url
		try:
			content = urllib.urlopen(url).read()
			tree    = lxml.html.fromstring(content)
			#fi = open("tmp/" + hashlib.sha1(urljson['url']).hexdigest(), 'w')
			#fi.write(content)
		except:
			return -1
		productpageitem = {}
		page_url_sh     = hashlib.sha1(response_url).hexdigest()
		
		restr3  = tree.xpath(self.ro.get(str(seed_id) + "_" + "TITLE_XPATH"))
		restr9  = tree.xpath(self.ro.get(str(seed_id) + "_" + "IMAGEURL_XPATH"))
		restr11 = tree.xpath(self.ro.get(str(seed_id) + "_" + "BRAND_XPATH"))
		restr12 = tree.xpath(self.ro.get(str(seed_id) + "_" + "DESCRIPTION_XPATH"))
		restr13 = tree.xpath(self.ro.get(str(seed_id) + "_" + "PRODUCTID_XPATH"))
		restr14 = tree.xpath(self.ro.get(str(seed_id) + "_" + "COLOR_XPATH"))
		restr15 = tree.xpath(self.ro.get(str(seed_id) + "_" + "PRICE_XPATH"))
		restr16 = tree.xpath(self.ro.get(str(seed_id) + "_" + "SIZE_XPATH"))
		restr17 = tree.xpath(self.ro.get(str(seed_id) + "_" + "MAINIMAGEURL_XPATH"))

		#print "------- title: ", str(restr3)
		#print "------- price: ", str(restr15)

		restr9  = self.util.tuplelist2list(restr9)
		restr17 = self.util.tuplelist2list(restr17)
		restr17 = self.util.getfirstnullelement(restr17)
		restr9  = self.util.get_url_from_strings(restr9)
		restr9  = self.util.convt2compurl(url, restr9)
		restr17 = self.util.convt2compurl(url, restr17)
		productpageitem['title']			 = "".join(self.util.tuplelist2list(restr3))
		productpageitem['imageamount']		 = -1
		productpageitem['brand']			 = "".join(self.util.tuplelist2list(restr11))
		productpageitem['desc']				 = "".join(self.util.tuplelist2list(restr12))
		productpageitem['productid']		 = "".join(self.util.tuplelist2list(restr13))
		productpageitem['color']			 = "".join(self.util.tuplelist2list(restr14))
		productpageitem['price']			 = "".join(self.util.tuplelist2list(restr15))
		productpageitem['size']				 = "".join(self.util.tuplelist2list(restr16))
		productpageitem['mainimageurl']		 = "".join(restr17)
		try:
			productpageitem['pagetext']	     = "".join(tree.xpath('//*[not(self::a | self::style | self::script | self::head | self::img | \
																		self::noscript | self::form | self::option)]/text()'))
		except:
			productpageitem['pagetext']      = 'no desc'
	
		productpageitem['producturl']		 = response_url
		productpageitem['crawlerseedurl_id'] = seed_id
	
		#print urljson
		print "------- ", str(restr17)

		# Check proudct page is existed: insert or update
		if restr17:
			productpageitem['mainimageid'] = 0
			productpageitem['category']    = category
			productpageitem['category_1']  = category_1
			page_re = self.datasyn.insertproductpage_on_duplicate(productpageitem)
			if page_re > 0:
				self.datasyn.insert_relationship_with_id('productpageseedrelation', "seed_id", page_re, seed_id)
				img_re = self.datasyn.insertimage_on_duplicate(productpageitem['mainimageurl'], response_url, page_re)
				if img_re > 0:
					self.datasyn.insert_relationship_with_id("productphotopagerelation", "photo_id", page_re, img_re)
		elif restr3 and restr15:
			self.ro.lpush(self.nothrow_urljson_list, urljson)

	def run(self):
		print self.getName() + " start run"
		while self.running:
			urljson   = self.get_urljson(self.nothrow_urljson_list)
			if urljson is not None:
				urljson  = eval(urljson)
				if urljson['pagetype'] == 3:
					self.parse_product_listpage(urljson)
				elif urljson['pagetype'] == 31:
					self.parse_product_detailpage(urljson['url'], urljson['seed_id'], urljson['depth'], urljson['category'], urljson['category_1'], urljson)

	def stop(self):
		pass

# The main procedure in worker process
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

# From Watcher import Watcher
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
