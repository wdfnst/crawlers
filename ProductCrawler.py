#!/bin/python
# -*- coding: utf-8 -*-
from redisoperation import RedisOperation
from mysqloperation import DataSynch
from util import Util

import sys, os, re, operator, datetime, time, signal, threading, socket
import hashlib, urlparse
import urllib, cookielib
import urllib2
from urllib2 import Request
import encodings.idna
import lxml.html

from settings import Settings

reload(sys)
sys.setdefaultencoding('utf-8')

# timeout in seconds
timeout = 180
socket.setdefaulttimeout(timeout)

class ProductCrawler(threading.Thread):

	running   = True
	settings  = Settings()

	def __init__(self, threadname, thread_no):
		threading.Thread.__init__(self,name=threadname)
		signal.signal(signal.SIGINT, self.sigint_handle)

		self.depth_limit			= self.settings.depth_limit
		self.url_set				= self.settings.productcrawlersettings["page_url_set"]
		self.image_url_set			= self.settings.productcrawlersettings["image_url_set"]
		self.nothrow_urljson_list	= self.settings.productcrawlersettings["nothrow_urljson_list"]
		self.failed_page_url_set_1	= self.settings.productcrawlersettings["failed_url_list_1"]
		self.failed_page_url_set_2	= self.settings.productcrawlersettings["failed_url_list_2"]
		self.failed_page_url_set_3	= self.settings.productcrawlersettings["failed_url_list_3"]
		self.delay_time				= 0.25
		self.datasyn				= DataSynch(thread_no)
		self.ro						= RedisOperation()
		self.util					= Util()

	#def __del__(self):
		#pass

	def sigint_handle(self, signum, frame):
		self.running = False

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
	def appendnothrowurllist(self, urls, base_url, seed_url, seed_ext, seed_id, depth, page_type):
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
			curl = self.util.normalize_url(curl)
			#print curl
			import re
			if curl is None or re.findall(r"#[0-9a-zA-Z]*$", curl):
				continue
			# If crawler type is blog-crawler, then all urls must start with seed_url
			# Exclude invalid urls
			if not(curl == None or curl == "") and not(curl.startswith('javascript:')) and not(curl.startswith('mailto:')) \
						 and not(('.jpg' in curl) or ('.png' in curl) or ('.gif' in curl) or  ('.gpeg' in curl) or  ('.bmp' in curl)\
								 or ('.tiff' in curl) or ('.pcx' in curl) or ('.tga' in curl) or ('facebook.com' in curl) or \
								 ('google.com' in curl) or ('twitter.com' in curl) or ('google.com' in curl)):
				page_url_sh = hashlib.sha1(curl).hexdigest()
				#saddre = self.ro.sadd(self.page_url_set,  page_url_sh)		# add into page_url_set
				urljson = {'url': curl, 'seed_id': seed_id, 'depth':depth + 1, 'pagetype':page_type}
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
			return -1
		# Depth surpass the limit, then exit
		if urljson['depth'] >= self.depth_limit:
			return None
		urls = tree.xpath(self.ro.get(str(seed_id) + "_" + "DETAILURL_XPATH"))
		print "detail page url num:", len(urls)
		restr6 = tree.xpath(self.ro.get(str(seed_id) + "_" + "NEXTPAGE_XPATH"))
		print "list page url num:", len(restr6)

		seed_url = self.ro.get(urljson['seed_id'])
		self.appendnothrowurllist(urls, urljson['url'], seed_url, self.util.getcompleteurl(seed_url), urljson['seed_id'], urljson['depth'], 31)
		self.appendnothrowurllist(restr6, urljson['url'], seed_url, self.util.getcompleteurl(seed_url), urljson['seed_id'], urljson['depth'], 3)
		
	# Extract product detail page info
	def parse_product_detailpage(self, url, seed_id, depth):
		response_url = url
		try:
			content = urllib.urlopen(url).read()
			tree    = lxml.html.fromstring(content)
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
		productpageitem['pagetext']			 = "".join(tree.xpath('//*[not(self::a | self::style | self::script | self::head | self::img | \
																	self::noscript | self::form | self::option)]/text()'))
		productpageitem['producturl']		 = response_url
		productpageitem['crawlerseedurl_id'] = seed_id

		# Check proudct page is existed: insert or update
		page_re = self.datasyn.insertproductpage_on_duplicate(productpageitem)

		# Check [productpage, seed] is existed: insert or update
		if page_re > 0:
			self.datasyn.insert_relationship_with_id('productpageseedrelation', "seed_id", page_re, seed_id)

			# Extract image info and cache image/[imageurl, pageurl] info
			imageitem    = {}
			image_amount = 0
			for imageurl in restr9:
				imageitem['src']          = imageurl
				imageitem['desc']         = "".join(self.util.tuplelist2list(restr12))
				imageitem['sourcepage']   = response_url
				imageitem['sourcetypeid'] = 3
				imageitem['postdate']     = ""
				image_url_sh = hashlib.sha1(imageurl).hexdigest()

				# Check image is existed: insert or update
				image_re = 0
				#if self.datasyn.check_image_exists(image_url_sh) == 1:
				if self.ro.sadd(self.image_url_set, image_url_sh) == 1:
					image_re = self.datasyn.insert_image_with_download(imageitem, 0)
					if image_re > 0:
						self.datasyn.insert_relationship_with_id("productphotopagerelation", "photo_id", page_re, image_re)
						image_amount += 1
						if image_amount == 1:
							mainimage_sh = image_url_sh
				else:
					pass

			# Update page's mainimage info and image amount
			if image_amount > 0:
				self.datasyn.updateproductpage_simple(page_url_sh, image_amount, mainimage_sh)
			else:
				# If process page failed, then push this page to the last layer queue
				if self.ro.lpush(self.failed_page_url_set_1, hashlib.sha1(response_url).hexdigest()) != 0 and \
						self.ro.lpush(self.failed_page_url_set_2, hashlib.sha1(response_url).hexdigest()) != 0 and \
						self.ro.lpush(self.failed_page_url_set_3, hashlib.sha1(response_url).hexdigest()) != 0:
					req_url_seed = {'url': response_url, 'seed_id': seed_id, 'depth':depth, 'pagetype':31}
					self.ro.lpush(self.nothrow_urljson_list, req_url_seed)
					#self.ro.lpush(self.level_url_list + str(self.depth_limit), req_url_seed)
				else:
					self.ro.lpush("failed_page_url", [response_url, restr9, restr17])

	#def parse(self):
	def run(self):
		print self.getName() + " start run"
		self.datasyn.read_seeds(3)
		while self.running:
			urljson   = self.get_urljson(self.nothrow_urljson_list)
			if urljson is not None:
				#print urljson
				urljson  = eval(urljson)
				if urljson['pagetype'] == 3:
					self.parse_product_listpage(urljson)
				elif urljson['pagetype'] == 31:
					self.parse_product_detailpage(urljson['url'], urljson['seed_id'], urljson['depth'])

	def stop(self):
		#self.thread_stop = True
		pass

def main(script, flag='with'):
	ro = RedisOperation()
	ro.cleardb()
	init_datasyn = DataSynch(100)
	init_datasyn.read_seeds(2)

	if flag == 'with':
		Watcher()
	elif flag != 'without':
		print 'unrecognized flag: ' + flag
		sys.exit()

	scs = []
	for i in range(12):
		scs.append(ProductCrawler("T" + str(i), i))
	for i in range(12):
		scs[i].start()
	for i in range(12):
		scs[i].join()
	for i in range(12):
		scs[i].stop()

if __name__ == '__main__':
	main(*sys.argv)
