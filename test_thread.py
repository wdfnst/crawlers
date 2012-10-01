#!/bin/python
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
