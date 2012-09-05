#!/bin/python
# -*- coding: utf-8 -*-
from redisoperation import RedisOperation
from mysqloperation import DataSynch
from util import Util
from settings import Settings

import sys, os, re, operator, datetime, time, signal, threading, gc, socket
import hashlib, urlparse
import urllib, cookielib
import urllib2
from urllib2 import Request
import encodings.idna
import lxml.html

reload(sys)
sys.setdefaultencoding('utf-8')

# timeout in seconds
timeout = 180
socket.setdefaulttimeout(timeout)

class Watcher:
	def __init__(self):
		settings = Settings()
		self.process_no = settings.blogcrawlersettings['process_no']
		self.children = []
		self.watch()

	def watch(self):
		counter = 0
		try:
			while True:
				child = os.fork()
				counter += 1
				if child == 0:
					return
				else:
					self.children.append(child)
					print "create child process: %d"%child
					time.sleep(3)
					
				while counter >= self.process_no:
					for pid in self.children:
						p_child = psutil.Process(pid)
						memory_info = p_child.get_memory_info()
						print str(pid) + " " + str(memory_info.rss)
						if memory_info.rss > 125829120:    #120M 524288000 500M:
							os.kill(pid, signal.SIGKILL)
							counter -= 1
							self.children.remove(pid)
					time.sleep(3)
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

class BlogCrawler(threading.Thread):

	running   = True
	settings  = Settings()

	def __init__(self, threadname, thread_no):
		threading.Thread.__init__(self,name=threadname)
		signal.signal(signal.SIGINT, self.sigint_handle)
		self.depth_limit          = self.settings.depth_limit
		self.page_url_set         = self.settings.blogcrawlersettings["page_url_set"]
		self.image_url_set        = self.settings.blogcrawlersettings["image_url_set"]
		self.nothrow_urljson_list = self.settings.blogcrawlersettings["nothrow_urljson_list"]
		self.delay_time           = 0.25
		self.datasyn			  = DataSynch(thread_no)
		self.ro					  = RedisOperation()
		self.util				  = Util()
				
	#def __del__(self):
		#pass

	def sigint_handle(self, signum, frame):
		self.running = False
		#self.logger.info("Catch SINGINT interrupt signal...")

	def init_delay_flag(self, domain_sh):
		self.ro.set(domain_sh, 0)

	def init_crawler(self):
		reading = self.ro.get("reading_seeds")
		if (reading is None or reading == 0) and self.ro.conn.llen(self.nothrow_urljson_list) <= 0:
			self.ro.set("reading_seeds", 1)
			init_level = 1
		else:
			init_level = 0
			time.sleep(3)

		#init_level = self.settings.blogcrawlersettings['init_level']
		if init_level == 0:
			pass
		elif init_level == 1:
			self.ro.conn.delete(self.page_url_set)
			self.ro.conn.delete(self.image_url_set)
			self.ro.conn.delete(self.nothrow_urljson_list)
			self.datasyn.read_seeds(2)
			self.ro.set("reading_seeds", 0)
		elif init_level == 2:
			self.ro.conn.delete(self.page_url_set)
			self.ro.conn.delete(self.nothrow_urljson_list)
			self.datasyn.read_seeds(2)
		elif init_level == 3:
			self.ro.conn.delete(self.page_url_set)
			self.ro.conn.delete(self.image_url_set)
			self.ro.conn.delete(self.nothrow_urljson_list)
			self.datasyn.read_seeds(2)
		elif init_level == 4:
			self.ro.conn.delete(self.page_url_set)
			self.ro.conn.delete(self.image_url_set)
			self.ro.conn.delete(self.nothrow_urljson_list)
			self.datasyn.read_seeds(2)
		elif init_level == 5:
			pass
		elif init_level == 6:
			pass

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

	# Append url to nothrow url list or level url list
	def appendnothrowurllist(self, urls, base_url, seed_url, seed_ext, seed_id, depth, page_type):
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
			if not(curl == None or curl == "") and not(curl.startswith('javascript:')) and not(curl.startswith('mailto:')) and \
						 not(('.jpg' in curl) or ('.png' in curl) or ('.gif' in curl) or  ('.gpeg' in curl) or  \
						('.bmp' in curl) or ('.tiff' in curl) or ('.pcx' in curl) or ('.tga' in curl) or \
						('facebook.com' in curl) or ('google.com' in curl) or ('twitter.com' in curl) or ('google.com' in curl)):
				page_url_sh = hashlib.sha1(curl).hexdigest()
				urljson = {'url': curl, 'seed_id': seed_id, 'depth':depth + 1, 'pagetype':page_type}
				re = self.ro.check_lpush(self.page_url_set, page_url_sh, self.nothrow_urljson_list, urljson)

	def extract_blog_image(self, tree, urljson, blogpage_id):
		elements = tree.xpath("//img")
		for element in elements:
			imgitem = {}
			if element.xpath('name()') == 'img' and element.xpath('@src') is not None and len(element.xpath('@src')) > 0:
				imgitem['src']          = self.util.concat_image_url(urljson['url'], element.xpath('@src')[0])
				imgitem['desc']         = self.util.get_start_one(element.xpath('@alt'))
				imgitem['postdate']     = datetime.datetime.now().date().isoformat()
				imgitem['sourcetypeid'] = 2
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
		# Insert blogpageitem into msyql
		blogpage_id = self.datasyn.insertblogpage_on_duplicate(blogpageitem)
		if blogpage_id > 0:
			self.datasyn.insertblogpageseedrelation_with_ignore(blogpage_id, urljson['seed_id'])
			self.extract_blog_image(tree, urljson, blogpage_id)

		#del tree
		#del urls
		#del title
		#del tagstxts
		#del blogpageitem

	#def parse(self):
	def run(self):
		print "Thread - " + self.getName() + " started run"
		#self.datasyn.read_seeds(2)
		self.init_crawler()
		gc_counter = 0
		while self.running:
			urljson   = self.get_urljson(self.nothrow_urljson_list)
			if urljson is not None:
				urljson  = eval(urljson)
				self.parse_blog_page(urljson, urljson['url'])
				#gc_counter += 1
				#if gc_counter % 10 == 0:
					#gc.collect()

	def stop(self):
		#self.thread_stop = True
		pass

def main(script, flag='with'):

	ro = RedisOperation()
	ro.cleardb()
	time.sleep(3)
	init_datasyn = DataSynch(100)
	init_datasyn.read_seeds(2)
	time.sleep(5)

	if flag == 'with':
		Watcher()
	elif flag != 'without':
		print 'unrecognized flag: ' + flag
		sys.exit()

	scs = []
	for i in range(36):
		scs.append(BlogCrawler("T" + str(i), i))
	for i in range(36):
		scs[i].start()
	for i in range(36):
		scs[i].join()
	for i in range(36):
		scs[i].stop()

if __name__ == '__main__':
	main(*sys.argv)
