#!/bin/python
# -*- coding: utf-8 -*-
from redisoperation import RedisOperation
from mysqloperation import DataSynch
from util import Util
from blogsettings import Settings

import sys, os, re, operator, datetime, time, signal, threading, gc, socket
import hashlib, urlparse
import urllib
import urllib2
from urllib2 import Request
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

# Get mysqldb/redis host configure info
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

# Definition of schedule process
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
						if memory_info.rss > 524288000:#  34M  35651584:    #120M 524288000 500M:
							os.kill(pid, signal.SIGKILL)
							counter -= 1
							self.children.remove(pid)
					time.sleep(5)
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

# Definition of BlogCrawler class
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

	# Set delay_flag to 1 after accessed on domain, Set delay_flag to 0 after delay_time
	def set_delay_timer(self, domain_sh):
		self.ro.set(domain_sh, 1)
		t = threading.Timer(self.delay_time, self.reset_delay_flag, [domain_sh])
		t.start()

	# Get a urljson from redis' queue(list)
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
				return self.ro.rpop(urljson_list)
		except:
			pass

	# Append url to nothrow url list or level url list
	def appendnothrowurllist(self, urls, base_url, seed_url, seed_ext, seed_id, depth, page_type):
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
			r = Request(urljson['url'])
			r.add_header('User-Agent', 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)')
			response = urllib2.urlopen(r)
			content = response.read()
			tree    = lxml.html.fromstring(content)
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

	def run(self):
		print "Thread - " + self.getName() + " started run"
		while self.running:
			urljson   = self.get_urljson(self.nothrow_urljson_list)
			if urljson is not None:
				urljson  = eval(urljson)
				self.parse_blog_page(urljson, urljson['url'])

	def stop(self):
		pass

# The main procedure
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
