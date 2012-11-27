#!/bin/python
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

from redisoperation import RedisOperation
from settings import Settings
from util import Util

# Global settings
#reload(sys)
#sys.setdefaultencoding('utf-8')
# Timeout in seconds
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

# The integrated mysql operation class
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
				self.cur.execute("select id, url, webkit, photosourcetype_id from seed where deleted=0 and photosourcetype_id=%d"%(seed_type))
				seeds = self.cur.fetchall()
				print "len(seeds):" + str(len(seeds))
				for seed in seeds:
					urljson = {'url':seed[1], 'seed_id':seed[0], 'depth':1, 'pagetype':seed[3], 'jsmw':seed[2]}
					self.ro.set(seed[0], seed[1])
					#print urljson
					self.ro.check_lpush(self.blog_page_url_set, hashlib.sha1(seed[1]).hexdigest(), self.blog_nothrow_urljson_list, urljson)
					self.init_delay_flag(hashlib.sha1(self.util.get_domain(seed[1])).hexdigest())
			elif seed_type == 3:
				self.cur.execute("select url, id, SELLER_URL, CATEGORY, TITLE_XPATH, DETAILURL_XPATH, DETAILURLHEADER, NEXTPAGE_XPATH, NEXTPAGEHEADER, IMAGESOURCE, IMAGEURL_XPATH, IMAGEURLHEADER, BRAND_XPATH, DESCRIPTION_XPATH, PRODUCTID_XPATH, COLOR_XPATH, PRICE_XPATH, SIZE_XPATH, MAINIMAGEURL_XPATH from seed where photosourcetype_id=3 and deleted=0 and webkit=0")
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
