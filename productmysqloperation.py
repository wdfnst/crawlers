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
#   2.getconn(self)
#   3.set(self, key, value)
#   4.get(self)
#   5.sadd(self, userset, uservalue)
#   .....
###############################################
#import urllib2
#import hashlib
#import time
#from urlparse import urlparse
#import ImageFile

#from PIL import Image
#import glob, os, socket, sys, re
#from datetime import datetime, date
#import logging
#import simplejson
#import json

#import MySQLdb as mdb
#from DBUtils.PooledDB import PooledDB
#import redis

#from redisoperation import RedisOperation
#from settings import Settings

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

# Timeout in seconds
timeout = 180
socket.setdefaulttimeout(timeout)

# Create logging handler
now = datetime.now()
nowstr = str(now.year) + str(now.month) + str(now.day) + str(now.hour)
logger = logging.getLogger('crawler')
hdlr = logging.FileHandler('productcrawler-' + nowstr + '.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.INFO)

# Integrated mysql operation class
class DataSynch(object):
	
	# Special characters which needed to be replace by blackspace in title, text etc.
	punctuations_to_erase = ['`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', \
							'+', '-', '=', '{', '[', '}', ']', '|', ':', ';', '\'', '"', '<', ',', '>', '.', '/', '$', '\\', '•', '"', "'"]

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

	def __init__(self, mysql_pool, ro, connection_no):
		self.pool = mysql_pool
		self.ro   = ro
		self.mysql_conn = self.pool.connection(connection_no)
		self.cur = self.mysql_conn.cursor()
		self.logger = logger

	# Save original image directly
	# The im.convert() appears to not work well
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
		try:
			if seed_type == 3:
				self.cur.execute("select url, id, SELLER_URL, CATEGORY, TITLE_XPATH, DETAILURL_XPATH, DETAILURLHEADER, NEXTPAGE_XPATH, NEXTPAGEHEADER, IMAGESOURCE, IMAGEURL_XPATH, IMAGEURLHEADER, BRAND_XPATH, DESCRIPTION_XPATH, PRODUCTID_XPATH, COLOR_XPATH, PRICE_XPATH, SIZE_XPATH, MAINIMAGEURL_XPATH, CATEGORY_1 from seed where photosourcetype_id=3 and deleted=0 and webkit=0")
				rows = self.cur.fetchall()
				print "len(product seeds):" + str(len(rows))
				for row in rows:
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
						urljson = {'url': row[0], 'seed_id': row[1], 'depth':1, 'pagetype':3, 'category':row[3], 'category_1':row[19]}
						print urljson
#						self.ro.lpush(self.product_nothrow_urljson_list, urljson) 
						self.ro.check_lpush(self.product_page_url_set, hashlib.sha1(seed_url).hexdigest(), self.product_nothrow_urljson_list, urljson)
		except mdb.Error, e:
			self.logger.error("Error %d:%s"%(e.args[0], e.args[1]))
			sys.exit()

	# Insert image and download
	def	insertimage_on_duplicate(self, image_url, sourcepage_url, page_id):
		import re
		if isinstance(image_url, unicode):
			itemsrc = image_url.encode('utf-8', 'ignore')
		else:
			itemsrc = image_url

		# Note: the photourlhash is calcuted before the '/" was escaped
		photourlhash = hashlib.sha1(itemsrc).hexdigest()
		itemsrc = itemsrc.replace("'", "\\\'")
		itemsrc = itemsrc.replace('"', '\\\"')
		sourcepage_url = sourcepage_url.replace("'", "\\\'")
		sourcepage_url = sourcepage_url.replace('"', '\\\"')

		sql = ""
		try:
			sql = "insert ignore into photocache(datepost, photosourcetype_id, photourl,  pageurl, photourlhash) value(now(), 3, '%s', '%s', '%s') on duplicate key update datepost=now()"%(itemsrc , sourcepage_url, photourlhash)
			re = self.cur.execute(sql)
			self.logger.info("Image insert re:%s"%(re))
			return self.cur.lastrowid
		except mdb.Error, e:
			self.logger.error("mysql insert image with download error, table: photocache, errormsg: %d - %s"%(e.args[0], e.args[1]))
			return 0
		except Exception, e:
			self.logger.error("mysql insert image with download error, table: photocache, errormsg:%s - sql:%s"%(e, sql))
			return 0

	# synchronize product
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
