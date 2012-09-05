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
#   3.read_seeds(self, key, value)
#   4.insertblogpage_on_duplicate(self)
#   5.update_image(self, imageitem)
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
	pool = PooledDB( creator = mdb, mincached = 5, db = mysql_db, host = mysql_host, user = mysql_user, passwd= mysql_passwd, charset = "utf8", use_unicode = True)
	ro = RedisOperation()
	util = Util()

	def __init__(self, connection_no):
		self.mysql_conn = self.pool.connection(connection_no)
		self.cur = self.mysql_conn.cursor()
		now = datetime.now()
		nowstr = str(now.year) + str(now.month) + str(now.day) + str(now.hour) + str(now.minute) + str(now.second)
		log.start("datasyn-" + nowstr + ".log")

	# Save original image directly
	def save_original_image(self, im, new_im_path):
		try:
			if im.mode != "RGB":
   				im = im.convert("RGB")
			im.save(new_im_path, "JPEG",quality=100)
			return 1
		except Exception, e:
			log.msg('save original image failed...error:%s'%(str(e)), level=log.ERROR)
			#self.logger.error('save original image failed...error:%s'%(str(e)))
			return 0

	def init_delay_flag(self, domain_sh):
		self.ro.set(domain_sh, 0)

	def read_seeds(self, seed_type):
		try:
			if seed_type == 2:
				self.cur.execute("select id, url, webkit, photosourcetype_id from crawlerseedurl where deleted=0 and photosourcetype_id=%d"%(seed_type))
				seeds = self.cur.fetchall()
				print "len(seeds):" + str(len(seeds))
				for seed in seeds:
					urljson = {'url':seed[1], 'seed_id':seed[0], 'depth':1, 'pagetype':seed[3], 'jsmw':seed[2]}
					self.ro.set(seed[0], seed[1])
					self.ro.check_lpush(self.blog_page_url_set, hashlib.sha1(seed[1]).hexdigest(), self.blog_nothrow_urljson_list, urljson)
					self.init_delay_flag(hashlib.sha1(self.util.get_domain(seed[1])).hexdigest())
			elif seed_type == 3:
				self.cur.execute("select url, id, SELLER_URL, CATEGORY, TITLE_XPATH, DETAILURL_XPATH, DETAILURLHEADER, NEXTPAGE_XPATH, NEXTPAGEHEADER, IMAGESOURCE, IMAGEURL_XPATH, IMAGEURLHEADER, BRAND_XPATH, DESCRIPTION_XPATH, PRODUCTID_XPATH, COLOR_XPATH, PRICE_XPATH, SIZE_XPATH, MAINIMAGEURL_XPATH from crawlerseedurl where photosourcetype_id=3 and deleted=0 and webkit=0")
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
						urljson = {'url': row[0], 'seed_id': row[1], 'depth':1, 'pagetype':3}
						self.ro.check_lpush(self.product_page_url_set, hashlib.sha1(seed_url).hexdigest(), self.product_nothrow_urljson_list, urljson)
						self.init_delay_flag(hashlib.sha1(self.util.get_domain(seed_url)).hexdigest())
		except mdb.Error, e:
			log.msg("Error %d:%s"%(e.args[0], e.args[1]), level=log.ERROR)
			sys.exit()

	# synchronize blog pge info
	def insertblogpage_on_duplicate(self, item):
		import re
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
			sql = "insert into blogpage(title, pageurl, pagetext, pageurlhash, detailed, dateinsert) values('%s', '%s', '%s', '%s', %d, UNIX_TIMESTAMP()) on duplicate key update title=values(title), pagetext=values(pagetext), daterefresh=UNIX_TIMESTAMP()"%(title, blogurl, blogdetail, blogurl_sh, int(item['detailed']))
			re = self.cur.execute(sql)
			self.mysql_conn.commit()
			if re >= 1:
				log.msg("Blog insert re:%s"%(re), level=log.INFO)
			return self.cur.lastrowid
		except mdb.Error, e:
			log.msg("mysql insert blogpage error, table: blogpage, errormsg:%d-%s"%(e.args[0], e.args[1]), level=log.ERROR)
			return 0
		except Exception, e:
			log.msg("mysql insert blogpage error, table: blogpage, errormsg: %s"%(e), level=log.ERROR)
			return 0

	# Insert blogpage photo relation by pageurlhash and photourlhash
	def insert_blogpage_photo_relationship_with_id(self, page_id, image_id):		
		try:	
			relationsql = "insert ignore into blogphotopagerelation(page_id, photo_id) values(%d, %d)"%(page_id, image_id)
			re = self.cur.execute(relationsql)
			log.msg("Relationship insert re:%d, %s"%(re, relationsql), level=log.INFO)
			return re
		except mdb.Error, e:
			log.msg("mysql insert relation error, table: blogphotopagerelation, errormsg:%d - %s"%(e.args[0], e.args[1]), level=log.ERROR)
			return 0
		except Exception, e:
			log.msg("insert relation error:%s - %s"%(e, relationsql), level=log.ERROR)
			return 0
	
	# Insert blogpage photo relation by pageurlhash and photourlhash
	def insert_relationship_with_id(self, tablename, field, page_id, image_id):		
		try:	
			relationsql = "insert ignore into %s(page_id, %s) values(%d, %d)"%(tablename, field, page_id, image_id)
			re = self.cur.execute(relationsql)
			log.msg("Relationship insert re:%d, %s"%(re, relationsql), level=log.INFO)
			return re
		except mdb.Error, e:
			log.msg("mysql insert relation error, table: %s, errormsg:%d - %s"%(tablename, e.args[0], e.args[1]), level=log.ERROR)
			return 0
		except Exception, e:
			log.msg("insert relation error:%s - %s"%(e, relationsql), level=log.ERROR)
			return 0

	# Insert blogpageseedrelation
	def insertblogpageseedrelation_with_ignore(self, blogpage_id, seed_id):
		if blogpage_id == None or seed_id == None or blogpage_id == 0 or seed_id == 0:
			return 0
		try:
			insert_sql = "insert ignore into blogpageseedrelation(page_id, seed_id) value(%d, %d)"%(blogpage_id, seed_id)
			re = self.cur.execute(insert_sql)
			log.msg("Blogpage seed relationship insert re:%d,blogpage_id - seed_id: %d - %d"%(re, blogpage_id, seed_id), level=log.INFO)
			return re
		except mdb.Error, e:
			log.msg("mysql insert blogpage-seed relation error, table: photocache, errormsg:%d - %s"%(e.args[0], e.args[1]), level=log.ERROR)
			return 0
		except Exception, e:
			log.msg("insert blogpage-seed relation error:%s - %d"%(e, blogpage_id), level=log.ERROR)
			return 0

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
					height = p.image.size[0]
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
				log.msg("save image failed url error: %s - %s"%(e, image_url), level=log.ERROR)
				time.sleep(0.25)
		# If failed save image then recode the image url
		self.ro.lpush("failed_image_ur", image_url)

	# insert image and download
	def	insert_image_with_download(self, item, quality_size):
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
			sql = "insert ignore into photocache(datepost, photosourcetype_id, photourl, title, photourlhash) value('%s', %d, '%s', '%s', '%s')"%(item['postdate'], int(item['sourcetypeid']) , itemsrc , desc, photourlhash)
			re = self.cur.execute(sql)
			log.msg("Image insert re:%s"%(re), level=log.INFO)
			return self.cur.lastrowid
		except mdb.Error, e:
			log.msg("mysql insert image with download error, table: photocache, errormsg: %d - %s"%(e.args[0], e.args[1]), level=log.ERROR)
			return 0
		except Exception, e:
			log.msg("mysql insert image with download error, table: photocache, errormsg:%s - sql:%s"%(e, sql), level=log.ERROR)
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
				log.msg("Product insert re:%s"%(re), level=log.INFO)
			return self.cur.lastrowid
		except mdb.Error, e:
			log.msg("mysql update productpage error, table: productpage errormsg:%d - %s"%(e.args[0], e.args[1]), level=log.ERROR)
			return 0
		except Exception, e:
			log.msg("mysql insert productpage page error, table: productpage, errormsg:%s"%(e), level=log.ERROR)
			return 0

	# update productpage's imageamount and mainimage_id
	def updateproductpage_simple(self, page_url_sh, image_amount, mainimage_sh):
		try:
			imageid_re       = []
			select_sql = "select id from photocache where photourlhash='%s'"%(mainimage_sh)
			self.cur.execute(select_sql)
			imageid_re  = self.cur.fetchone()
			if imageid_re != None and len(imageid_re) > 0:
				image_id  = int(imageid_re[0])
				sql = "update productpage set imageamount=%d, mainimage_id=%d where pageurlhash='%s'"%(image_amount, image_id, page_url_sh)
				re = self.cur.execute(sql)
				self.mysql_conn.commit()
				return re
			else:
				log.msg("mysql update productpage error, table: productpage errormsg:%d - %s"%(e.args[0], e.args[1]), level=log.ERROR)
		except mdb.Error, e:
			log.msg("mysql update productpage error, table: productpage errormsg:%d - %s"%(e.args[0], e.args[1]), level=log.ERROR)
			return 0
		except Exception, e:
			log.msg("mysql update productpage error, table: productpage, errormsg:%d-%s"%(e.args[0], e.args[1]), level=log.ERROR)
			return 0
