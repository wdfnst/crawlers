#This file is to define the redis operations
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
